# Path: ~/klasker-bot/database.py
"""TiDB persistence layer for KlaskerBot.

TiDB speaks the MySQL protocol, so this module uses PyMySQL.
The database is the shared source of truth for SVELTRON and CORSAIR.

Environment variables:
    KLASKER_DB_HOST
    KLASKER_DB_PORT       (default: 4000)
    KLASKER_DB_USER
    KLASKER_DB_PASSWORD
    KLASKER_DB_NAME       (default: klaskerbot)
    KLASKER_DB_SSL        (default: true)
    KLASKER_DB_SSL_CA     (optional CA certificate path)

If KLASKER_DB_SSL_CA is not supplied, the module automatically uses
the local ISRG Root X1 certificate at:

    ~/klasker-bot/certs/isrgrootx1.pem

when that file exists.

Command-line operations:
    python3 database.py
    python3 database.py --check
    python3 database.py --init

The module deliberately contains no crawler or network-discovery logic.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

try:
    import pymysql
    from pymysql.connections import Connection
except ImportError as exc:
    raise RuntimeError(
        "PyMySQL is required. Install it with: pip install PyMySQL"
    ) from exc

from klaskerbot import (
    BotRuntime,
    BotState,
    Checkpoint,
    DiscoveryRelationship,
    FrontierItem,
    HumanRequest,
    Job,
    JobPriority,
    JobStatus,
    JobType,
    Worker,
    WorkerStatus,
)


SCHEMA_VERSION = 1
DEFAULT_PORT = 4000
DEFAULT_DATABASE = "klaskerbot"

DEFAULT_SSL_CA = (
    Path(__file__).resolve().parent
    / "certs"
    / "isrgrootx1.pem"
)


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_db_time(value: Optional[str]) -> Optional[str]:
    """Convert an ISO timestamp to a MySQL DATETIME string."""

    if value is None:
        return None

    parsed = datetime.fromisoformat(value)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)

    return parsed.strftime("%Y-%m-%d %H:%M:%S.%f")


def from_db_time(value: Any) -> Optional[str]:
    """Convert a MySQL datetime value to an ISO-8601 UTC string."""

    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)

        return value.isoformat()

    text = str(value)

    parsed = datetime.fromisoformat(
        text.replace("Z", "+00:00")
    )

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)

    return parsed.isoformat()


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class DatabaseConfig:
    """TiDB connection configuration loaded from environment variables."""

    def __init__(
        self,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        ssl: Optional[bool] = None,
        ssl_ca: Optional[str] = None,
    ) -> None:
        self.host = host or os.getenv(
            "KLASKER_DB_HOST",
            "",
        )

        self.port = port or int(
            os.getenv(
                "KLASKER_DB_PORT",
                DEFAULT_PORT,
            )
        )

        self.user = user or os.getenv(
            "KLASKER_DB_USER",
            "",
        )

        self.password = (
            password
            if password is not None
            else os.getenv(
                "KLASKER_DB_PASSWORD",
                "",
            )
        )

        self.database = database or os.getenv(
            "KLASKER_DB_NAME",
            DEFAULT_DATABASE,
        )

        if ssl is None:
            ssl = (
                os.getenv(
                    "KLASKER_DB_SSL",
                    "true",
                ).lower()
                not in {
                    "0",
                    "false",
                    "no",
                    "off",
                }
            )

        self.ssl = ssl

        configured_ca = (
            ssl_ca
            if ssl_ca is not None
            else os.getenv(
                "KLASKER_DB_SSL_CA",
                "",
            ).strip()
        )

        if configured_ca:
            self.ssl_ca = configured_ca
        elif DEFAULT_SSL_CA.is_file():
            self.ssl_ca = str(DEFAULT_SSL_CA)
        else:
            self.ssl_ca = ""

    def validate(self) -> None:
        missing = []

        if not self.host:
            missing.append("KLASKER_DB_HOST")

        if not self.user:
            missing.append("KLASKER_DB_USER")

        if not self.password:
            missing.append("KLASKER_DB_PASSWORD")

        if self.ssl and not self.ssl_ca:
            missing.append(
                "KLASKER_DB_SSL_CA "
                "(or local certs/isrgrootx1.pem)"
            )

        if self.ssl and self.ssl_ca:
            ca_path = Path(self.ssl_ca).expanduser()

            if not ca_path.is_file():
                raise RuntimeError(
                    "TiDB TLS CA certificate was not found: "
                    f"{ca_path}"
                )

        if missing:
            raise RuntimeError(
                "Missing TiDB configuration: "
                + ", ".join(missing)
            )


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS schema_meta (
        schema_version INT NOT NULL,
        updated_at DATETIME(6) NOT NULL,
        PRIMARY KEY (schema_version)
    ) ENGINE=InnoDB
    """,

    """
    CREATE TABLE IF NOT EXISTS bot_state (
        id TINYINT NOT NULL,
        state VARCHAR(32) NOT NULL,
        checkpoint_id VARCHAR(80) NOT NULL,
        updated_at DATETIME(6) NOT NULL,
        PRIMARY KEY (id)
    ) ENGINE=InnoDB
    """,

    """
    CREATE TABLE IF NOT EXISTS checkpoints (
        checkpoint_id VARCHAR(80) NOT NULL,
        current_frontier_id VARCHAR(80) NULL,
        frontier_position BIGINT NOT NULL DEFAULT 0,
        discovery_depth INT NOT NULL DEFAULT 0,
        last_completed_operation TEXT NULL,
        updated_at DATETIME(6) NOT NULL,
        PRIMARY KEY (checkpoint_id),
        KEY idx_checkpoints_updated (updated_at)
    ) ENGINE=InnoDB
    """,

    """
    CREATE TABLE IF NOT EXISTS frontier (
        frontier_id VARCHAR(80) NOT NULL,
        url TEXT NOT NULL,
        url_hash CHAR(64) NOT NULL,
        domain VARCHAR(255) NOT NULL,
        source_url TEXT NULL,
        relationship VARCHAR(32) NOT NULL,
        priority TINYINT NOT NULL,
        status VARCHAR(32) NOT NULL,
        depth INT NOT NULL DEFAULT 0,
        discovered_at DATETIME(6) NOT NULL,
        last_attempted DATETIME(6) NULL,
        attempt_count INT NOT NULL DEFAULT 0,
        PRIMARY KEY (frontier_id),
        UNIQUE KEY uq_frontier_url_hash (url_hash),
        KEY idx_frontier_queue (
            status,
            priority,
            depth,
            discovered_at
        ),
        KEY idx_frontier_domain (domain)
    ) ENGINE=InnoDB
    """,

    """
    CREATE TABLE IF NOT EXISTS jobs (
        job_id VARCHAR(80) NOT NULL,
        job_type VARCHAR(32) NOT NULL,
        priority TINYINT NOT NULL,
        status VARCHAR(32) NOT NULL,
        target_url TEXT NULL,
        query_text TEXT NULL,
        created_at DATETIME(6) NOT NULL,
        claimed_by VARCHAR(32) NULL,
        claim_expires DATETIME(6) NULL,
        heartbeat_at DATETIME(6) NULL,
        started_at DATETIME(6) NULL,
        completed_at DATETIME(6) NULL,
        checkpoint_id VARCHAR(80) NULL,
        error_text TEXT NULL,
        PRIMARY KEY (job_id),
        KEY idx_jobs_queue (
            status,
            priority,
            created_at
        ),
        KEY idx_jobs_claim (
            claimed_by,
            status
        ),
        KEY idx_jobs_heartbeat (
            status,
            heartbeat_at
        )
    ) ENGINE=InnoDB
    """,

    """
    CREATE TABLE IF NOT EXISTS workers (
        worker_id VARCHAR(32) NOT NULL,
        status VARCHAR(16) NOT NULL,
        current_job_id VARCHAR(80) NULL,
        last_heartbeat DATETIME(6) NULL,
        PRIMARY KEY (worker_id)
    ) ENGINE=InnoDB
    """,

    """
    CREATE TABLE IF NOT EXISTS human_requests (
        request_id VARCHAR(80) NOT NULL,
        query_text TEXT NOT NULL,
        created_at DATETIME(6) NOT NULL,
        job_id VARCHAR(80) NULL,
        status VARCHAR(32) NOT NULL,
        result_summary TEXT NULL,
        websites_discovered JSON NULL,
        agent_capabilities_discovered JSON NULL,
        PRIMARY KEY (request_id),
        KEY idx_human_requests_created (created_at),
        KEY idx_human_requests_job (job_id)
    ) ENGINE=InnoDB
    """,
)


EXPECTED_TABLES = (
    "schema_meta",
    "bot_state",
    "checkpoints",
    "frontier",
    "jobs",
    "workers",
    "human_requests",
)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


class KlaskerDatabase:
    """Transactional persistence for the KlaskerBot runtime."""

    def __init__(
        self,
        config: Optional[DatabaseConfig] = None,
    ) -> None:
        self.config = config or DatabaseConfig()

    def connect(self) -> Connection:
        self.config.validate()

        ssl_args: dict[str, Any] = {}

        if self.config.ssl:
            ssl_args = {
                "ssl": {
                    "ca": str(
                        Path(
                            self.config.ssl_ca
                        ).expanduser()
                    ),
                },
                "ssl_verify_cert": True,
                "ssl_verify_identity": True,
            }

        return pymysql.connect(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            database=self.config.database,
            charset="utf8mb4",
            autocommit=False,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
            read_timeout=30,
            write_timeout=30,
            **ssl_args,
        )

    @contextmanager
    def transaction(self) -> Iterator[Connection]:
        """Open a transaction and commit or roll it back atomically."""

        connection = self.connect()

        try:
            yield connection
            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

    def initialise(self) -> None:
        """Create the schema and shared worker records."""

        with self.transaction() as connection:
            with connection.cursor() as cursor:

                for statement in SCHEMA_STATEMENTS:
                    cursor.execute(statement)

                cursor.execute(
                    """
                    INSERT INTO schema_meta (
                        schema_version,
                        updated_at
                    )
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                        updated_at = VALUES(updated_at)
                    """,
                    (
                        SCHEMA_VERSION,
                        to_db_time(utc_now()),
                    ),
                )

                for worker_id in (
                    "SVELTRON",
                    "CORSAIR",
                ):
                    cursor.execute(
                        """
                        INSERT INTO workers (
                            worker_id,
                            status,
                            current_job_id,
                            last_heartbeat
                        )
                        VALUES (%s, %s, NULL, NULL)
                        ON DUPLICATE KEY UPDATE
                            worker_id = VALUES(worker_id)
                        """,
                        (
                            worker_id,
                            WorkerStatus.OFFLINE.value,
                        ),
                    )

    def check(self) -> dict[str, Any]:
        """Verify the TiDB connection and KlaskerBot schema."""

        with self.connect() as connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    "SELECT VERSION(), DATABASE()"
                )

                version, database = cursor.fetchone().values()

                cursor.execute(
                    """
                    SELECT
                        schema_version,
                        updated_at
                    FROM schema_meta
                    ORDER BY schema_version DESC
                    LIMIT 1
                    """
                )

                schema_row = cursor.fetchone()

                cursor.execute(
                    """
                    SELECT COUNT(*) AS table_count
                    FROM information_schema.tables
                    WHERE table_schema = %s
                      AND table_name IN (
                          'schema_meta',
                          'bot_state',
                          'checkpoints',
                          'frontier',
                          'jobs',
                          'workers',
                          'human_requests'
                      )
                    """,
                    (self.config.database,),
                )

                table_row = cursor.fetchone()

                cursor.execute(
                    """
                    SELECT
                        worker_id,
                        status
                    FROM workers
                    ORDER BY worker_id
                    """
                )

                workers = cursor.fetchall()

        table_count = int(table_row["table_count"])

        return {
            "version": version,
            "database": database,
            "schema_version": (
                int(schema_row["schema_version"])
                if schema_row is not None
                else None
            ),
            "table_count": table_count,
            "expected_table_count": len(
                EXPECTED_TABLES
            ),
            "workers": workers,
        }

    # ------------------------------------------------------------------
    # Runtime / checkpoint
    # ------------------------------------------------------------------

    def save_checkpoint(
        self,
        checkpoint: Checkpoint,
        state: BotState,
    ) -> None:
        """Persist checkpoint and Bot state atomically."""

        with self.transaction() as connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    INSERT INTO checkpoints (
                        checkpoint_id,
                        current_frontier_id,
                        frontier_position,
                        discovery_depth,
                        last_completed_operation,
                        updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s
                    )
                    ON DUPLICATE KEY UPDATE
                        current_frontier_id =
                            VALUES(current_frontier_id),
                        frontier_position =
                            VALUES(frontier_position),
                        discovery_depth =
                            VALUES(discovery_depth),
                        last_completed_operation =
                            VALUES(last_completed_operation),
                        updated_at =
                            VALUES(updated_at)
                    """,
                    (
                        checkpoint.checkpoint_id,
                        checkpoint.current_frontier_id,
                        checkpoint.frontier_position,
                        checkpoint.discovery_depth,
                        checkpoint.last_completed_operation,
                        to_db_time(
                            checkpoint.updated_at
                        ),
                    ),
                )

                cursor.execute(
                    """
                    INSERT INTO bot_state (
                        id,
                        state,
                        checkpoint_id,
                        updated_at
                    )
                    VALUES (
                        1, %s, %s, %s
                    )
                    ON DUPLICATE KEY UPDATE
                        state = VALUES(state),
                        checkpoint_id =
                            VALUES(checkpoint_id),
                        updated_at =
                            VALUES(updated_at)
                    """,
                    (
                        state.value,
                        checkpoint.checkpoint_id,
                        to_db_time(utc_now()),
                    ),
                )

    def load_checkpoint(self) -> Optional[Checkpoint]:
        """Load the most recently updated checkpoint."""

        with self.connect() as connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    SELECT
                        checkpoint_id,
                        current_frontier_id,
                        frontier_position,
                        discovery_depth,
                        last_completed_operation,
                        updated_at
                    FROM checkpoints
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """
                )

                row = cursor.fetchone()

        if row is None:
            return None

        return Checkpoint(
            checkpoint_id=row["checkpoint_id"],
            current_frontier_id=row[
                "current_frontier_id"
            ],
            frontier_position=int(
                row["frontier_position"]
            ),
            discovery_depth=int(
                row["discovery_depth"]
            ),
            last_completed_operation=row[
                "last_completed_operation"
            ],
            updated_at=(
                from_db_time(row["updated_at"])
                or utc_now()
            ),
        )

    def load_state(self) -> Optional[BotState]:
        with self.connect() as connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    SELECT state
                    FROM bot_state
                    WHERE id = 1
                    """
                )

                row = cursor.fetchone()

        if row is None:
            return None

        return BotState(row["state"])

    # ------------------------------------------------------------------
    # Frontier
    # ------------------------------------------------------------------

    def upsert_frontier_item(
        self,
        item: FrontierItem,
    ) -> None:
        """Persist a frontier item without URL duplication."""

        url_hash = hashlib.sha256(
            item.url.encode("utf-8")
        ).hexdigest()

        with self.transaction() as connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    INSERT INTO frontier (
                        frontier_id,
                        url,
                        url_hash,
                        domain,
                        source_url,
                        relationship,
                        priority,
                        status,
                        depth,
                        discovered_at,
                        last_attempted,
                        attempt_count
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    )
                    ON DUPLICATE KEY UPDATE
                        source_url =
                            COALESCE(
                                VALUES(source_url),
                                source_url
                            ),
                        relationship =
                            VALUES(relationship),
                        priority =
                            LEAST(
                                priority,
                                VALUES(priority)
                            ),
                        status =
                            VALUES(status),
                        depth =
                            LEAST(
                                depth,
                                VALUES(depth)
                            ),
                        last_attempted =
                            VALUES(last_attempted),
                        attempt_count =
                            VALUES(attempt_count)
                    """,
                    (
                        item.frontier_id,
                        item.url,
                        url_hash,
                        item.domain,
                        item.source_url,
                        item.relationship.value,
                        int(item.priority),
                        item.status.value,
                        item.depth,
                        to_db_time(item.discovered_at),
                        to_db_time(item.last_attempted),
                        item.attempt_count,
                    ),
                )

    def claim_next_frontier(
        self,
        worker_id: str,
    ) -> Optional[FrontierItem]:
        """Atomically claim the next waiting frontier item.

        FOR UPDATE SKIP LOCKED prevents SVELTRON and CORSAIR
        from claiming the same frontier item.
        """

        with self.transaction() as connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    SELECT *
                    FROM frontier
                    WHERE status = %s
                    ORDER BY
                        priority ASC,
                        depth ASC,
                        discovered_at ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    """,
                    (
                        JobStatus.WAITING.value,
                    ),
                )

                row = cursor.fetchone()

                if row is None:
                    return None

                now = utc_now()

                cursor.execute(
                    """
                    UPDATE frontier
                    SET status = %s,
                        last_attempted = %s,
                        attempt_count =
                            attempt_count + 1
                    WHERE frontier_id = %s
                    """,
                    (
                        JobStatus.RUNNING.value,
                        to_db_time(now),
                        row["frontier_id"],
                    ),
                )

        row["status"] = JobStatus.RUNNING.value
        row["last_attempted"] = datetime.now(
            timezone.utc
        )
        row["attempt_count"] = (
            int(row["attempt_count"]) + 1
        )

        return self._frontier_from_row(row)

    def complete_frontier(
        self,
        frontier_id: str,
    ) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    UPDATE frontier
                    SET status = %s
                    WHERE frontier_id = %s
                      AND status = %s
                    """,
                    (
                        JobStatus.COMPLETED.value,
                        frontier_id,
                        JobStatus.RUNNING.value,
                    ),
                )

                if cursor.rowcount != 1:
                    raise RuntimeError(
                        "Frontier item is not running"
                    )

    def fail_frontier(
        self,
        frontier_id: str,
    ) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    UPDATE frontier
                    SET status = %s
                    WHERE frontier_id = %s
                      AND status = %s
                    """,
                    (
                        JobStatus.FAILED.value,
                        frontier_id,
                        JobStatus.RUNNING.value,
                    ),
                )

                if cursor.rowcount != 1:
                    raise RuntimeError(
                        "Frontier item is not running"
                    )

    # ------------------------------------------------------------------
    # Jobs
    # ------------------------------------------------------------------

    def save_job(
        self,
        job: Job,
    ) -> None:
        with self.transaction() as connection:
            self._save_job_cursor(
                connection,
                job,
            )

    def _save_job_cursor(
        self,
        connection: Connection,
        job: Job,
    ) -> None:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO jobs (
                    job_id,
                    job_type,
                    priority,
                    status,
                    target_url,
                    query_text,
                    created_at,
                    claimed_by,
                    claim_expires,
                    heartbeat_at,
                    started_at,
                    completed_at,
                    checkpoint_id,
                    error_text
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    job_type =
                        VALUES(job_type),
                    priority =
                        VALUES(priority),
                    status =
                        VALUES(status),
                    target_url =
                        VALUES(target_url),
                    query_text =
                        VALUES(query_text),
                    claimed_by =
                        VALUES(claimed_by),
                    claim_expires =
                        VALUES(claim_expires),
                    heartbeat_at =
                        VALUES(heartbeat_at),
                    started_at =
                        VALUES(started_at),
                    completed_at =
                        VALUES(completed_at),
                    checkpoint_id =
                        VALUES(checkpoint_id),
                    error_text =
                        VALUES(error_text)
                """,
                (
                    job.job_id,
                    job.job_type.value,
                    int(job.priority),
                    job.status.value,
                    job.target_url,
                    job.query,
                    to_db_time(job.created_at),
                    job.claimed_by,
                    to_db_time(job.claim_expires),
                    to_db_time(job.heartbeat_at),
                    to_db_time(job.started_at),
                    to_db_time(job.completed_at),
                    job.checkpoint_id,
                    job.error,
                ),
            )

    def claim_next_job(
        self,
        worker_id: str,
        claim_seconds: int = 120,
    ) -> Optional[Job]:
        """Atomically claim the highest-priority waiting job."""

        now = datetime.now(timezone.utc)
        expires = now + timedelta(
            seconds=claim_seconds
        )

        with self.transaction() as connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    SELECT *
                    FROM jobs
                    WHERE status = %s
                    ORDER BY
                        priority ASC,
                        created_at ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    """,
                    (
                        JobStatus.WAITING.value,
                    ),
                )

                row = cursor.fetchone()

                if row is None:
                    return None

                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = %s,
                        claimed_by = %s,
                        claim_expires = %s,
                        heartbeat_at = %s
                    WHERE job_id = %s
                    """,
                    (
                        JobStatus.CLAIMED.value,
                        worker_id,
                        to_db_time(
                            expires.isoformat()
                        ),
                        to_db_time(
                            now.isoformat()
                        ),
                        row["job_id"],
                    ),
                )

                cursor.execute(
                    """
                    UPDATE workers
                    SET status = %s,
                        current_job_id = %s,
                        last_heartbeat = %s
                    WHERE worker_id = %s
                    """,
                    (
                        WorkerStatus.BUSY.value,
                        row["job_id"],
                        to_db_time(
                            now.isoformat()
                        ),
                        worker_id,
                    ),
                )

                row["status"] = JobStatus.CLAIMED.value
                row["claimed_by"] = worker_id
                row["claim_expires"] = (
                    expires.replace(tzinfo=None)
                )
                row["heartbeat_at"] = (
                    now.replace(tzinfo=None)
                )

        return self._job_from_row(row)

    def recover_expired_jobs(self) -> int:
        """Return expired claims to WAITING."""

        now = to_db_time(utc_now())

        with self.transaction() as connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = %s,
                        claimed_by = NULL,
                        claim_expires = NULL,
                        heartbeat_at = NULL
                    WHERE status IN (%s, %s)
                      AND claim_expires IS NOT NULL
                      AND claim_expires < %s
                    """,
                    (
                        JobStatus.WAITING.value,
                        JobStatus.CLAIMED.value,
                        JobStatus.RUNNING.value,
                        now,
                    ),
                )

                return cursor.rowcount

    def heartbeat_job(
        self,
        worker_id: str,
        job_id: str,
        claim_seconds: int = 120,
    ) -> None:
        """Refresh the job claim and worker heartbeat."""

        now = datetime.now(timezone.utc)

        expires = now + timedelta(
            seconds=claim_seconds
        )

        with self.transaction() as connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    UPDATE jobs
                    SET heartbeat_at = %s,
                        claim_expires = %s
                    WHERE job_id = %s
                      AND claimed_by = %s
                      AND status IN (%s, %s)
                    """,
                    (
                        to_db_time(
                            now.isoformat()
                        ),
                        to_db_time(
                            expires.isoformat()
                        ),
                        job_id,
                        worker_id,
                        JobStatus.CLAIMED.value,
                        JobStatus.RUNNING.value,
                    ),
                )

                if cursor.rowcount != 1:
                    raise RuntimeError(
                        "Worker does not own the active job"
                    )

                cursor.execute(
                    """
                    UPDATE workers
                    SET last_heartbeat = %s
                    WHERE worker_id = %s
                      AND current_job_id = %s
                    """,
                    (
                        to_db_time(
                            now.isoformat()
                        ),
                        worker_id,
                        job_id,
                    ),
                )

    def start_job(
        self,
        worker_id: str,
        job_id: str,
    ) -> None:
        """Move a claimed job into RUNNING."""

        with self.transaction() as connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = %s,
                        started_at = %s
                    WHERE job_id = %s
                      AND claimed_by = %s
                      AND status = %s
                    """,
                    (
                        JobStatus.RUNNING.value,
                        to_db_time(utc_now()),
                        job_id,
                        worker_id,
                        JobStatus.CLAIMED.value,
                    ),
                )

                if cursor.rowcount != 1:
                    raise RuntimeError(
                        "Worker cannot start this job"
                    )

    def complete_job(
        self,
        worker_id: str,
        job_id: str,
    ) -> None:
        self._finish_job(
            worker_id,
            job_id,
            JobStatus.COMPLETED,
            None,
        )

    def fail_job(
        self,
        worker_id: str,
        job_id: str,
        error: str,
    ) -> None:
        self._finish_job(
            worker_id,
            job_id,
            JobStatus.FAILED,
            error,
        )

    def _finish_job(
        self,
        worker_id: str,
        job_id: str,
        status: JobStatus,
        error: Optional[str],
    ) -> None:
        now = to_db_time(utc_now())

        with self.transaction() as connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = %s,
                        completed_at = %s,
                        error_text = %s,
                        claim_expires = NULL
                    WHERE job_id = %s
                      AND claimed_by = %s
                    """,
                    (
                        status.value,
                        now,
                        error,
                        job_id,
                        worker_id,
                    ),
                )

                if cursor.rowcount != 1:
                    raise RuntimeError(
                        "Worker does not own the job"
                    )

                cursor.execute(
                    """
                    UPDATE workers
                    SET status = %s,
                        current_job_id = NULL,
                        last_heartbeat = %s
                    WHERE worker_id = %s
                    """,
                    (
                        WorkerStatus.IDLE.value,
                        now,
                        worker_id,
                    ),
                )

    # ------------------------------------------------------------------
    # Workers
    # ------------------------------------------------------------------

    def set_worker_status(
        self,
        worker_id: str,
        status: WorkerStatus,
        current_job_id: Optional[str] = None,
    ) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    INSERT INTO workers (
                        worker_id,
                        status,
                        current_job_id,
                        last_heartbeat
                    )
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        status =
                            VALUES(status),
                        current_job_id =
                            VALUES(current_job_id),
                        last_heartbeat =
                            VALUES(last_heartbeat)
                    """,
                    (
                        worker_id,
                        status.value,
                        current_job_id,
                        to_db_time(utc_now()),
                    ),
                )

    def load_workers(self) -> dict[str, Worker]:
        with self.connect() as connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    SELECT *
                    FROM workers
                    """
                )

                rows = cursor.fetchall()

        return {
            row["worker_id"]: Worker(
                worker_id=row["worker_id"],
                status=WorkerStatus(
                    row["status"]
                ),
                current_job_id=row[
                    "current_job_id"
                ],
                last_heartbeat=from_db_time(
                    row["last_heartbeat"]
                ),
            )
            for row in rows
        }

    # ------------------------------------------------------------------
    # Human requests
    # ------------------------------------------------------------------

    def save_human_request(
        self,
        request: HumanRequest,
    ) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    INSERT INTO human_requests (
                        request_id,
                        query_text,
                        created_at,
                        job_id,
                        status,
                        result_summary,
                        websites_discovered,
                        agent_capabilities_discovered
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON DUPLICATE KEY UPDATE
                        query_text =
                            VALUES(query_text),
                        job_id =
                            VALUES(job_id),
                        status =
                            VALUES(status),
                        result_summary =
                            VALUES(result_summary),
                        websites_discovered =
                            VALUES(websites_discovered),
                        agent_capabilities_discovered =
                            VALUES(
                                agent_capabilities_discovered
                            )
                    """,
                    (
                        request.request_id,
                        request.query,
                        to_db_time(
                            request.created_at
                        ),
                        request.job_id,
                        request.status.value,
                        request.result_summary,
                        json.dumps(
                            request.websites_discovered
                        ),
                        json.dumps(
                            request.agent_capabilities_discovered
                        ),
                    ),
                )

    # ------------------------------------------------------------------
    # Runtime loading
    # ------------------------------------------------------------------

    def load_runtime(self) -> BotRuntime:
        """Load the persistent runtime needed to resume discovery."""

        runtime = BotRuntime()

        state = self.load_state()
        checkpoint = self.load_checkpoint()

        if state is not None:
            runtime.state = state

        if checkpoint is not None:
            runtime.checkpoint = checkpoint

        runtime.workers = self.load_workers()

        self.recover_expired_jobs()

        return runtime

    # ------------------------------------------------------------------
    # Row conversion helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _frontier_from_row(
        row: dict[str, Any],
    ) -> FrontierItem:
        return FrontierItem(
            frontier_id=row["frontier_id"],
            url=row["url"],
            domain=row["domain"],
            source_url=row["source_url"],
            relationship=DiscoveryRelationship(
                row["relationship"]
            ),
            priority=JobPriority(
                int(row["priority"])
            ),
            status=JobStatus(
                row["status"]
            ),
            depth=int(row["depth"]),
            discovered_at=(
                from_db_time(
                    row["discovered_at"]
                )
                or utc_now()
            ),
            last_attempted=from_db_time(
                row["last_attempted"]
            ),
            attempt_count=int(
                row["attempt_count"]
            ),
        )

    @staticmethod
    def _job_from_row(
        row: dict[str, Any],
    ) -> Job:
        return Job(
            job_id=row["job_id"],
            job_type=JobType(
                row["job_type"]
            ),
            priority=JobPriority(
                int(row["priority"])
            ),
            status=JobStatus(
                row["status"]
            ),
            target_url=row["target_url"],
            query=row["query_text"],
            created_at=(
                from_db_time(
                    row["created_at"]
                )
                or utc_now()
            ),
            claimed_by=row["claimed_by"],
            claim_expires=from_db_time(
                row["claim_expires"]
            ),
            heartbeat_at=from_db_time(
                row["heartbeat_at"]
            ),
            started_at=from_db_time(
                row["started_at"]
            ),
            completed_at=from_db_time(
                row["completed_at"]
            ),
            checkpoint_id=row["checkpoint_id"],
            error=row["error_text"],
        )


# ---------------------------------------------------------------------------
# Command-line interface
# ---------------------------------------------------------------------------


def print_configuration(
    config: DatabaseConfig,
) -> None:
    """Print non-secret database configuration."""

    print("KlaskerBot TiDB database module")
    print(
        "Host configured:",
        "yes" if config.host else "no",
    )
    print(
        "Port:",
        config.port,
    )
    print(
        "Database:",
        config.database,
    )
    print(
        "TLS:",
        "enabled" if config.ssl else "disabled",
    )

    if config.ssl:
        print(
            "TLS CA:",
            config.ssl_ca or "not configured",
        )

    print(
        "Schema version:",
        SCHEMA_VERSION,
    )


def build_argument_parser() -> argparse.ArgumentParser:
    """Build the database command-line parser."""

    parser = argparse.ArgumentParser(
        description="KlaskerBot TiDB database management."
    )

    group = parser.add_mutually_exclusive_group()

    group.add_argument(
        "--init",
        action="store_true",
        help="Initialise the KlaskerBot database schema.",
    )

    group.add_argument(
        "--check",
        action="store_true",
        help="Check the TiDB connection and schema.",
    )

    return parser


def main() -> int:
    """Run the database command-line interface."""

    parser = build_argument_parser()
    args = parser.parse_args()

    config = DatabaseConfig()
    print_configuration(config)

    if not args.init and not args.check:
        print(
            "No database connection was opened by this self-test."
        )
        return 0

    database = KlaskerDatabase(config)

    try:
        if args.init:
            print()
            print("Initialising KlaskerBot database schema...")

            database.initialise()

            print("SUCCESS")
            print(
                "KlaskerBot database schema initialised."
            )

            return 0

        result = database.check()

        print()
        print("Database connection: OK")
        print(
            f"TiDB version: {result['version']}"
        )
        print(
            f"Database: {result['database']}"
        )
        print(
            "Schema version:",
            result["schema_version"],
        )
        print(
            "Tables:",
            f"{result['table_count']}/"
            f"{result['expected_table_count']}",
        )

        if (
            result["table_count"]
            != result["expected_table_count"]
        ):
            print(
                "WARNING: Expected schema tables are missing."
            )
            return 1

        print("Workers:")

        for worker in result["workers"]:
            print(
                f"  {worker['worker_id']}: "
                f"{worker['status']}"
            )

        print("SUCCESS")
        print(
            "KlaskerBot database check completed."
        )

        return 0

    except Exception as exc:
        print()
        print("ERROR")
        print(str(exc))
        return 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    raise SystemExit(main())
