# Path: ~/klasker-bot/scheduler.py
"""Priority scheduler and worker coordination for KlaskerBot.

The scheduler sits between the KlaskerBot state model and the TiDB
persistence layer. It coordinates SVELTRON and CORSAIR without performing
network discovery itself.

Key guarantees:
    * P0 human jobs always outrank automatic discovery work.
    * TiDB performs atomic job claiming with row locks.
    * Expired claims can be recovered by either engine.
    * Heartbeats extend ownership while work is running.
    * A human interruption is cooperative: the current atomic operation
      finishes and its checkpoint is persisted before the human job runs.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from database import KlaskerDatabase
from klaskerbot import (
    BotState,
    Checkpoint,
    Job,
    JobPriority,
    JobStatus,
    JobType,
    WorkerStatus,
)


WORKERS = ("SVELTRON", "CORSAIR")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class SchedulerConfig:
    """Timing configuration for distributed job ownership."""

    claim_seconds: int = 120
    heartbeat_seconds: int = 30
    worker_timeout_seconds: int = 90
    recovery_interval_seconds: int = 30


@dataclass(frozen=True)
class SchedulerClaim:
    """A worker's persistent claim on a job."""

    worker_id: str
    job: Job


class KlaskerScheduler:
    """Coordinate the two KlaskerBot engines through TiDB."""

    WORKERS = WORKERS

    def __init__(
        self,
        database: KlaskerDatabase,
        config: Optional[SchedulerConfig] = None,
    ) -> None:
        self.database = database
        self.config = config or SchedulerConfig()

    # ------------------------------------------------------------------
    # Worker lifecycle
    # ------------------------------------------------------------------

    def register(self, worker_id: str) -> None:
        self._validate_worker(worker_id)
        self.database.set_worker_status(
            worker_id,
            WorkerStatus.IDLE,
            None,
        )

    def mark_offline(self, worker_id: str) -> None:
        self._validate_worker(worker_id)
        self.database.set_worker_status(
            worker_id,
            WorkerStatus.OFFLINE,
            None,
        )

    def heartbeat(self, worker_id: str) -> None:
        self._validate_worker(worker_id)
        workers = self.database.load_workers()
        worker = workers.get(worker_id)

        if worker is None:
            raise RuntimeError(
                f"Unknown worker: {worker_id}"
            )

        self.database.set_worker_status(
            worker_id,
            worker.status,
            worker.current_job_id,
        )

    def heartbeat_job(
        self,
        worker_id: str,
        job_id: str,
    ) -> None:
        self._validate_worker(worker_id)
        self.database.heartbeat_job(
            worker_id,
            job_id,
            self.config.claim_seconds,
        )

    # ------------------------------------------------------------------
    # Queue recovery / claiming
    # ------------------------------------------------------------------

    def recover_expired_claims(self) -> int:
        return self.database.recover_expired_jobs()

    def claim_next(
        self,
        worker_id: str,
    ) -> Optional[SchedulerClaim]:
        self._validate_worker(worker_id)
        self.recover_expired_claims()

        job = self.database.claim_next_job(
            worker_id,
            self.config.claim_seconds,
        )

        if job is None:
            return None

        return SchedulerClaim(
            worker_id=worker_id,
            job=job,
        )

    def claim_human_job(
        self,
        worker_id: str,
    ) -> Optional[SchedulerClaim]:
        """Claim a waiting P0 human job.

        The database queue is already ordered by priority, so this method
        verifies that the claimed job is actually a human job. If a future
        database implementation supports an explicit priority predicate,
        this method can use it without changing the scheduler API.
        """

        claim = self.claim_next(worker_id)

        if claim is None:
            return None

        if (
            claim.job.priority != JobPriority.HUMAN
            or claim.job.job_type != JobType.HUMAN_SEARCH
        ):
            self.release(
                worker_id,
                claim.job.job_id,
            )
            return None

        return claim

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    def start(
        self,
        claim: SchedulerClaim,
    ) -> None:
        self._require_ownership(claim)
        self.database.start_job(
            claim.worker_id,
            claim.job.job_id,
        )

    def complete(
        self,
        claim: SchedulerClaim,
    ) -> None:
        self._require_ownership(claim)
        self.database.complete_job(
            claim.worker_id,
            claim.job.job_id,
        )

    def fail(
        self,
        claim: SchedulerClaim,
        error: str,
    ) -> None:
        self._require_ownership(claim)
        self.database.fail_job(
            claim.worker_id,
            claim.job.job_id,
            error,
        )

    def release(
        self,
        worker_id: str,
        job_id: str,
    ) -> None:
        """Return a claimed job to WAITING without completing it."""

        self._validate_worker(worker_id)

        with self.database.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = %s,
                        claimed_by = NULL,
                        claim_expires = NULL,
                        heartbeat_at = NULL
                    WHERE job_id = %s
                      AND claimed_by = %s
                      AND status IN (%s, %s)
                    """,
                    (
                        JobStatus.WAITING.value,
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
                    SET status = %s,
                        current_job_id = NULL,
                        last_heartbeat = %s
                    WHERE worker_id = %s
                      AND current_job_id = %s
                    """,
                    (
                        WorkerStatus.IDLE.value,
                        datetime.now(timezone.utc).strftime(
                            "%Y-%m-%d %H:%M:%S.%f"
                        ),
                        worker_id,
                        job_id,
                    ),
                )

    # ------------------------------------------------------------------
    # Cooperative human interruption
    # ------------------------------------------------------------------

    def prepare_human_interruption(
        self,
        checkpoint: Checkpoint,
    ) -> None:
        """Persist the exact automatic-discovery stopping point."""

        self.database.save_checkpoint(
            checkpoint,
            BotState.PAUSED,
        )

    def enter_human_job(
        self,
        worker_id: str,
        checkpoint: Checkpoint,
    ) -> Optional[SchedulerClaim]:
        """Persist a safe checkpoint, then claim the highest-priority job."""

        self._validate_worker(worker_id)
        self.prepare_human_interruption(checkpoint)
        return self.claim_human_job(worker_id)

    # ------------------------------------------------------------------
    # Health / status
    # ------------------------------------------------------------------

    def worker_is_stale(
        self,
        worker_id: str,
    ) -> bool:
        self._validate_worker(worker_id)
        worker = self.database.load_workers().get(worker_id)

        if worker is None or worker.last_heartbeat is None:
            return True

        heartbeat = datetime.fromisoformat(
            worker.last_heartbeat
        )

        if heartbeat.tzinfo is None:
            heartbeat = heartbeat.replace(
                tzinfo=timezone.utc
            )

        return (
            datetime.now(timezone.utc) - heartbeat
            > timedelta(
                seconds=self.config.worker_timeout_seconds
            )
        )

    def status(self) -> dict[str, object]:
        workers = self.database.load_workers()

        return {
            "workers": {
                worker_id: {
                    "status": worker.status.value,
                    "current_job_id": worker.current_job_id,
                    "last_heartbeat": worker.last_heartbeat,
                    "stale": self.worker_is_stale(worker_id),
                }
                for worker_id, worker in workers.items()
            },
            "config": {
                "claim_seconds": self.config.claim_seconds,
                "heartbeat_seconds": self.config.heartbeat_seconds,
                "worker_timeout_seconds": self.config.worker_timeout_seconds,
                "recovery_interval_seconds": self.config.recovery_interval_seconds,
            },
        }

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @classmethod
    def _validate_worker(cls, worker_id: str) -> None:
        if worker_id not in cls.WORKERS:
            raise ValueError(
                f"Unknown KlaskerBot worker: {worker_id}"
            )

    @staticmethod
    def _require_ownership(
        claim: SchedulerClaim,
    ) -> None:
        if claim.job.claimed_by != claim.worker_id:
            raise RuntimeError(
                "Scheduler claim is not owned by the supplied worker"
            )


# ---------------------------------------------------------------------------
# Minimal local self-test
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    config = SchedulerConfig()

    print("KlaskerBot scheduler")
    print("Workers:", ", ".join(WORKERS))
    print("Claim timeout:", config.claim_seconds, "seconds")
    print("Heartbeat interval:", config.heartbeat_seconds, "seconds")
    print("Worker timeout:", config.worker_timeout_seconds, "seconds")
    print("Human priority:", JobPriority.HUMAN.value)
    print("No database connection was opened by this self-test.")
