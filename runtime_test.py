# Path: ~/klasker-bot/runtime_test.py

"""
KlaskerBot two-worker runtime integration test.

Verifies the real TiDB-backed scheduler behaviour:

    1. SVELTRON and CORSAIR register successfully.
    2. A temporary job is persisted to TiDB.
    3. Both workers attempt to claim it concurrently.
    4. Exactly one worker obtains the claim.
    5. The claim expires.
    6. The other worker recovers and claims the job.
    7. The recovered worker starts and completes the job.
    8. The temporary job is removed.

Run from ~/klasker-bot:

    python3 runtime_test.py

This test deliberately uses separate KlaskerDatabase instances for the
two workers so that each worker has its own TiDB connection.
"""

from __future__ import annotations

import threading
import time
import uuid

from database import KlaskerDatabase
from klaskerbot import Job, JobPriority, JobStatus, JobType, WorkerStatus
from scheduler import KlaskerScheduler, SchedulerConfig


WORKER_1 = "SVELTRON"
WORKER_2 = "CORSAIR"

CLAIM_SECONDS = 1


def test_database_connection(database: KlaskerDatabase) -> None:
    """Confirm that the TiDB schema is available."""

    result = database.check()

    expected = result["expected_table_count"]
    actual = result["table_count"]

    assert actual == expected, (
        f"Expected {expected} tables, found {actual}"
    )

    print("PASS  TiDB connection and schema")


def cleanup_job(
    database: KlaskerDatabase,
    job_id: str,
) -> None:
    """Remove the temporary test job and reset worker records."""

    with database.transaction() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM jobs WHERE job_id = %s",
                (job_id,),
            )

            cursor.execute(
                """
                UPDATE workers
                SET status = %s,
                    current_job_id = NULL
                WHERE worker_id IN (%s, %s)
                """,
                (
                    WorkerStatus.OFFLINE.value,
                    WORKER_1,
                    WORKER_2,
                ),
            )


def main() -> None:
    database = KlaskerDatabase()

    test_database_connection(database)

    scheduler_1 = KlaskerScheduler(
        KlaskerDatabase(),
        SchedulerConfig(
            claim_seconds=CLAIM_SECONDS,
            heartbeat_seconds=1,
            worker_timeout_seconds=5,
            recovery_interval_seconds=1,
        ),
    )

    scheduler_2 = KlaskerScheduler(
        KlaskerDatabase(),
        SchedulerConfig(
            claim_seconds=CLAIM_SECONDS,
            heartbeat_seconds=1,
            worker_timeout_seconds=5,
            recovery_interval_seconds=1,
        ),
    )

    job_id = f"runtime-test-{uuid.uuid4().hex}"

    job = Job(
        job_id=job_id,
        job_type=JobType.DISCOVERY,
        priority=JobPriority.FRONTIER_EXPANSION,
        status=JobStatus.WAITING,
        target_url="https://runtime-test.invalid/",
        query="KlaskerBot runtime integration test",
    )

    try:
        # --------------------------------------------------------------
        # Worker registration
        # --------------------------------------------------------------

        scheduler_1.register(WORKER_1)
        scheduler_2.register(WORKER_2)

        print("PASS  SVELTRON registered")
        print("PASS  CORSAIR registered")

        # --------------------------------------------------------------
        # Persist temporary job
        # --------------------------------------------------------------

        database.save_job(job)

        loaded_runtime = database.load_runtime()

        loaded_job = next(
            (
                item
                for item in loaded_runtime.jobs
                if item.job_id == job_id
            ),
            None,
        )

        assert loaded_job is not None, (
            "Temporary job was not reconstructed by load_runtime()"
        )

        assert loaded_job.status == JobStatus.WAITING

        print("PASS  Job persisted and reconstructed")

        # --------------------------------------------------------------
        # Concurrent claim
        # --------------------------------------------------------------

        barrier = threading.Barrier(2)

        claims: dict[str, object] = {}
        errors: list[Exception] = []

        def claim_worker(
            name: str,
            scheduler: KlaskerScheduler,
        ) -> None:
            try:
                barrier.wait()

                claims[name] = scheduler.claim_next(name)

            except Exception as exc:
                errors.append(exc)

        thread_1 = threading.Thread(
            target=claim_worker,
            args=(WORKER_1, scheduler_1),
        )

        thread_2 = threading.Thread(
            target=claim_worker,
            args=(WORKER_2, scheduler_2),
        )

        thread_1.start()
        thread_2.start()

        thread_1.join()
        thread_2.join()

        assert not errors, (
            f"Concurrent claim failed: {errors}"
        )

        claim_1 = claims.get(WORKER_1)
        claim_2 = claims.get(WORKER_2)

        winners = [
            name
            for name, claim in claims.items()
            if claim is not None
        ]

        assert len(winners) == 1, (
            "Atomic claim failed: "
            f"expected exactly one winner, got {winners}"
        )

        winner = winners[0]
        loser = (
            WORKER_2
            if winner == WORKER_1
            else WORKER_1
        )

        winning_claim = (
            claim_1
            if winner == WORKER_1
            else claim_2
        )

        assert winning_claim is not None

        assert winning_claim.worker_id == winner
        assert winning_claim.job.job_id == job_id
        assert winning_claim.job.status == JobStatus.CLAIMED

        print(
            "PASS  Concurrent claim: "
            f"{winner} won, {loser} received no claim"
        )

        # --------------------------------------------------------------
        # Verify persistent ownership
        # --------------------------------------------------------------

        runtime_after_claim = database.load_runtime()

        claimed_job = next(
            (
                item
                for item in runtime_after_claim.jobs
                if item.job_id == job_id
            ),
            None,
        )

        assert claimed_job is not None
        assert claimed_job.status == JobStatus.CLAIMED
        assert claimed_job.claimed_by == winner

        print("PASS  TiDB records the winning worker")

        # --------------------------------------------------------------
        # Let the claim expire
        # --------------------------------------------------------------

        time.sleep(CLAIM_SECONDS + 1)

        recovered = scheduler_2.recover_expired_claims()

        assert recovered >= 1, (
            "Expired job was not returned to WAITING"
        )

        print("PASS  Expired claim recovered")

        # --------------------------------------------------------------
        # Other worker claims recovered job
        # --------------------------------------------------------------

        recovery_scheduler = (
            scheduler_2
            if loser == WORKER_2
            else scheduler_1
        )

        recovered_claim = recovery_scheduler.claim_next(
            loser
        )

        assert recovered_claim is not None
        assert recovered_claim.worker_id == loser
        assert recovered_claim.job.job_id == job_id
        assert recovered_claim.job.status == JobStatus.CLAIMED

        print(
            "PASS  Recovery claim: "
            f"{loser} successfully claimed the expired job"
        )

        # --------------------------------------------------------------
        # Complete recovered job
        # --------------------------------------------------------------

        recovery_scheduler.start(recovered_claim)

        print("PASS  Recovered job entered RUNNING")

        recovery_scheduler.complete(recovered_claim)

        print("PASS  Recovered job completed")

        # --------------------------------------------------------------
        # Verify final persistent state
        # --------------------------------------------------------------

        final_runtime = database.load_runtime()

        final_job = next(
            (
                item
                for item in final_runtime.jobs
                if item.job_id == job_id
            ),
            None,
        )

        assert final_job is not None
        assert final_job.status == JobStatus.COMPLETED
        assert final_job.claimed_by == loser

        workers = database.load_workers()

        assert workers[loser].status == WorkerStatus.IDLE
        assert workers[loser].current_job_id is None

        print("PASS  Final TiDB state verified")

        print()
        print("Runtime integration test: PASS")
        print()
        print(
            "Verified:"
        )
        print(
            "  - shared TiDB job queue"
        )
        print(
            "  - atomic two-worker claiming"
        )
        print(
            "  - exclusive job ownership"
        )
        print(
            "  - claim expiry"
        )
        print(
            "  - expired-claim recovery"
        )
        print(
            "  - cross-worker recovery"
        )
        print(
            "  - RUNNING transition"
        )
        print(
            "  - COMPLETED transition"
        )
        print(
            "  - persistent final state"
        )

    finally:
        cleanup_job(database, job_id)


if __name__ == "__main__":
    main()
