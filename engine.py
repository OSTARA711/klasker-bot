# Path: ~/klasker-bot/engine.py
"""Long-running KlaskerBot worker engine.

Each Serv00 installation runs one instance of this engine. The engine does
not contain discovery logic itself; it coordinates scheduling, persistence,
heartbeats, cooperative interruption, and the eventual discovery worker.

The same code runs on SVELTRON and CORSAIR. TiDB is the shared source of
truth, so either engine can recover work whose claim has expired.
"""

from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass
from typing import Optional

from database import KlaskerDatabase
from klaskerbot import (
    BotState,
    Checkpoint,
    JobType,
)
from scheduler import KlaskerScheduler, SchedulerClaim, SchedulerConfig


LOGGER = logging.getLogger("klaskerbot.engine")


@dataclass(frozen=True)
class EngineConfig:
    """Runtime configuration for one KlaskerBot engine."""

    worker_id: str
    idle_sleep_seconds: float = 5.0
    heartbeat_seconds: int = 30
    shutdown_grace_seconds: int = 30


class KlaskerEngine:
    """Run one persistent KlaskerBot worker."""

    def __init__(
        self,
        database: KlaskerDatabase,
        config: EngineConfig,
        scheduler: Optional[KlaskerScheduler] = None,
    ) -> None:
        self.database = database
        self.config = config
        self.scheduler = scheduler or KlaskerScheduler(
            database,
            SchedulerConfig(
                heartbeat_seconds=config.heartbeat_seconds,
            ),
        )
        self._running = False
        self._shutdown_requested = False
        self._last_heartbeat = 0.0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Run until a graceful shutdown is requested."""

        self._install_signal_handlers()
        self.scheduler.register(self.config.worker_id)
        self._running = True
        self._shutdown_requested = False

        LOGGER.info(
            "KlaskerBot engine started: %s",
            self.config.worker_id,
        )

        try:
            while self._running:
                self._heartbeat_if_due()

                if self._shutdown_requested:
                    break

                claim = self.scheduler.claim_next(
                    self.config.worker_id,
                )

                if claim is None:
                    self._sleep(self.config.idle_sleep_seconds)
                    continue

                self._execute_claim(claim)

        finally:
            self._shutdown()

    def request_shutdown(self, *_signal_args: object) -> None:
        """Request a cooperative shutdown after the current atomic job."""

        LOGGER.info(
            "Shutdown requested for %s",
            self.config.worker_id,
        )
        self._shutdown_requested = True

    def _shutdown(self) -> None:
        if not self._running:
            return

        self._running = False

        try:
            self.scheduler.mark_offline(self.config.worker_id)
        except Exception:
            LOGGER.exception("Failed to mark worker offline")

        LOGGER.info(
            "KlaskerBot engine stopped: %s",
            self.config.worker_id,
        )

    # ------------------------------------------------------------------
    # Job execution
    # ------------------------------------------------------------------

    def _execute_claim(self, claim: SchedulerClaim) -> None:
        """Execute exactly one claimed atomic operation."""

        LOGGER.info(
            "Claimed job %s (%s) on %s",
            claim.job.job_id,
            claim.job.job_type.value,
            claim.worker_id,
        )

        try:
            self.scheduler.start(claim)

            if claim.job.job_type == JobType.HUMAN_SEARCH:
                self._run_human_search(claim)
            elif claim.job.job_type == JobType.WEBSITE_ANALYSIS:
                self._run_website_analysis(claim)
            elif claim.job.job_type == JobType.DISCOVERY:
                self._run_discovery(claim)
            elif claim.job.job_type == JobType.REVERIFICATION:
                self._run_reverification(claim)
            else:
                raise RuntimeError(
                    f"Unsupported job type: {claim.job.job_type}"
                )

            self.scheduler.complete(claim)

            LOGGER.info(
                "Completed job %s on %s",
                claim.job.job_id,
                claim.worker_id,
            )

        except Exception as exc:
            LOGGER.exception(
                "Job %s failed on %s",
                claim.job.job_id,
                claim.worker_id,
            )
            self.scheduler.fail(
                claim,
                str(exc),
            )

    def _run_human_search(self, claim: SchedulerClaim) -> None:
        """Execute a P0 human request.

        Actual search/analysis is deliberately delegated to the discovery
        layer. This method currently provides the execution boundary and
        heartbeat discipline needed by that layer.
        """

        self._heartbeat(force=True)
        LOGGER.info(
            "Human search boundary reached for job %s",
            claim.job.job_id,
        )

    def _run_website_analysis(self, claim: SchedulerClaim) -> None:
        """Execute a website-analysis job through the discovery layer."""

        self._heartbeat(force=True)
        LOGGER.info(
            "Website analysis boundary reached for job %s target=%s",
            claim.job.job_id,
            claim.job.target_url,
        )

    def _run_discovery(self, claim: SchedulerClaim) -> None:
        """Execute one atomic automatic-discovery operation."""

        self._heartbeat(force=True)
        LOGGER.info(
            "Discovery boundary reached for job %s target=%s",
            claim.job.job_id,
            claim.job.target_url,
        )

    def _run_reverification(self, claim: SchedulerClaim) -> None:
        """Execute one re-verification operation."""

        self._heartbeat(force=True)
        LOGGER.info(
            "Reverification boundary reached for job %s target=%s",
            claim.job.job_id,
        )

    # ------------------------------------------------------------------
    # Checkpoint / interruption support
    # ------------------------------------------------------------------

    def prepare_human_interruption(
        self,
        checkpoint: Checkpoint,
    ) -> None:
        """Persist the current automatic-discovery checkpoint safely."""

        self.scheduler.prepare_human_interruption(checkpoint)
        LOGGER.info(
            "Automatic discovery checkpoint persisted: %s",
            checkpoint.checkpoint_id,
        )

    # ------------------------------------------------------------------
    # Heartbeats
    # ------------------------------------------------------------------

    def _heartbeat_if_due(self) -> None:
        now = time.monotonic()

        if (
            now - self._last_heartbeat
            >= self.config.heartbeat_seconds
        ):
            self._heartbeat()

    def _heartbeat(self, force: bool = False) -> None:
        now = time.monotonic()

        if (
            not force
            and now - self._last_heartbeat
            < self.config.heartbeat_seconds
        ):
            return

        self.scheduler.heartbeat(self.config.worker_id)
        self._last_heartbeat = now

    # ------------------------------------------------------------------
    # Signals / sleeping
    # ------------------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        signal.signal(signal.SIGTERM, self.request_shutdown)
        signal.signal(signal.SIGINT, self.request_shutdown)

    @staticmethod
    def _sleep(seconds: float) -> None:
        if seconds <= 0:
            return
        time.sleep(seconds)


# ---------------------------------------------------------------------------
# Environment / entry point
# ---------------------------------------------------------------------------


def create_engine_from_environment() -> KlaskerEngine:
    """Create an engine using the KLASKER_WORKER_ID environment variable."""

    import os

    worker_id = os.environ.get("KLASKER_WORKER_ID")

    if not worker_id:
        raise RuntimeError(
            "KLASKER_WORKER_ID must be set to SVELTRON or CORSAIR"
        )

    if worker_id not in KlaskerScheduler.WORKERS:
        raise RuntimeError(
            "KLASKER_WORKER_ID must be SVELTRON or CORSAIR"
        )

    database = KlaskerDatabase.from_environment()

    config = EngineConfig(
        worker_id=worker_id,
        idle_sleep_seconds=float(
            os.environ.get("KLASKER_IDLE_SLEEP_SECONDS", "5")
        ),
        heartbeat_seconds=int(
            os.environ.get("KLASKER_HEARTBEAT_SECONDS", "30")
        ),
        shutdown_grace_seconds=int(
            os.environ.get("KLASKER_SHUTDOWN_GRACE_SECONDS", "30")
        ),
    )

    return KlaskerEngine(
        database,
        config,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    engine = create_engine_from_environment()
    engine.run()
