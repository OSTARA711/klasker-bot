# Path: ~/klasker-bot/klaskerbot.py
"""KlaskerBot continuous Web-discovery control model."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum, IntEnum
from typing import Optional
from urllib.parse import urlparse
from uuid import uuid4


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def normalise_url(url: str) -> str:
    value = url.strip()
    parsed = urlparse(value)

    if parsed.scheme not in {"http", "https"}:
        raise ValueError("URL must use http or https")

    if not parsed.netloc:
        raise ValueError("URL must contain a hostname")

    return value


# ---------------------------------------------------------------------------
# Bot state
# ---------------------------------------------------------------------------


class BotState(str, Enum):
    STOPPED = "STOPPED"
    RUNNING = "RUNNING"
    PAUSING = "PAUSING"
    PAUSED = "PAUSED"
    HUMAN_JOB = "HUMAN_JOB"


class JobPriority(IntEnum):
    """Lower number means higher priority."""

    HUMAN = 0
    ACTIVE_INVESTIGATION = 1
    PROMISING_AGENT = 2
    BUSINESS = 3
    FRONTIER_EXPANSION = 4
    REVERIFICATION = 5


class JobType(str, Enum):
    HUMAN_SEARCH = "HUMAN_SEARCH"
    WEBSITE_ANALYSIS = "WEBSITE_ANALYSIS"
    DISCOVERY = "DISCOVERY"
    REVERIFICATION = "REVERIFICATION"


class JobStatus(str, Enum):
    WAITING = "WAITING"
    CLAIMED = "CLAIMED"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class WorkerStatus(str, Enum):
    OFFLINE = "OFFLINE"
    IDLE = "IDLE"
    BUSY = "BUSY"


class DiscoveryRelationship(str, Enum):
    DIRECT_LINK = "DIRECT_LINK"
    BACKLINK = "BACKLINK"
    SITEMAP = "SITEMAP"
    SEARCH_RESULT = "SEARCH_RESULT"
    AGENT_REFERENCE = "AGENT_REFERENCE"
    PRODUCT_REFERENCE = "PRODUCT_REFERENCE"


# ---------------------------------------------------------------------------
# Persistent-state contracts
# ---------------------------------------------------------------------------


@dataclass
class Checkpoint:
    """Safe restart point; only completed operations become checkpoints."""

    checkpoint_id: str = field(
        default_factory=lambda: new_id("checkpoint")
    )
    current_frontier_id: Optional[str] = None
    frontier_position: int = 0
    discovery_depth: int = 0
    last_completed_operation: Optional[str] = None
    updated_at: str = field(default_factory=utc_now)


@dataclass
class FrontierItem:
    """A URL waiting to be discovered or revisited."""

    frontier_id: str = field(
        default_factory=lambda: new_id("frontier")
    )
    url: str = ""
    domain: str = ""
    source_url: Optional[str] = None
    relationship: DiscoveryRelationship = (
        DiscoveryRelationship.DIRECT_LINK
    )
    priority: JobPriority = JobPriority.FRONTIER_EXPANSION
    status: JobStatus = JobStatus.WAITING
    depth: int = 0
    discovered_at: str = field(default_factory=utc_now)
    last_attempted: Optional[str] = None
    attempt_count: int = 0

    def __post_init__(self) -> None:
        self.url = normalise_url(self.url)

        if not self.domain:
            self.domain = urlparse(self.url).hostname or ""


@dataclass
class Job:
    """A unit of work claimable by either Serv00 engine."""

    job_id: str = field(default_factory=lambda: new_id("job"))
    job_type: JobType = JobType.DISCOVERY
    priority: JobPriority = JobPriority.FRONTIER_EXPANSION
    status: JobStatus = JobStatus.WAITING
    target_url: Optional[str] = None
    query: Optional[str] = None
    created_at: str = field(default_factory=utc_now)
    claimed_by: Optional[str] = None
    claim_expires: Optional[str] = None
    heartbeat_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    checkpoint_id: Optional[str] = None
    error: Optional[str] = None


@dataclass
class Worker:
    """Execution identity for a Serv00 engine."""

    worker_id: str
    status: WorkerStatus = WorkerStatus.OFFLINE
    current_job_id: Optional[str] = None
    last_heartbeat: Optional[str] = None


@dataclass
class HumanRequest:
    """A human request retained as reusable discovery knowledge."""

    request_id: str = field(default_factory=lambda: new_id("human"))
    query: str = ""
    created_at: str = field(default_factory=utc_now)
    job_id: Optional[str] = None
    status: JobStatus = JobStatus.WAITING
    result_summary: Optional[str] = None
    websites_discovered: list[str] = field(default_factory=list)
    agent_capabilities_discovered: list[str] = field(
        default_factory=list
    )


@dataclass
class BotRuntime:
    """
    In-memory Bot state.

    database.py will later persist equivalent records so either
    SVELTRON or CORSAIR can recover after failure.
    """

    state: BotState = BotState.STOPPED

    checkpoint: Checkpoint = field(
        default_factory=Checkpoint
    )

    frontier: list[FrontierItem] = field(
        default_factory=list
    )

    jobs: list[Job] = field(
        default_factory=list
    )

    human_requests: list[HumanRequest] = field(
        default_factory=list
    )

    workers: dict[str, Worker] = field(
        default_factory=lambda: {
            "SVELTRON": Worker("SVELTRON"),
            "CORSAIR": Worker("CORSAIR"),
        }
    )


# ---------------------------------------------------------------------------
# KlaskerBot control model
# ---------------------------------------------------------------------------


class KlaskerBot:
    """
    Control-plane state machine.

    No network or database operations are performed here.
    This class establishes the state transitions and data contracts
    that the persistent database, discovery engine and scheduler will use.
    """

    def __init__(
        self,
        runtime: Optional[BotRuntime] = None,
    ) -> None:
        self.runtime = runtime or BotRuntime()

    @property
    def state(self) -> BotState:
        return self.runtime.state

    @property
    def checkpoint(self) -> Checkpoint:
        return self.runtime.checkpoint

    # ------------------------------------------------------------------
    # Bot lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start or resume automatic discovery."""

        if self.state == BotState.HUMAN_JOB:
            raise RuntimeError(
                "Cannot start discovery during a human job"
            )

        self.runtime.state = BotState.RUNNING

        self._touch_checkpoint(
            "automatic discovery started/resumed"
        )

    def request_pause(self) -> None:
        """
        Request a safe pause.

        The discovery engine should finish its current atomic operation
        before confirm_pause() is called.
        """

        if self.state == BotState.RUNNING:
            self.runtime.state = BotState.PAUSING

    def confirm_pause(self) -> None:
        """
        Confirm that the current atomic operation has been committed.
        """

        if self.state not in {
            BotState.PAUSING,
            BotState.RUNNING,
        }:
            raise RuntimeError(
                f"Cannot confirm pause from {self.state.value}"
            )

        self.runtime.state = BotState.PAUSED

        self._touch_checkpoint(
            "automatic discovery paused safely"
        )

    def stop(self) -> None:
        """
        Stop automatic execution without deleting the checkpoint.
        """

        if self.state == BotState.HUMAN_JOB:
            raise RuntimeError(
                "Cannot stop during a human job"
            )

        self.runtime.state = BotState.STOPPED

        self._touch_checkpoint("Bot stopped")

    # ------------------------------------------------------------------
    # Human jobs
    # ------------------------------------------------------------------

    def begin_human_job(self, job: Job) -> None:
        """
        Safely interrupt automatic discovery and activate a P0 job.
        """

        if job.priority != JobPriority.HUMAN:
            raise ValueError(
                "Human jobs must have P0 HUMAN priority"
            )

        if job.job_type not in {
            JobType.HUMAN_SEARCH,
            JobType.WEBSITE_ANALYSIS,
        }:
            raise ValueError(
                "Invalid job type for a human request"
            )

        if self.state == BotState.RUNNING:
            self.request_pause()
            self.confirm_pause()

        elif self.state != BotState.PAUSED:
            raise RuntimeError(
                "Human job requires PAUSED or RUNNING state, "
                f"got {self.state.value}"
            )

        if job not in self.runtime.jobs:
            self.runtime.jobs.append(job)

        job.status = JobStatus.RUNNING
        job.started_at = utc_now()

        self.runtime.state = BotState.HUMAN_JOB

    def finish_human_job(
        self,
        job_id: str,
        result_summary: Optional[str] = None,
    ) -> None:
        """
        Commit human results and resume automatic discovery
        from the existing checkpoint.
        """

        job = self._get_job(job_id)

        if self.state != BotState.HUMAN_JOB:
            raise RuntimeError(
                "No human job is currently active"
            )

        if job.status != JobStatus.RUNNING:
            raise RuntimeError(
                "Human job is not running"
            )

        job.status = JobStatus.COMPLETED
        job.completed_at = utc_now()

        request = self._human_request_for_job(job_id)

        if request:
            request.status = JobStatus.COMPLETED
            request.result_summary = result_summary

        self.runtime.state = BotState.RUNNING

        self._touch_checkpoint(
            "human job completed; automatic discovery resumed"
        )

    def fail_job(
        self,
        job_id: str,
        error: str,
    ) -> None:
        """
        Mark a job failed without destroying the discovery checkpoint.
        """

        job = self._get_job(job_id)

        job.status = JobStatus.FAILED
        job.error = error
        job.completed_at = utc_now()

        if (
            job.priority == JobPriority.HUMAN
            and self.state == BotState.HUMAN_JOB
        ):
            self.runtime.state = BotState.RUNNING

            self._touch_checkpoint(
                "human job failed; automatic discovery resumed"
            )

    # ------------------------------------------------------------------
    # Discovery frontier
    # ------------------------------------------------------------------

    def add_frontier_item(
        self,
        url: str,
        *,
        source_url: Optional[str] = None,
        relationship: DiscoveryRelationship = (
            DiscoveryRelationship.DIRECT_LINK
        ),
        priority: JobPriority = (
            JobPriority.FRONTIER_EXPANSION
        ),
        depth: int = 0,
    ) -> FrontierItem:
        """
        Add a URL to the frontier.

        Existing non-failed URLs are reused rather than duplicated.
        """

        normalised = normalise_url(url)

        for item in self.runtime.frontier:
            if (
                item.url == normalised
                and item.status != JobStatus.FAILED
            ):
                return item

        item = FrontierItem(
            url=normalised,
            source_url=source_url,
            relationship=relationship,
            priority=priority,
            depth=depth,
        )

        self.runtime.frontier.append(item)

        return item

    def next_frontier_item(
        self,
    ) -> Optional[FrontierItem]:
        """
        Return the highest-priority waiting frontier item.
        """

        waiting = [
            item
            for item in self.runtime.frontier
            if item.status == JobStatus.WAITING
        ]

        if not waiting:
            return None

        return min(
            waiting,
            key=lambda item: (
                item.priority,
                item.depth,
                item.discovered_at,
            ),
        )

    def begin_frontier_operation(
        self,
        item: FrontierItem,
    ) -> None:
        """
        Mark a frontier item as the current atomic operation.
        """

        if self.state != BotState.RUNNING:
            raise RuntimeError(
                "Automatic discovery is not running"
            )

        if item.status != JobStatus.WAITING:
            raise RuntimeError(
                "Frontier item is not waiting"
            )

        item.status = JobStatus.RUNNING
        item.last_attempted = utc_now()
        item.attempt_count += 1

        self.runtime.checkpoint.current_frontier_id = (
            item.frontier_id
        )

        self.runtime.checkpoint.frontier_position = (
            self.runtime.frontier.index(item)
        )

        self.runtime.checkpoint.discovery_depth = (
            item.depth
        )

        self._touch_checkpoint(
            f"started frontier operation {item.frontier_id}"
        )

    def complete_frontier_operation(
        self,
        item: FrontierItem,
        *,
        operation: str,
    ) -> None:
        """
        Commit a completed frontier operation.
        """

        if item.status != JobStatus.RUNNING:
            raise RuntimeError(
                "Frontier item is not running"
            )

        item.status = JobStatus.COMPLETED

        self._touch_checkpoint(operation)

    def fail_frontier_operation(
        self,
        item: FrontierItem,
        error: str,
    ) -> None:
        """
        Record a failed operation while retaining its history.
        """

        if item.status != JobStatus.RUNNING:
            raise RuntimeError(
                "Frontier item is not running"
            )

        item.status = JobStatus.FAILED

        self._touch_checkpoint(
            f"frontier operation failed: {error}"
        )

    # ------------------------------------------------------------------
    # Jobs and workers
    # ------------------------------------------------------------------

    def queue_job(
        self,
        job_type: JobType,
        priority: JobPriority,
        *,
        target_url: Optional[str] = None,
        query: Optional[str] = None,
    ) -> Job:
        """
        Create a job using the central priority model.
        """

        if target_url is not None:
            target_url = normalise_url(target_url)

        job = Job(
            job_type=job_type,
            priority=priority,
            target_url=target_url,
            query=query,
            checkpoint_id=self.checkpoint.checkpoint_id,
        )

        self.runtime.jobs.append(job)

        return job

    def claim_next_job(
        self,
        worker_id: str,
    ) -> Optional[Job]:
        """
        Claim the highest-priority waiting job.

        The future database implementation must make this
        operation transactional.
        """

        worker = self._get_worker(worker_id)

        if worker.status == WorkerStatus.BUSY:
            raise RuntimeError(
                f"Worker {worker_id} already has a job"
            )

        waiting = [
            job
            for job in self.runtime.jobs
            if job.status == JobStatus.WAITING
        ]

        if not waiting:
            return None

        job = min(
            waiting,
            key=lambda candidate: (
                candidate.priority,
                candidate.created_at,
            ),
        )

        job.status = JobStatus.CLAIMED
        job.claimed_by = worker_id
        job.heartbeat_at = utc_now()

        worker.status = WorkerStatus.BUSY
        worker.current_job_id = job.job_id
        worker.last_heartbeat = job.heartbeat_at

        return job

    def heartbeat(
        self,
        worker_id: str,
        job_id: str,
    ) -> None:
        """Refresh worker/job liveness."""

        worker = self._get_worker(worker_id)
        job = self._get_job(job_id)

        if (
            worker.current_job_id != job_id
            or job.claimed_by != worker_id
        ):
            raise RuntimeError(
                "Worker does not own this job"
            )

        now = utc_now()

        worker.last_heartbeat = now
        job.heartbeat_at = now

    def release_worker(
        self,
        worker_id: str,
    ) -> None:
        """Return a worker to IDLE."""

        worker = self._get_worker(worker_id)

        worker.status = WorkerStatus.IDLE
        worker.current_job_id = None
        worker.last_heartbeat = utc_now()

    # ------------------------------------------------------------------
    # Human request records
    # ------------------------------------------------------------------

    def register_human_request(
        self,
        query: str,
        job: Job,
    ) -> HumanRequest:
        """
        Associate a human search with its P0 job.
        """

        if job.priority != JobPriority.HUMAN:
            raise ValueError(
                "Human request must reference a P0 job"
            )

        request = HumanRequest(
            query=query.strip(),
            job_id=job.job_id,
        )

        self.runtime.human_requests.append(request)

        return request

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def snapshot(self) -> dict:
        """
        Return a JSON-compatible snapshot.

        database.py will later persist these records properly.
        """

        return {
            "state": self.runtime.state.value,

            "checkpoint": asdict(
                self.runtime.checkpoint
            ),

            "frontier": [
                {
                    **asdict(item),
                    "relationship": (
                        item.relationship.value
                    ),
                    "priority": int(item.priority),
                    "status": item.status.value,
                }
                for item in self.runtime.frontier
            ],

            "jobs": [
                {
                    **asdict(job),
                    "job_type": job.job_type.value,
                    "priority": int(job.priority),
                    "status": job.status.value,
                }
                for job in self.runtime.jobs
            ],

            "human_requests": [
                asdict(request)
                for request in self.runtime.human_requests
            ],

            "workers": {
                worker_id: asdict(worker)
                for worker_id, worker in (
                    self.runtime.workers.items()
                )
            },
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _touch_checkpoint(
        self,
        operation: str,
    ) -> None:
        self.runtime.checkpoint.last_completed_operation = (
            operation
        )

        self.runtime.checkpoint.updated_at = utc_now()

    def _get_job(
        self,
        job_id: str,
    ) -> Job:
        for job in self.runtime.jobs:
            if job.job_id == job_id:
                return job

        raise KeyError(
            f"Unknown job: {job_id}"
        )

    def _get_worker(
        self,
        worker_id: str,
    ) -> Worker:
        try:
            return self.runtime.workers[worker_id]

        except KeyError as exc:
            raise KeyError(
                f"Unknown worker: {worker_id}"
            ) from exc

    def _human_request_for_job(
        self,
        job_id: str,
    ) -> Optional[HumanRequest]:
        return next(
            (
                request
                for request in self.runtime.human_requests
                if request.job_id == job_id
            ),
            None,
        )


# ---------------------------------------------------------------------------
# Minimal local self-test
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    bot = KlaskerBot()

    bot.start()

    seed = bot.add_frontier_item(
        "https://example.com"
    )

    bot.begin_frontier_operation(seed)

    bot.complete_frontier_operation(
        seed,
        operation="seed URL analysed",
    )

    human_job = bot.queue_job(
        JobType.WEBSITE_ANALYSIS,
        JobPriority.HUMAN,
        target_url="https://example.org",
    )

    request = bot.register_human_request(
        "example.org",
        human_job,
    )

    bot.begin_human_job(human_job)

    bot.finish_human_job(
        human_job.job_id,
        "Human analysis completed",
    )

    print(
        "KlaskerBot state:",
        bot.state.value,
    )

    print(
        "Checkpoint:",
        bot.checkpoint.last_completed_operation,
    )

    print(
        "Human request:",
        request.request_id,
    )

    print(
        "Snapshot keys:",
        ", ".join(bot.snapshot().keys()),
    )
