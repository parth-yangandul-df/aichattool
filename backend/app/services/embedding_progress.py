"""In-memory progress tracker for background embedding generation."""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class EmbeddingProgress:
    connection_id: str
    total: int = 0
    completed: int = 0
    status: str = "pending"  # pending | running | completed | failed
    error: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


# Module-level state
_progress: dict[str, EmbeddingProgress] = {}
_tasks: dict[str, asyncio.Task] = {}


def start_tracking(connection_id: str, total: int) -> EmbeddingProgress:
    p = EmbeddingProgress(
        connection_id=connection_id,
        total=total,
        status="running",
        started_at=datetime.now(timezone.utc),
    )
    _progress[connection_id] = p
    return p


def increment(connection_id: str) -> None:
    if connection_id in _progress:
        _progress[connection_id].completed += 1


def mark_completed(connection_id: str) -> None:
    if connection_id in _progress:
        p = _progress[connection_id]
        p.status = "completed"
        p.finished_at = datetime.now(timezone.utc)


def mark_failed(connection_id: str, error: str) -> None:
    if connection_id in _progress:
        p = _progress[connection_id]
        p.status = "failed"
        p.error = error
        p.finished_at = datetime.now(timezone.utc)


def get_progress(connection_id: str) -> EmbeddingProgress | None:
    return _progress.get(connection_id)


def get_all_progress() -> dict[str, EmbeddingProgress]:
    return dict(_progress)


def register_task(connection_id: str, task: asyncio.Task) -> None:
    """Store task reference to prevent garbage collection."""
    _tasks[connection_id] = task


def is_running(connection_id: str) -> bool:
    return connection_id in _progress and _progress[connection_id].status == "running"
