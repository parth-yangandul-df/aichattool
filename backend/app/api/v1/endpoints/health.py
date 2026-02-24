from fastapi import APIRouter

from app.services.embedding_progress import get_all_progress

router = APIRouter(tags=["health"])


@router.get("/health")
async def health_check():
    return {"status": "ok"}


@router.get("/embeddings/status")
async def embedding_status():
    """Return embedding generation progress for all connections."""
    progress = get_all_progress()
    return {
        "tasks": [
            {
                "connection_id": p.connection_id,
                "status": p.status,
                "total": p.total,
                "completed": p.completed,
                "error": p.error,
            }
            for p in progress.values()
        ]
    }
