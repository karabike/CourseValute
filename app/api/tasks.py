from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import get_db
from app.tasks.currency_task import CurrencyUpdateTask
from app.services.currency_service import CurrencyService

router = APIRouter()


@router.post("/run")
async def run_currency_task(
        background_tasks: BackgroundTasks,
        db: AsyncSession = Depends(get_db)
):
    task = CurrencyUpdateTask(db)

    if task.is_running:
        raise HTTPException(
            status_code=400,
            detail="Task is already running"
        )

    background_tasks.add_task(task.run_task)

    await CurrencyService.log_task(
        db,
        "currency_update",
        "manual_start",
        "Currency update task manually triggered"
    )

    return {"message": "Currency update task started manually"}


@router.get("/status")
async def get_task_status():
    return {"message": "Task status endpoint"}
