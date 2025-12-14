from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from app.websocket.currency_ws import websocket_manager

from app.db.database import get_db
from app.schemas.currency import (
    CurrencyRateCreate,
    CurrencyRateUpdate,
    CurrencyRateInDB,
    TaskLogInDB
)
from app.services.currency_service import CurrencyService
from app.nats.publisher import nats_publisher
from app.db.models import TaskLog, CurrencyRate
from sqlalchemy import select

router = APIRouter()


@router.get("/rates", response_model=List[CurrencyRateInDB])
async def get_all_rates(db: AsyncSession = Depends(get_db)):
    rates = await CurrencyService.get_all_rates(db)
    return rates


@router.get("/rates/{rate_id}", response_model=CurrencyRateInDB)
async def get_rate(rate_id: int, db: AsyncSession = Depends(get_db)):
    rate = await CurrencyService.get_rate(db, rate_id)
    if not rate:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Currency rate not found"
        )
    return rate


@router.post("/rates", response_model=CurrencyRateInDB, status_code=status.HTTP_201_CREATED)
async def create_rate(rate_data: CurrencyRateCreate, db: AsyncSession = Depends(get_db)):
    from sqlalchemy import select

    result = await db.execute(
        select(CurrencyRate).where(
            (CurrencyRate.base_currency == rate_data.base_currency) &
            (CurrencyRate.target_currency == rate_data.target_currency)
        )
    )
    existing = result.first()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Currency rate already exists"
        )

    rate = await CurrencyService.create_rate(db, rate_data)

    await nats_publisher.publish_currency_update(
        "created",
        {
            "id": rate.id,
            "base_currency": rate.base_currency,
            "target_currency": rate.target_currency,
            "rate": rate.rate,
            "last_updated": rate.last_updated.isoformat() if rate.last_updated else None
        }
    )

    await websocket_manager.broadcast_currency_update({
        "id": rate.id,
        "base_currency": rate.base_currency,
        "target_currency": rate.target_currency,
        "rate": float(rate.rate),
        "last_updated": rate.last_updated.isoformat() if rate.last_updated else None,
        "action": "created"
    })

    rates = await CurrencyService.get_all_rates(db)
    rates_data = [
        {
            "id": r.id,
            "base_currency": r.base_currency,
            "target_currency": r.target_currency,
            "rate": float(r.rate),
            "last_updated": r.last_updated.isoformat() if r.last_updated else None
        }
        for r in rates
    ]
    await websocket_manager.broadcast_rates_list(rates_data)

    return rate


@router.patch("/rates/{rate_id}", response_model=CurrencyRateInDB)
async def update_rate(
        rate_id: int,
        rate_data: CurrencyRateUpdate,
        db: AsyncSession = Depends(get_db)
):
    rate = await CurrencyService.update_rate(db, rate_id, rate_data)
    if not rate:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Currency rate not found"
        )

    await nats_publisher.publish_currency_update(
        "updated",
        {
            "id": rate.id,
            "base_currency": rate.base_currency,
            "target_currency": rate.target_currency,
            "rate": rate.rate,
            "last_updated": rate.last_updated.isoformat() if rate.last_updated else None
        }
    )

    await websocket_manager.broadcast_currency_update({
        "id": rate.id,
        "base_currency": rate.base_currency,
        "target_currency": rate.target_currency,
        "rate": float(rate.rate),
        "last_updated": rate.last_updated.isoformat() if rate.last_updated else None,
        "action": "updated"
    })

    rates = await CurrencyService.get_all_rates(db)
    rates_data = [
        {
            "id": r.id,
            "base_currency": r.base_currency,
            "target_currency": r.target_currency,
            "rate": float(r.rate),
            "last_updated": r.last_updated.isoformat() if r.last_updated else None
        }
        for r in rates
    ]
    await websocket_manager.broadcast_rates_list(rates_data)

    return rate


@router.delete("/rates/{rate_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_rate(rate_id: int, db: AsyncSession = Depends(get_db)):
    rate = await CurrencyService.get_rate(db, rate_id)
    if not rate:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Currency rate not found"
        )

    success = await CurrencyService.delete_rate(db, rate_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete currency rate"
        )

    await nats_publisher.publish_currency_update(
        "deleted",
        {
            "id": rate.id,
            "base_currency": rate.base_currency,
            "target_currency": rate.target_currency
        }
    )

    await websocket_manager.broadcast_currency_update({
        "id": rate.id,
        "base_currency": rate.base_currency,
        "target_currency": rate.target_currency,
        "action": "deleted"
    })

    rates = await CurrencyService.get_all_rates(db)
    rates_data = [
        {
            "id": r.id,
            "base_currency": r.base_currency,
            "target_currency": r.target_currency,
            "rate": float(r.rate),
            "last_updated": r.last_updated.isoformat() if r.last_updated else None
        }
        for r in rates
    ]
    await websocket_manager.broadcast_rates_list(rates_data)


@router.get("/task-logs", response_model=List[TaskLogInDB])
async def get_task_logs(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(TaskLog).order_by(TaskLog.created_at.desc())
    )
    logs = result.scalars().all()
    return logs
