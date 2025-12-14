from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.exc import SQLAlchemyError
from app.db.models import CurrencyRate, TaskLog
from app.schemas.currency import CurrencyRateCreate, CurrencyRateUpdate
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


class CurrencyService:
    @staticmethod
    async def get_all_rates(db: AsyncSession) -> List[CurrencyRate]:
        result = await db.execute(select(CurrencyRate))
        return result.scalars().all()

    @staticmethod
    async def get_rate(db: AsyncSession, rate_id: int) -> Optional[CurrencyRate]:
        result = await db.execute(
            select(CurrencyRate).where(CurrencyRate.id == rate_id)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def create_rate(db: AsyncSession, rate_data: CurrencyRateCreate) -> CurrencyRate:
        db_rate = CurrencyRate(**rate_data.dict())
        db.add(db_rate)
        await db.commit()
        await db.refresh(db_rate)
        return db_rate

    @staticmethod
    async def update_rate(db: AsyncSession, rate_id: int, rate_data: CurrencyRateUpdate) -> Optional[CurrencyRate]:
        try:
            await db.execute(
                update(CurrencyRate)
                .where(CurrencyRate.id == rate_id)
                .values(**rate_data.dict(exclude_unset=True))
            )
            await db.commit()

            result = await db.execute(
                select(CurrencyRate).where(CurrencyRate.id == rate_id)
            )
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"Ошибка при обновлении курса: {e}")
            await db.rollback()
            return None

    @staticmethod
    async def delete_rate(db: AsyncSession, rate_id: int) -> bool:
        try:
            await db.execute(
                delete(CurrencyRate).where(CurrencyRate.id == rate_id)
            )
            await db.commit()
            return True
        except SQLAlchemyError as e:
            logger.error(f"Ошибка при удалении курса: {e}")
            await db.rollback()
            return False

    @staticmethod
    async def log_task(db: AsyncSession, task_name: str, status: str, details: str):
        task_log = TaskLog(
            task_name=task_name,
            status=status,
            details=details
        )
        db.add(task_log)
        await db.commit()
