import asyncio
import httpx
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.db.models import CurrencyRate
from app.services.currency_service import CurrencyService
from app.nats.publisher import nats_publisher
from app.websocket.currency_ws import websocket_manager
from config import settings
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class CurrencyUpdateTask:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.is_running = False
        self.task_interval = settings.TASK_INTERVAL_SECONDS

    async def fetch_external_rates(self):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(settings.CURRENCY_API_URL)
                response.raise_for_status()
                data = response.json()

                logger.info(f"Получены курсы для базовой валюты: {data.get('base_code')}")
                return {
                    "base_currency": data.get("base_code", "EUR"),
                    "rates": data.get("conversion_rates", {}),
                    "last_updated": datetime.utcnow().isoformat()
                }
        except Exception as e:
            logger.error(f"Ошибка при получении курсов: {e}")
            return {
                "base_currency": "EUR",
                "rates": {
                    "USD": 1.08,
                    "GBP": 0.86,
                    "JPY": 161.5,
                    "RUB": 100.5
                },
                "last_updated": datetime.utcnow().isoformat()
            }

    async def run_task(self):
        try:
            logger.info("Запуск задачи обновления курсов...")

            await CurrencyService.log_task(
                self.db,
                "currency_update",
                "started",
                "Fetching currency rates"
            )

            external_data = await self.fetch_external_rates()

            if external_data:
                updated_rates = await self.save_rates_to_db(external_data)

                if updated_rates:
                    await websocket_manager.broadcast_rates_list(updated_rates)

                    for rate_data in updated_rates:
                        await websocket_manager.broadcast_currency_update({
                            "base_currency": rate_data["base_currency"],
                            "target_currency": rate_data["target_currency"],
                            "rate": rate_data["rate"],
                            "last_updated": rate_data["last_updated"]
                        })

                await CurrencyService.log_task(
                    self.db,
                    "currency_update",
                    "success",
                    f"Получено {len(external_data['rates'])} курсов для {external_data['base_currency']}"
                )

                for currency, rate in list(external_data["rates"].items())[:3]:
                    await nats_publisher.publish_currency_update(
                        "updated",
                        {
                            "base_currency": external_data["base_currency"],
                            "target_currency": currency,
                            "rate": rate,
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )

                    logger.info(
                        f"Задача успешно завершена. Отправлено обновлений {len(websocket_manager.active_connections)} клиентам WebSocket.")

        except Exception as e:
            logger.error(f"Ошибка в задаче: {e}")
            await CurrencyService.log_task(
                self.db,
                "currency_update",
                "failed",
                f"Ошибка: {str(e)}"
            )

    async def save_rates_to_db(self, external_data: dict) -> list:
        updated_rates = []
        try:
            for currency, rate in external_data["rates"].items():
                stmt = select(CurrencyRate).where(
                    CurrencyRate.base_currency == external_data["base_currency"],
                    CurrencyRate.target_currency == currency
                )
                result = await self.db.execute(stmt)
                existing_rate = result.scalar_one_or_none()

                now = datetime.utcnow()

                if existing_rate:
                    if existing_rate.rate != rate:
                        existing_rate.rate = rate
                        existing_rate.last_updated = now
                        updated_rates.append({
                            "id": existing_rate.id,
                            "base_currency": existing_rate.base_currency,
                            "target_currency": existing_rate.target_currency,
                            "rate": float(rate),
                            "last_updated": now.isoformat()
                        })
                else:
                    new_rate = CurrencyRate(
                        base_currency=external_data["base_currency"],
                        target_currency=currency,
                        rate=rate,
                        last_updated=now
                    )
                    self.db.add(new_rate)
                    updated_rates.append({
                        "base_currency": external_data["base_currency"],
                        "target_currency": currency,
                        "rate": float(rate),
                        "last_updated": now.isoformat()
                    })

            await self.db.commit()
            logger.info(f"Курсы валют сохранены/обновлены в БД. Обновлено {len(updated_rates)} записей.")

            return updated_rates

        except SQLAlchemyError as e:
            await self.db.rollback()
            logger.error(f"Ошибка БД при сохранении курсов: {e}")
            return []

    async def run_periodically(self):
        self.is_running = True
        while self.is_running:
            try:
                await self.run_task()
                await asyncio.sleep(self.task_interval)
            except asyncio.CancelledError:
                logger.info("Задача отменена")
                break
            except Exception as e:
                logger.error(f"Ошибка в периодической задаче: {e}")
                await asyncio.sleep(10)
