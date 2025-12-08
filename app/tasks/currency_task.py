import asyncio
import httpx
import json
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.db.models import CurrencyRate
from app.services.currency_service import CurrencyService
from app.nats.publisher import nats_publisher
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

                logger.info(f"Fetched rates for base: {data.get('base_code')}")
                return {
                    "base_currency": data.get("base_code", "EUR"),
                    "rates": data.get("conversion_rates", {}),
                    "last_updated": datetime.utcnow().isoformat()
                }
        except Exception as e:
            logger.error(f"Error fetching rates: {e}")
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
            logger.info("Starting currency update task...")

            await CurrencyService.log_task(
                self.db,
                "currency_update",
                "started",
                "Fetching currency rates"
            )

            external_data = await self.fetch_external_rates()

            if external_data:
                await CurrencyService.log_task(
                    self.db,
                    "currency_update",
                    "success",
                    f"Fetched {len(external_data['rates'])} rates for {external_data['base_currency']}"
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

                logger.info(f"Task completed successfully")

        except Exception as e:
            logger.error(f"Error in task: {e}")
            await CurrencyService.log_task(
                self.db,
                "currency_update",
                "failed",
                f"Error: {str(e)}"
            )

    async def run_periodically(self):
        self.is_running = True
        while self.is_running:
            try:
                await self.run_task()
                await asyncio.sleep(self.task_interval)
            except asyncio.CancelledError:
                logger.info("Task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in periodic task: {e}")
                await asyncio.sleep(10)
