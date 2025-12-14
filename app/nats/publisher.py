import asyncio
import json
from nats.aio.client import Client as NATS
from config import settings
import logging

logger = logging.getLogger(__name__)


class NATSPublisher:
    def __init__(self):
        self.nc = NATS()
        self.is_connected = False

    async def connect(self):
        try:
            await self.nc.connect(servers=[settings.NATS_URL])
            self.is_connected = True
            logger.info(f"Подключено к NATS по адресу {settings.NATS_URL}")
        except Exception as e:
            logger.error(f"Не удалось подключиться к NATS: {e}")
            self.is_connected = False

    async def publish_currency_update(self, action: str, currency_data: dict):
        if not self.is_connected:
            await self.connect()

        try:
            message = {
                "action": action,
                "data": currency_data,
                "timestamp": asyncio.get_event_loop().time()
            }

            await self.nc.publish(
                settings.NATS_CHANNEL,
                json.dumps(message).encode()
            )
            logger.info(f"Опубликовано в NATS: {action} - {currency_data.get('target_currency')}")
        except Exception as e:
            logger.error(f"Не удалось опубликовать в NATS: {e}")

    async def close(self):
        await self.nc.close()


nats_publisher = NATSPublisher()
