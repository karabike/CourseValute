import asyncio
import json
import os
import logging
from nats.aio.client import Client as NATS
from datetime import datetime

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def message_handler(msg):
    try:
        data = json.loads(msg.data.decode())
        timestamp = datetime.now().strftime("%H:%M:%S")
        logger.info(f"[{timestamp}] NATS [{msg.subject}]: {json.dumps(data, indent=2)}")
    except Exception as e:
        logger.error(f"Ошибка обработки сообщения: {e}")


async def main():
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")

    logger.info(f"Запуск подписчика NATS. Подключение к: {nats_url}")

    max_retries = 10
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            nc = NATS()
            await nc.connect(
                servers=[nats_url],
                reconnect_time_wait=5,
                max_reconnect_attempts=-1
            )

            logger.info("Успешно подключено к серверу NATS")

            await nc.subscribe("currency.updates", cb=message_handler)
            await nc.subscribe("currency.*", cb=message_handler)

            logger.info("Подписано на каналы: currency.updates, currency.*")
            logger.info("Ожидание сообщений...")

            while True:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Попытка {attempt + 1}/{max_retries} не удалась: {e}")

            if attempt < max_retries - 1:
                logger.info(f"Повтор через {retry_delay} секунд...")
                await asyncio.sleep(retry_delay)
            else:
                logger.error("Достигнуто максимальное число попыток. Выход.")
                break


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Подписчик остановлен")
    except Exception as e:
        logger.error(f"Фатальная ошибка: {e}")
