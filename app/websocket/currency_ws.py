import json
import asyncio
from fastapi import WebSocket, WebSocketDisconnect
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.currency_service import CurrencyService
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class WebSocketManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"Новое WebSocket-подключение. Всего подключений: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WebSocket отключён. Всего подключений: {len(self.active_connections)}")

    async def send_personal_message(self, message: dict, websocket: WebSocket):
        try:
            message_json = json.dumps(message)
            await websocket.send_text(message_json)
        except Exception as e:
            logger.error(f"Ошибка при отправке личного сообщения: {e}")
            self.disconnect(websocket)

    async def broadcast(self, message: dict):
        message_json = json.dumps(message)
        disconnected = []

        for connection in self.active_connections:
            try:
                await connection.send_text(message_json)
            except Exception as e:
                logger.error(f"Ошибка при рассылке по WebSocket: {e}")
                disconnected.append(connection)

        for connection in disconnected:
            self.disconnect(connection)

    async def broadcast_currency_update(self, update_data: dict):
        logger.info(f"Отправка обновления курса в WebSocket: {update_data}")
        message = {
            "type": "currency_update",
            "data": update_data,
            "timestamp": datetime.utcnow().isoformat()
        }
        await self.broadcast(message)

    async def broadcast_rates_list(self, rates: list):
        message = {
            "type": "rates_list",
            "data": rates,
            "timestamp": datetime.utcnow().isoformat()
        }
        await self.broadcast(message)


websocket_manager = WebSocketManager()


async def websocket_endpoint(websocket: WebSocket, db: AsyncSession):
    await websocket_manager.connect(websocket)

    try:
        rates = await CurrencyService.get_all_rates(db)
        initial_data = {
            "type": "initial",
            "data": [
                {
                    "id": rate.id,
                    "base_currency": rate.base_currency,
                    "target_currency": rate.target_currency,
                    "rate": float(rate.rate),
                    "last_updated": rate.last_updated.isoformat() if rate.last_updated else None
                }
                for rate in rates
            ],
            "timestamp": datetime.utcnow().isoformat()
        }
        await websocket_manager.send_personal_message(initial_data, websocket)

        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                logger.info(f"Получено сообщение WebSocket: {message}")

                if message.get("type") == "ping":
                    await websocket_manager.send_personal_message({
                        "type": "pong",
                        "timestamp": datetime.utcnow().isoformat()
                    }, websocket)
                elif message.get("type") == "get_rates":
                    rates = await CurrencyService.get_all_rates(db)
                    rates_data = {
                        "type": "rates_list",
                        "data": [
                            {
                                "id": rate.id,
                                "base_currency": rate.base_currency,
                                "target_currency": rate.target_currency,
                                "rate": float(rate.rate),
                                "last_updated": rate.last_updated.isoformat() if rate.last_updated else None
                            }
                            for rate in rates
                        ],
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    await websocket_manager.send_personal_message(rates_data, websocket)

            except json.JSONDecodeError:
                logger.error(f"Получен некорректный JSON: {data}")
                await websocket_manager.send_personal_message({
                    "type": "error",
                    "message": "Invalid JSON format"
                }, websocket)

    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket)
        logger.info("WebSocket отключён")
    except Exception as e:
        logger.error(f"Ошибка WebSocket: {e}")
        websocket_manager.disconnect(websocket)