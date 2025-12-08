import json
import asyncio
from fastapi import WebSocket, WebSocketDisconnect
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.currency_service import CurrencyService
import logging

logger = logging.getLogger(__name__)


class WebSocketManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"New WebSocket connection. Total connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        message_json = json.dumps(message)
        disconnected = []

        for connection in self.active_connections:
            try:
                await connection.send_text(message_json)
            except Exception as e:
                logger.error(f"Error sending to WebSocket: {e}")
                disconnected.append(connection)

        for connection in disconnected:
            self.disconnect(connection)


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
                    "rate": rate.rate,
                    "last_updated": rate.last_updated.isoformat() if rate.last_updated else None
                }
                for rate in rates
            ]
        }
        await websocket.send_text(json.dumps(initial_data))

        while True:
            data = await websocket.receive_text()
            logger.info(f"Received WebSocket message: {data}")

    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        websocket_manager.disconnect(websocket)
