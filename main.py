from fastapi import FastAPI, WebSocket, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import logging

from config import settings
from app.db.database import init_db, get_db, AsyncSessionLocal
from app.tasks.currency_task import CurrencyUpdateTask
from app.nats.publisher import nats_publisher
from app.api import currency, tasks
from app.websocket.currency_ws import websocket_endpoint
from sqlalchemy.ext.asyncio import AsyncSession

logging.basicConfig(
    level=settings.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

background_task_obj = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global background_task_obj

    logger.info("Запуск API мониторинга валют...")

    await init_db()
    logger.info("База данных инициализирована")

    await nats_publisher.connect()
    logger.info("Публикатор NATS подключен")

    db = AsyncSessionLocal()

    try:
        background_task_obj = CurrencyUpdateTask(db)
        task = asyncio.create_task(background_task_obj.run_periodically())
        logger.info("Фоновая задача обновления курсов запущена")

        yield

    finally:
        logger.info("Завершение работы...")

        if background_task_obj:
            background_task_obj.is_running = False

        await db.close()
        await nats_publisher.close()
        logger.info("Завершение работы выполнено")


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(currency.router, prefix=f"{settings.API_V1_PREFIX}/currency", tags=["currency"])
app.include_router(tasks.router, prefix=f"{settings.API_V1_PREFIX}/tasks", tags=["tasks"])


@app.websocket("/ws/currency")
async def websocket_currency(
        websocket: WebSocket,
        db: AsyncSession = Depends(get_db)
):
    await websocket_endpoint(websocket, db)


@app.get("/")
async def root():
    return {
        "message": "Currency Monitor API",
        "version": settings.VERSION,
        "docs": "/docs",
        "websocket": "/ws/currency"
    }


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "nats_connected": nats_publisher.is_connected,
        "background_task_running": background_task_obj.is_running if background_task_obj else False
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower()
    )
