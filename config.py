import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    PROJECT_NAME: str = os.getenv("PROJECT_NAME", "Currency Monitor API")
    VERSION: str = os.getenv("VERSION", "1.0.0")
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))
    API_V1_PREFIX: str = os.getenv("API_V1_PREFIX", "/api/v1")
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./currency.db")
    DATABASE_ECHO: bool = os.getenv("DATABASE_ECHO", "False").lower() == "true"
    NATS_URL: str = os.getenv("NATS_URL", "nats://localhost:4222")
    NATS_CHANNEL: str = os.getenv("NATS_CHANNEL", "currency.updates")
    CURRENCY_API_URL: str = os.getenv("CURRENCY_API_URL", "https://v6.exchangerate-api.com/v6/420ad69df25c5df6f82be95e/latest/USD")
    TASK_INTERVAL_SECONDS: int = int(os.getenv("TASK_INTERVAL_SECONDS", "60"))
    TASK_MAX_RETRIES: int = int(os.getenv("TASK_MAX_RETRIES", "3"))
    TASK_RETRY_DELAY: int = int(os.getenv("TASK_RETRY_DELAY", "5"))
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: str = os.getenv("LOG_FILE", "logs/app.log")
    SECRET_KEY: str = os.getenv("SECRET_KEY", "dev-secret-key-change-in-production")
    DEBUG: bool = os.getenv("DEBUG", "True").lower() == "true"


settings = Settings()
