from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class CurrencyRateBase(BaseModel):
    base_currency: str = Field(default="USD")
    target_currency: str
    rate: float


class CurrencyRateCreate(CurrencyRateBase):
    pass


class CurrencyRateUpdate(BaseModel):
    rate: Optional[float] = None


class CurrencyRateInDB(CurrencyRateBase):
    id: int
    last_updated: datetime

    class Config:
        from_attributes = True


class TaskLogBase(BaseModel):
    task_name: str
    status: str
    details: str


class TaskLogInDB(TaskLogBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True
