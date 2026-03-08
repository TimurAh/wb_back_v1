"""
Общие модели запросов для API дашборда
═══════════════════════════════════════════════════════════════
"""

from pydantic import BaseModel
from typing import Optional


class DateRange(BaseModel):
    """Диапазон дат для запроса"""
    start: str  # "2024-01-01"
    end: str    # "2024-01-31"


class DateRangeRequest(BaseModel):
    """Запрос с датами (для /metrics и /dynamics)"""
    primary: DateRange
    compare: Optional[DateRange] = None
    user_id: Optional[int] = None