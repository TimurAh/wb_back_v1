"""
Общие модели запросов для API дашборда
═══════════════════════════════════════════════════════════════
"""

from pydantic import BaseModel
from typing import Optional, List


class DateRange(BaseModel):
    """Диапазон дат для запроса"""
    start: str  # "2024-01-01"
    end: str    # "2024-01-31"


class DateRangeRequest(BaseModel):
    """Запрос с датами (для /metrics, /dynamics, /details)"""
    primary: DateRange
    compare: Optional[DateRange] = None
    user_id: Optional[int] = None

    # Фильтры
    brends: Optional[List[str]] = None
    category: Optional[List[str]] = None
    sa_name: Optional[List[str]] = None