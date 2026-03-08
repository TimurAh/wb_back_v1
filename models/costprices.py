"""
API-модели для блока «Себестоимость» (/api/dashboard/costprices)
"""

from pydantic import BaseModel
from typing import List, Optional


class CostPriceItem(BaseModel):
    """Один товар в API-ответе блока себестоимости"""
    id: str
    nmId: int
    sa_name: str
    productImageUrl: str = ''
    costPrice: Optional[float] = None     # null = не указана
    fulfillment: Optional[float] = None   # null = не указан


class CostPricesApiResponse(BaseModel):
    """Ответ от GET /api/dashboard/costprices"""
    items: List[CostPriceItem]


class CostPriceSaveRequest(BaseModel):
    """Запрос на сохранение себестоимости POST /api/dashboard/costprices/load"""
    nm_id: int
    costPrice: Optional[float] = None
    fulfillment: Optional[float] = None
    user_id: int


class CostPriceSaveResponse(BaseModel):
    """Ответ от POST /api/dashboard/costprices/load"""
    success: bool = True