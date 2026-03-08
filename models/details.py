"""
Модели для блока «Детализация» (/api/dashboard/details)
═══════════════════════════════════════════════════════════════

Фронтенд ожидает:
{
    "details": [
        {
            "id": "1",
            "nmId": 100000001,
            "sa_name": "...",
            "net_profit": { "value": 250000, "change": 15000, "changePercent": 6 },
            ...все 24 метрики...
        },
    ]
}
"""

from pydantic import BaseModel
from typing import List, Optional


class MetricValue(BaseModel):
    """Значение одной метрики с изменением относительно периода сравнения"""
    value: float = 0
    change: float = 0
    changePercent: float = 0


class DetailRow(BaseModel):
    """Одна строка таблицы детализации (один товар)"""

    # ─── Информация о товаре ───
    id: str
    nmId: int
    sa_name: Optional[str] = None
    barcode: Optional[str] = None
    productImageUrl: Optional[str] = None
    productName: Optional[str] = None
    brand: Optional[str] = None
    category: Optional[str] = None

    # ─── Группа 1: Прибыльность ───
    net_profit: MetricValue = MetricValue()
    margin: MetricValue = MetricValue()
    roi: MetricValue = MetricValue()

    # ─── Группа 2: Заказы и продажи ───
    orders_rub: MetricValue = MetricValue()
    orders_qty: MetricValue = MetricValue()
    sales_rub: MetricValue = MetricValue()
    sales_qty: MetricValue = MetricValue()
    buyout_percent: MetricValue = MetricValue()
    returns_rub: MetricValue = MetricValue()
    returns_qty: MetricValue = MetricValue()
    cancels_qty: MetricValue = MetricValue()

    # ─── Группа 3: Конверсии ───
    cr_cart: MetricValue = MetricValue()
    cr_order: MetricValue = MetricValue()

    # ─── Группа 4: Реклама ───
    ad_expense: MetricValue = MetricValue()
    drr_orders: MetricValue = MetricValue()
    drr: MetricValue = MetricValue()

    # ─── Группа 5: Логистика и расходы ───
    logistics_rub: MetricValue = MetricValue()
    logistics_unit: MetricValue = MetricValue()
    penalties: MetricValue = MetricValue()
    tax: MetricValue = MetricValue()

    # ─── Группа 6: Операционные ───
    wb_commission: MetricValue = MetricValue()
    turnover: MetricValue = MetricValue()
    stock_qty: MetricValue = MetricValue()

    # ─── Группа 7: Себестоимость ───
    cost_price: MetricValue = MetricValue()


class DetailsApiResponse(BaseModel):
    """Ответ от POST /api/dashboard/details"""
    details: List[DetailRow] = []