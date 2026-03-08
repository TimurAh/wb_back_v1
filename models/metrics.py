from pydantic import BaseModel
from typing import List, Dict, Any, Optional, Literal, Set


class MetricCard(BaseModel):
    """Карточка одного показателя"""
    id: str
    metricKey: str
    name: str
    unit: Literal['currency', 'percent', 'count']
    trendStatus: Literal['up', 'down', 'flat'] = 'flat'
    trendPercent: float = 0.0
    currentPercent: float = 0.0
    previousPercent: float = 0.0
    currentValue: float = 0.0
    previousValue: float = 0.0


class MetricsCollection:
    """Коллекция всех 24 показателей"""

    # Метрики, для которых снижение = хорошо (инвертируем тренд)
    INVERTED_METRICS: Set[str] = {
        'returns_rub', 'returns_qty', 'cancels_qty',
        'ad_expense', 'drr_orders', 'drr',
        'logistics_rub', 'logistics_unit', 'penalties', 'tax',
        'wb_commission', 'cost_price'
    }

    # Определения всех 24 метрик: (key, name, unit)
    DEFINITIONS = [
        # Группа 1: Прибыльность
        ('net_profit', 'Чистая прибыль', 'currency'),           #+
        ('margin', 'Маржинальность', 'percent'),                #+
        ('roi', 'ROI', 'percent'),                              #+

        # Группа 2: Заказы и продажи
        ('orders_rub', 'Заказы', 'currency'),                   #+
        ('orders_qty', 'Заказы', 'count'),                      #+
        ('sales_rub', 'Продажи', 'currency'),                   #+
        ('sales_qty', 'Продажи', 'count'),                      #+
        ('buyout_percent', 'Выкуп', 'percent'),                 #+
        ('returns_rub', 'Возвраты', 'currency'),                #+
        ('returns_qty', 'Возвраты', 'count'),                   #+
        ('cancels_qty', 'Отмены', 'count'),                     #+

        # Группа 3: Конверсии
        ('cr_cart', 'Конверсия в корзину', 'percent'),          #+
        ('cr_order', 'Конверсия в заказ', 'percent'),           #+

        # Группа 4: Реклама
        ('ad_expense', 'Рекламные расходы', 'currency'),        #+
        ('drr_orders', 'ДРРз', 'percent'),                      #+
        ('drr', 'ДРР', 'percent'),                              #+

        # Группа 5: Логистика и расходы
        ('logistics_rub', 'Логистика', 'currency'),             #+
        ('logistics_unit', 'Логистика за единицу', 'currency'), #+
        ('penalties', 'Штрафы', 'currency'),                    #+
        ('tax', 'Налог', 'currency'),                           #+

        # Группа 6: Операционные
        ('wb_commission', 'Комиссия ВБ', 'percent'),            #+
        ('turnover', 'Оборачиваемость', 'count'),               #-
        ('stock_qty', 'Остатки', 'count'),                      #+

        # Группа 7: Себестоимость
        ('cost_price', 'Себестоимость', 'currency'),            #+
    ]

    def __init__(self):
        """Инициализация коллекции с пустыми метриками"""
        self.metrics: Dict[str, MetricCard] = {}
        self._create_default_metrics()

    def _create_default_metrics(self) -> None:
        """Создаёт все 24 метрики с нулевыми значениями"""
        for key, name, unit in self.DEFINITIONS:
            self.metrics[key] = MetricCard(
                id=key,
                metricKey=key,
                name=name,
                unit=unit
            )

    def get(self, key: str) -> Optional[MetricCard]:
        """Получить метрику по ключу"""
        return self.metrics.get(key)

    def update(
            self,
            key: str,
            current_value: float,
            previous_value: float,
            current_percent: float = 0.0,
            previous_percent: float = 0.0
    ) -> None:
        """Обновить значения метрики и вычислить тренд автоматически"""
        metric = self.metrics.get(key)
        if not metric:
            return

        metric.currentValue = round(current_value, 2)
        metric.previousValue = round(previous_value, 2)
        metric.currentPercent = round(current_percent, 2)
        metric.previousPercent = round(previous_percent, 2)

        # Вычисляем тренд
        trend_percent = self._calculate_trend_percent(current_value, previous_value)
        trend_status = self._calculate_trend_status(trend_percent)

        # Инвертируем для "негативных" метрик
        if key in self.INVERTED_METRICS:
            if trend_status == 'up':
                trend_status = 'down'
            elif trend_status == 'down':
                trend_status = 'up'

        metric.trendPercent = trend_percent
        metric.trendStatus = trend_status

    def _calculate_trend_percent(self, current: float, previous: float) -> float:
        """Вычисляет процент изменения"""
        if previous == 0:
            return 0.0 if current == 0 else 100.0
        return round(((current - previous) / abs(previous)) * 100, 1)

    def _calculate_trend_status(self, trend_percent: float) -> Literal['up', 'down', 'flat']:
        """Определяет статус тренда"""
        if trend_percent > 1:
            return 'up'
        elif trend_percent < -1:
            return 'down'
        else:
            return 'flat'

    def to_list(self) -> List[dict]:
        """Преобразование в список словарей для JSON-ответа"""
        result = []
        for key, name, unit in self.DEFINITIONS:
            metric = self.metrics[key]
            result.append(metric.model_dump())
        return result


# ═══════════════════════════════════════════════════════════════
# МОДЕЛЬ API-ОТВЕТА
# ═══════════════════════════════════════════════════════════════

class MetricsApiResponse(BaseModel):
    """Ответ от POST /api/dashboard/metrics"""
    metrics: List[MetricCard]