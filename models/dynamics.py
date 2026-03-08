"""
Модели для блока «Динамика» (/api/dashboard/dynamics)
═══════════════════════════════════════════════════════════════

Структура аналогична metrics.py:
- Pydantic-модели для API-ответа
- DynamicsCollection — коллекция всех 24 показателей с сериями для графиков

Фронтенд (App.tsx) ожидает:
- dynamics: массив из 24 DynamicsMetric
- каждый содержит series.day / series.week / series.month
- id показателей совпадают с METRIC_DEFINITIONS на фронте
"""

from pydantic import BaseModel
from typing import List, Dict, Optional, Literal, Set


# ═══════════════════════════════════════════════════════════════
# PYDANTIC-МОДЕЛИ (структура данных для API)
# ═══════════════════════════════════════════════════════════════

class DynamicsPoint(BaseModel):
    """Одна точка на графике"""
    x: int       # порядковый номер (0, 1, 2, ...)
    y: float     # значение метрики


class DynamicsSeries(BaseModel):
    """Серии данных для графика (по дням, неделям, месяцам)"""
    day: List[DynamicsPoint]
    week: List[DynamicsPoint]
    month: List[DynamicsPoint]


class DynamicsMetric(BaseModel):
    """Один показатель для блока динамики"""
    id: str                                          # ключ ("net_profit", "orders_rub", ...)
    name: str                                        # название ("Чистая прибыль", ...)
    trendStatus: Literal['up', 'down', 'flat']       # направление тренда
    diffValue: float                                 # процент изменения
    unit: Literal['currency', 'percent', 'count']    # единица измерения
    series: DynamicsSeries                           # данные для графиков


# ═══════════════════════════════════════════════════════════════
# КОЛЛЕКЦИЯ ПОКАЗАТЕЛЕЙ ДИНАМИКИ
# ═══════════════════════════════════════════════════════════════

class DynamicsCollection:
    """
    Коллекция всех 24 показателей динамики.

    Аналог MetricsCollection из metrics.py, но для графиков:
    - Хранит серии точек (day / week / month) для каждого показателя
    - Автоматически рассчитывает тренд и процент изменения
    - Инвертирует тренд для «негативных» метрик (рост штрафов = плохо)
    - Автоматически агрегирует дневные данные в недельные и месячные

    Использование:
        collection = DynamicsCollection()
        collection.update('net_profit', day_points=[...])
        collection.update('orders_rub', day_points=[...])
        result = collection.to_list()  # → список словарей для JSON
    """

    # ─── Метрики, для которых рост = плохо (инвертируем тренд) ───
    # Полностью совпадает с MetricsCollection.INVERTED_METRICS
    INVERTED_METRICS: Set[str] = {
        'returns_rub', 'returns_qty', 'cancels_qty',
        'ad_expense', 'drr_orders', 'drr',
        'logistics_rub', 'logistics_unit', 'penalties', 'tax',
        'wb_commission', 'cost_price'
    }

    # ─── Определения всех 24 метрик: (key, name, unit) ───
    # Полностью совпадает с MetricsCollection.DEFINITIONS
    # и с METRIC_DEFINITIONS на фронтенде (App.tsx)
    DEFINITIONS = [
        # Группа 1: Прибыльность
        ('net_profit', 'Чистая прибыль', 'currency'),
        ('margin', 'Маржинальность', 'percent'),
        ('roi', 'ROI', 'percent'),

        # Группа 2: Заказы и продажи
        ('orders_rub', 'Заказы', 'currency'),
        ('orders_qty', 'Заказы', 'count'),
        ('sales_rub', 'Продажи', 'currency'),
        ('sales_qty', 'Продажи', 'count'),
        ('buyout_percent', 'Выкуп', 'percent'),
        ('returns_rub', 'Возвраты', 'currency'),
        ('returns_qty', 'Возвраты', 'count'),
        ('cancels_qty', 'Отмены', 'count'),

        # Группа 3: Конверсии
        ('cr_cart', 'Конверсия в корзину', 'percent'),
        ('cr_order', 'Конверсия в заказ', 'percent'),

        # Группа 4: Реклама
        ('ad_expense', 'Рекламные расходы', 'currency'),
        ('drr_orders', 'ДРРз', 'percent'),
        ('drr', 'ДРР', 'percent'),

        # Группа 5: Логистика и расходы
        ('logistics_rub', 'Логистика', 'currency'),
        ('logistics_unit', 'Логистика за единицу', 'currency'),
        ('penalties', 'Штрафы', 'currency'),
        ('tax', 'Налог', 'currency'),

        # Группа 6: Операционные
        ('wb_commission', 'Комиссия ВБ', 'percent'),
        ('turnover', 'Оборачиваемость', 'count'),
        ('stock_qty', 'Остатки', 'count'),

        # Группа 7: Себестоимость
        ('cost_price', 'Себестоимость', 'currency'),
    ]

    def __init__(self):
        """Инициализация коллекции с пустыми метриками (24 шт.)"""
        self.metrics: Dict[str, DynamicsMetric] = {}
        self._create_default_metrics()

    def _create_default_metrics(self) -> None:
        """Создаёт все 24 метрики с пустыми сериями и нулевым трендом"""
        for key, name, unit in self.DEFINITIONS:
            self.metrics[key] = DynamicsMetric(
                id=key,
                name=name,
                trendStatus='flat',
                diffValue=0.0,
                unit=unit,
                series=DynamicsSeries(day=[], week=[], month=[])
            )

    def get(self, key: str) -> Optional[DynamicsMetric]:
        """Получить метрику по ключу"""
        return self.metrics.get(key)

    def update(
        self,
        key: str,
        day_points: List[DynamicsPoint],
        week_points: Optional[List[DynamicsPoint]] = None,
        month_points: Optional[List[DynamicsPoint]] = None,
    ) -> None:
        """
        Обновить серии данных для показателя и вычислить тренд автоматически.

        Простыми словами:
        - Передаёшь дневные точки → автоматически создаются недельные и месячные
        - Тренд считается по сравнению первой и второй половины дневных данных
        - Для «негативных» метрик (штрафы, возвраты и т.д.) тренд инвертируется

        Args:
            key:          ключ показателя (например, 'net_profit')
            day_points:   точки по дням (обязательно)
            week_points:  точки по неделям (если None — агрегируются из day_points)
            month_points: точки по месяцам (если None — агрегируются из day_points)
        """
        metric = self.metrics.get(key)
        if not metric:
            return

        # Автоматическая агрегация, если не переданы явно
        if week_points is None:
            week_points = self._aggregate_points(day_points, 7)
        if month_points is None:
            month_points = self._aggregate_points(day_points, 30)

        # Обновляем серии
        metric.series = DynamicsSeries(
            day=day_points,
            week=week_points,
            month=month_points,
        )

        # Вычисляем тренд из дневных данных
        diff_value, trend_status = self._calculate_trend(day_points)

        # Инвертируем для «негативных» метрик
        # (рост штрафов/возвратов/расходов = красная стрелка вниз)
        if key in self.INVERTED_METRICS:
            if trend_status == 'up':
                trend_status = 'down'
            elif trend_status == 'down':
                trend_status = 'up'

        metric.diffValue = diff_value
        metric.trendStatus = trend_status

    # ─── Внутренние методы ───

    def _aggregate_points(
        self,
        day_points: List[DynamicsPoint],
        chunk_size: int
    ) -> List[DynamicsPoint]:
        """
        Агрегирует дневные точки в более крупные периоды.

        Пример: 14 дневных точек → 2 недельных (chunk_size=7)
        Каждый чанк суммируется в одну точку.
        """
        if not day_points:
            return []

        result: List[DynamicsPoint] = []
        for i in range(0, len(day_points), chunk_size):
            chunk = day_points[i:i + chunk_size]
            total = sum(p.y for p in chunk)
            result.append(DynamicsPoint(x=len(result), y=round(total, 2)))
        return result

    def _calculate_trend(
        self,
        day_points: List[DynamicsPoint]
    ) -> tuple:
        """
        Рассчитывает тренд, сравнивая среднее первой и второй половины данных.

        Логика:
        - Делим дневные точки пополам
        - Считаем среднее каждой половины
        - Процент изменения = (вторая - первая) / первая × 100

        Возвращает: (diff_value, trend_status)
        """
        if len(day_points) < 2:
            return (0.0, 'flat')

        mid = len(day_points) // 2
        first_half = day_points[:mid]
        second_half = day_points[mid:]

        avg_first = sum(p.y for p in first_half) / len(first_half)
        avg_second = sum(p.y for p in second_half) / len(second_half)

        # Защита от деления на ноль
        if avg_first == 0:
            if avg_second > 0:
                return (100.0, 'up')
            return (0.0, 'flat')

        change_percent = ((avg_second - avg_first) / abs(avg_first)) * 100
        diff_value = round(change_percent, 1)

        if change_percent > 1:
            trend_status = 'up'
        elif change_percent < -1:
            trend_status = 'down'
        else:
            trend_status = 'flat'

        return (diff_value, trend_status)

    def to_list(self) -> List[dict]:
        """
        Преобразование в список словарей для JSON-ответа.
        Порядок совпадает с DEFINITIONS (и с фронтендом).
        """
        result = []
        for key, name, unit in self.DEFINITIONS:
            metric = self.metrics[key]
            result.append(metric.model_dump())
        return result


# ═══════════════════════════════════════════════════════════════
# МОДЕЛЬ API-ОТВЕТА
# ═══════════════════════════════════════════════════════════════

class DynamicsApiResponse(BaseModel):
    """Ответ от POST /api/dashboard/dynamics"""
    primary_data: Dict[str, List[float]] = {}
    compare_data: Dict[str, List[float]] = {}