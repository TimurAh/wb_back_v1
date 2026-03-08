"""
Модель для валидации ответа WB API воронки продуктов.
Преобразует вложенную структуру JSON в плоскую для записи в БД.

ОПТИМИЗАЦИЯ: Поддерживает извлечение данных из обоих периодов
(selected + past) за один запрос, сокращая количество API-вызовов вдвое.

Endpoint: /api/analytics/v3/sales-funnel/products
"""

from pydantic import BaseModel, field_validator, model_validator
from typing import Optional, Any, List
from datetime import datetime, date
from decimal import Decimal


class FunnelProductRow(BaseModel):
    """
    Одна строка воронки продукта из WB API.

    Валидирует и преобразует данные:
    - Вложенные объекты (product, statistic) → плоская структура
    - camelCase → snake_case
    - Пустые строки "" → None
    - Строковые даты → date

    Поддерживает извлечение как из 'selected', так и из 'past' периода.
    """

    # ===== Product (информация о товаре) =====
    nm_id: int
    vendor_code: Optional[str] = None
    brand_name: Optional[str] = None
    stocks_wb: Optional[int] = 0
    stocks_mp: Optional[int] = 0
    stocks_balance_sum: Optional[int] = 0

    # ===== Statistic (статистика за период) =====
    date_funnel: date
    open_count: Optional[int] = 0
    cart_count: Optional[int] = 0
    order_count: Optional[int] = 0
    order_sum: Optional[int] = 0
    cancel_count: Optional[int] = 0
    cancel_sum: Optional[int] = 0
    avg_price: Optional[Decimal] = Decimal('0')

    # Время до готовности
    time_to_ready_days: Optional[int] = 0
    time_to_ready_hours: Optional[int] = 0
    time_to_ready_mins: Optional[int] = 0

    # Локализация и конверсии
    localization_percent: Optional[int] = 0
    conversions_add_to_cart_percent: Optional[Decimal] = Decimal('0')
    conversions_cart_to_order_percent: Optional[Decimal] = Decimal('0')
    conversions_buyout_percent: Optional[Decimal] = Decimal('0')

    class Config:
        extra = 'ignore'

    @model_validator(mode='before')
    @classmethod
    def flatten_nested_structure(cls, data: Any) -> Any:
        """
        Преобразует вложенную структуру API в плоскую структуру для БД.

        Поддерживает извлечение данных как из 'selected', так и из 'past'.
        Тип периода передаётся через служебное поле '_period_type'.
        """
        if not isinstance(data, dict):
            return data

        result = {}

        # ===== Извлекаем данные из product =====
        product = data.get('product', {})
        if product:
            result['nm_id'] = product.get('nmId')
            result['vendor_code'] = product.get('vendorCode')
            result['brand_name'] = product.get('brandName')

            stocks = product.get('stocks', {})
            if stocks:
                result['stocks_wb'] = stocks.get('wb', 0)
                result['stocks_mp'] = stocks.get('mp', 0)
                result['stocks_balance_sum'] = stocks.get('balanceSum', 0)

        # ===== Извлекаем данные из statistic =====
        # Поддерживаем как 'selected', так и 'past' (передаётся через _period_type)
        statistic = data.get('statistic', {})
        period_type = data.get('_period_type', 'selected')  # selected или past
        period_data = statistic.get(period_type, {})

        if period_data:
            # Дата из period.start
            period = period_data.get('period', {})
            result['date_funnel'] = period.get('start')

            # Основные метрики
            result['open_count'] = period_data.get('openCount', 0)
            result['cart_count'] = period_data.get('cartCount', 0)
            result['order_count'] = period_data.get('orderCount', 0)
            result['order_sum'] = period_data.get('orderSum', 0)
            result['cancel_count'] = period_data.get('cancelCount', 0)
            result['cancel_sum'] = period_data.get('cancelSum', 0)
            result['avg_price'] = period_data.get('avgPrice', 0)
            result['localization_percent'] = period_data.get('localizationPercent', 0)

            # Извлекаем timeToReady (вложенный объект)
            time_to_ready = period_data.get('timeToReady', {})
            if time_to_ready:
                result['time_to_ready_days'] = time_to_ready.get('days', 0)
                result['time_to_ready_hours'] = time_to_ready.get('hours', 0)
                result['time_to_ready_mins'] = time_to_ready.get('mins', 0)

            # Извлекаем conversions (вложенный объект)
            conversions = period_data.get('conversions', {})
            if conversions:
                result['conversions_add_to_cart_percent'] = conversions.get('addToCartPercent', 0)
                result['conversions_cart_to_order_percent'] = conversions.get('cartToOrderPercent', 0)
                result['conversions_buyout_percent'] = conversions.get('buyoutPercent', 0)

        # ===== Если данные уже плоские (прямое создание модели) =====
        if not product and not statistic:
            return data

        # ===== Преобразуем пустые строки в None =====
        for key, value in result.items():
            if value == "" or (isinstance(value, str) and value.strip() == ""):
                result[key] = None

        return result

    @field_validator('date_funnel', mode='before')
    @classmethod
    def parse_date(cls, value: Any) -> Optional[date]:
        """Парсит date из строки."""
        if value is None or value == "":
            return None

        if isinstance(value, date):
            return value

        if isinstance(value, datetime):
            return value.date()

        if isinstance(value, str):
            try:
                return datetime.strptime(value, "%Y-%m-%d").date()
            except ValueError:
                return None

        return None

    @field_validator(
        'nm_id', 'stocks_wb', 'stocks_mp', 'stocks_balance_sum',
        'open_count', 'cart_count', 'order_count', 'order_sum',
        'cancel_count', 'cancel_sum', 'time_to_ready_days',
        'time_to_ready_hours', 'time_to_ready_mins', 'localization_percent',
        mode='before'
    )
    @classmethod
    def parse_int(cls, value: Any) -> Optional[int]:
        """Парсит int из строки или возвращает None."""
        if value is None or value == "":
            return None

        if isinstance(value, int):
            return value

        if isinstance(value, (float, Decimal)):
            return int(value)

        if isinstance(value, str):
            try:
                return int(float(value))
            except (ValueError, TypeError):
                return None

        return None

    @field_validator(
        'avg_price', 'conversions_add_to_cart_percent',
        'conversions_cart_to_order_percent', 'conversions_buyout_percent',
        mode='before'
    )
    @classmethod
    def parse_decimal(cls, value: Any) -> Optional[Decimal]:
        """Парсит Decimal из строки или возвращает None."""
        if value is None or value == "":
            return None

        if isinstance(value, Decimal):
            return value

        if isinstance(value, (int, float)):
            return Decimal(str(value))

        if isinstance(value, str):
            try:
                return Decimal(value)
            except Exception:
                return None

        return None

    def to_db_dict(self, user_id: int) -> dict:
        """
        Преобразует модель в словарь для вставки в БД.
        Добавляет user_id.

        Args:
            user_id: ID пользователя из таблицы user

        Returns:
            Словарь с полями, готовыми для INSERT
        """
        data = self.model_dump()
        data['user_id'] = user_id

        # Преобразуем Decimal в float для psycopg2
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = float(value)

        return data


def validate_funnel_products(
    raw_products: List[dict],
    period_type: str = 'selected'
) -> List[FunnelProductRow]:
    """
    Валидирует список продуктов воронки из WB API.

    Args:
        raw_products: Сырые данные из API (список из data.products)
        period_type: 'selected' или 'past' — какой период извлекать

    Returns:
        Список валидированных моделей

    Пример использования:
        response = api.get_funnel_products(...)
        products = response.get('data', {}).get('products', [])
        validated = validate_funnel_products(products, period_type='selected')
    """
    validated = []
    errors = []

    for i, raw in enumerate(raw_products):
        try:
            # Добавляем маркер периода для валидатора
            raw_with_period = {**raw, '_period_type': period_type}
            product = FunnelProductRow.model_validate(raw_with_period)
            validated.append(product)
        except Exception as e:
            errors.append({
                'index': i,
                'nm_id': raw.get('product', {}).get('nmId', 'unknown'),
                'error': str(e),
                'data': raw
            })

    if errors:
        from utils import logger
        logger.warning(
            f"Ошибки валидации воронки ({period_type}): "
            f"{len(errors)} из {len(raw_products)} записей"
        )
        # Логируем первые 5 ошибок для отладки
        for err in errors[:5]:
            logger.debug(
                f"Ошибка в записи {err['index']} (nm_id={err['nm_id']}): {err['error']}"
            )

    return validated


def extract_both_periods(raw_products: List[dict]) -> List[FunnelProductRow]:
    """
    Извлекает данные из ОБОИХ периодов (selected + past) из одного ответа API.

    ОПТИМИЗАЦИЯ: Это позволяет за один запрос получить данные за 2 дня,
    сокращая количество запросов к API вдвое!

    Args:
        raw_products: Сырые данные из API (список из data.products)

    Returns:
        Список валидированных моделей (selected + past объединены)

    Пример:
        Если в raw_products 50 товаров, то вернётся до 100 записей:
        - 50 записей за selected период
        - 50 записей за past период
    """
    all_validated = []

    # Извлекаем данные за selected период
    selected = validate_funnel_products(raw_products, period_type='selected')
    all_validated.extend(selected)

    # Извлекаем данные за past период
    past = validate_funnel_products(raw_products, period_type='past')
    all_validated.extend(past)

    return all_validated