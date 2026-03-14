"""
API эндпоинты для FastAPI — разделённые по блокам дашборда
═══════════════════════════════════════════════════════════════
"""
import logging

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from decimal import Decimal

from models import (
    # Общие
    DateRangeRequest,
    # Метрики
    MetricsCollection, MetricsApiResponse,
    # Динамика
    DynamicsPoint, DynamicsSeries, DynamicsMetric, DynamicsApiResponse,
    # Детализация
    MetricValue, DetailRow, DetailsApiResponse,
    # Себестоимость (API)
    CostPriceItem, CostPricesApiResponse,
    CostPriceSaveRequest, CostPriceSaveResponse, DynamicsCollection
)
from database import (
    get_users_load_info,
    get_metrics_for_period_from_report,
    get_metrics_for_period_from_funnel,
    # get_dynamics_by_day,
    get_details_by_product,
    get_metrics_for_period_from_advert_stats,
    get_cost_price,
    insert_cost_price,
    get_dynamic_for_period_from_report,
    get_dynamic_for_period_from_advert_stats,
    get_dynamic_for_period_from_funnel,
    get_detail_for_period_from_report,
    get_detail_for_period_from_advert_stats,
    get_detail_for_period_from_funnel,
    get_filters_for_user
)
from utils import logger

router = APIRouter()


# ═══════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════════════

def calculate_trend(current: float, previous: float) -> tuple:
    """
    Рассчитывает тренд и процент изменения.
    Возвращает: (trend_status, trend_percent)
    """
    if previous == 0:
        if current > 0:
            return ('up', 100.0)
        return ('flat', 0.0)

    change_percent = ((current - previous) / abs(previous)) * 100

    if change_percent > 1:
        return ('up', round(change_percent, 1))
    elif change_percent < -1:
        return ('down', round(change_percent, 1))
    else:
        return ('flat', 0.0)


def build_metrics(
        primary_data_report: Dict[str, Any],
        compare_data_report: Optional[Dict[str, Any]],
        primary_data_funnel: Dict[str, Any],
        compare_data_funnel: Optional[Dict[str, Any]],
        primary_data_advert_stats: Dict[str, Any],
        compare_data_advert_stats: Optional[Dict[str, Any]]
) -> List[dict]:
    """
    Формирует список карточек метрик из сырых данных.
    Логика полностью сохранена из предыдущей версии.
    """
    logger.debug(f"build_metrics primary: report={primary_data_report}, funnel={primary_data_funnel}")
    logger.debug(f"build_metrics compare: report={compare_data_report}, funnel={compare_data_funnel}")

    if compare_data_report is None:
        compare_data_report = {k: 0 for k in primary_data_report.keys()}
    if compare_data_funnel is None:
        compare_data_funnel = {k: 0 for k in primary_data_funnel.keys()}
    if compare_data_advert_stats is None:
        compare_data_advert_stats = {k: 0 for k in primary_data_advert_stats.keys()}

    # ===== Извлекаем сырые данные =====

    revenue = float(primary_data_report.get('revenue', 0))
    revenue_prev = float(compare_data_report.get('revenue', 0))

    acceptance = float(primary_data_report.get('acceptance', 0))
    acceptance_prev = float(compare_data_report.get('acceptance', 0))

    deduction = float(primary_data_report.get('deduction', 0))
    deduction_prev = float(compare_data_report.get('deduction', 0))

    storage = float(primary_data_report.get('storage', 0))
    storage_prev = float(compare_data_report.get('storage', 0))

    ppvz_for_pay = float(primary_data_report.get('ppvz_for_pay', 0))
    ppvz_for_pay_prev = float(compare_data_report.get('ppvz_for_pay', 0))

    sum_cost_price = float(primary_data_report.get('sum_cost_price', 0))
    sum_cost_price_prev = float(compare_data_report.get('sum_cost_price', 0))

    sum_for_contribution = float(primary_data_report.get('sum_for_contribution', 0))
    sum_for_contribution_prev = float(compare_data_report.get('sum_for_contribution', 0))

    returns_sum = float(primary_data_report.get('returns_sum', 0))
    returns_prev = float(compare_data_report.get('returns_sum', 0))

    returns_qty = int(primary_data_report.get('returns_quantity', 0))
    returns_qty_prev = int(compare_data_report.get('returns_quantity', 0))

    logistics = float(primary_data_report.get('logistics', 0))
    logistics_prev = float(compare_data_report.get('logistics', 0))

    commission = float(primary_data_report.get('commission', 0))
    commission_prev = float(compare_data_report.get('commission', 0))

    penalties = float(primary_data_report.get('penalties', 0))
    penalties_prev = float(compare_data_report.get('penalties', 0))

    orders_rub = float(primary_data_funnel.get('order_sum', 0))
    orders_rub_prev = float(compare_data_funnel.get('order_sum', 0))

    orders_qty = int(primary_data_funnel.get('order_count', 0))
    orders_qty_prev = int(compare_data_funnel.get('order_count', 0))

    sales_rub = float(primary_data_report.get('revenue', 0))
    sales_rub_prev = float(compare_data_report.get('revenue', 0))

    sales_qty = int(primary_data_report.get('sales_count', 0))
    sales_qty_prev = int(compare_data_report.get('sales_count', 0))

    cancels_qty = int(primary_data_funnel.get('cancel_count', 0))
    cancels_qty_prev = int(compare_data_funnel.get('cancel_count', 0))

    ad_expense = float(primary_data_advert_stats.get('ad_expense', 0))
    ad_expense_prev = float(compare_data_advert_stats.get('ad_expense', 0))

    stock_qty = int(primary_data_funnel.get('stocks_balance_sum', 0))
    stock_qty_prev = int(compare_data_funnel.get('stocks_balance_sum', 0))

    turnover = float(primary_data_report.get('turnover', 0))
    turnover_prev = float(compare_data_report.get('turnover', 0))

    cr_cart = float(primary_data_funnel.get('conversions_add_to_cart_percent', 0))
    cr_cart_prev = float(compare_data_funnel.get('conversions_add_to_cart_percent', 0))

    cr_order = float(primary_data_funnel.get('conversions_cart_to_order_percent', 0))
    cr_order_prev = float(compare_data_funnel.get('conversions_cart_to_order_percent', 0))

    # ===== Вычисляем производные метрики =====

    coef_contribution = 0.1048
    tax = sum_for_contribution * coef_contribution
    tax_prev = sum_for_contribution_prev * coef_contribution
    net_profit = ppvz_for_pay - logistics - sum_cost_price - tax - ad_expense - acceptance - storage - deduction
    net_profit_prev = ppvz_for_pay_prev - logistics_prev - sum_cost_price_prev - tax_prev - ad_expense_prev - acceptance_prev - storage_prev - deduction_prev - storage_prev

    margin = (net_profit / revenue * 100) if revenue > 0 else 0
    margin_prev = (net_profit_prev / revenue_prev * 100) if revenue_prev > 0 else 0

    roi = (net_profit / sum_cost_price * 100) if sum_cost_price > 0 else 0
    roi_prev = (net_profit_prev / sum_cost_price_prev * 100) if sum_cost_price_prev > 0 else 0

    buyout_percent = (sales_qty / orders_qty * 100) if orders_qty > 0 else 0
    buyout_percent_prev = (sales_qty_prev / orders_qty_prev * 100) if orders_qty_prev > 0 else 0

    logistics_unit = (logistics / (sales_qty - returns_qty)) if (sales_qty - returns_qty) > 0 else 0
    logistics_unit_prev = (logistics_prev / (sales_qty_prev - returns_qty_prev)) if (
                                                                                                sales_qty_prev - returns_qty_prev) > 0 else 0

    drr_orders = (ad_expense / orders_rub * 100) if orders_rub > 0 else 0
    drr_orders_prev = (ad_expense_prev / orders_rub_prev * 100) if orders_rub_prev > 0 else 0

    drr = (ad_expense / sales_rub * 100) if sales_rub > 0 else 0
    drr_prev = (ad_expense_prev / sales_rub_prev * 100) if sales_rub_prev > 0 else 0

    # ===== Заполняем коллекцию метрик =====

    metrics = MetricsCollection()

    metrics.update('net_profit', net_profit, net_profit_prev)
    metrics.update('margin', margin, margin_prev)
    metrics.update('roi', roi, roi_prev)
    metrics.update('orders_rub', orders_rub, orders_rub_prev)
    metrics.update('orders_qty', orders_qty, orders_qty_prev)
    metrics.update('sales_rub', sales_rub, sales_rub_prev)
    metrics.update('sales_qty', sales_qty, sales_qty_prev)
    metrics.update('buyout_percent', buyout_percent, buyout_percent_prev)
    metrics.update('returns_rub', returns_sum, returns_prev)
    metrics.update('returns_qty', returns_qty, returns_qty_prev)
    metrics.update('cancels_qty', cancels_qty, cancels_qty_prev)
    metrics.update('cr_cart', cr_cart, cr_cart_prev)
    metrics.update('cr_order', cr_order, cr_order_prev)
    metrics.update('ad_expense', ad_expense, ad_expense_prev)
    metrics.update('drr_orders', drr_orders, drr_orders_prev)
    metrics.update('drr', drr, drr_prev)
    metrics.update('logistics_rub', logistics, logistics_prev)
    metrics.update('logistics_unit', logistics_unit, logistics_unit_prev)
    metrics.update('penalties', penalties, penalties_prev)
    metrics.update('tax', tax, tax_prev)
    metrics.update('wb_commission', commission, commission_prev)
    metrics.update('turnover', turnover, turnover_prev)
    metrics.update('stock_qty', stock_qty, stock_qty_prev)
    metrics.update('cost_price', sum_cost_price, sum_cost_price_prev)

    return metrics.to_list()


# ═══════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ — ДИНАМИКА
# ═══════════════════════════════════════════════════════════════

COEF_CONTRIBUTION = 0.1048

ALL_METRIC_KEYS = [
    'net_profit', 'margin', 'roi',
    'orders_rub', 'orders_qty',
    'sales_rub', 'sales_qty',
    'buyout_percent',
    'returns_rub', 'returns_qty',
    'cancels_qty',
    'cr_cart', 'cr_order',
    'ad_expense', 'drr_orders', 'drr',
    'logistics_rub', 'logistics_unit',
    'penalties', 'tax',
    'wb_commission', 'turnover',
    'stock_qty', 'cost_price',
]


def _safe_float(value, default=0.0) -> float:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _safe_int(value, default=0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _get_date_key(row: Dict[str, Any]) -> str:
    for key in ('date', 'dt', 'report_date', 'day'):
        if key in row:
            return str(row[key])
    return ''


def _compute_day_metrics(
        report_row: Dict[str, Any],
        funnel_row: Dict[str, Any],
        advert_row: Dict[str, Any],
) -> Dict[str, float]:
    """
    Вычисляет все 24 метрики для ОДНОГО ДНЯ.
    Логика повторяет build_metrics.
    """
    # --- Из report ---
    revenue = _safe_float(report_row.get('revenue'))
    acceptance = _safe_float(report_row.get('acceptance'))
    deduction = _safe_float(report_row.get('deduction'))
    storage = _safe_float(report_row.get('storage'))
    ppvz_for_pay = _safe_float(report_row.get('ppvz_for_pay'))
    sum_cost_price = _safe_float(report_row.get('sum_cost_price'))
    sum_for_contribution = _safe_float(report_row.get('sum_for_contribution'))
    returns_sum = _safe_float(report_row.get('returns_sum'))
    returns_qty = _safe_int(report_row.get('returns_quantity'))
    logistics = _safe_float(report_row.get('logistics'))
    commission = _safe_float(report_row.get('commission'))
    penalties = _safe_float(report_row.get('penalties'))
    sales_count = _safe_int(report_row.get('sales_count'))
    turnover = _safe_float(report_row.get('turnover'))
    sales_rub = revenue

    # --- Из funnel (пока пусто) ---
    orders_rub = _safe_float(funnel_row.get('order_sum'))
    orders_qty = _safe_int(funnel_row.get('order_count'))
    cancels_qty = _safe_int(funnel_row.get('cancel_count'))
    stock_qty = _safe_int(funnel_row.get('stocks_balance_sum'))
    cr_cart = _safe_float(funnel_row.get('conversions_add_to_cart_percent'))
    cr_order = _safe_float(funnel_row.get('conversions_cart_to_order_percent'))

    # --- Из advert (пока пусто) ---
    ad_expense = _safe_float(advert_row.get('ad_expense'))

    # --- Производные ---
    tax = sum_for_contribution * COEF_CONTRIBUTION

    net_profit = (
            ppvz_for_pay - logistics - sum_cost_price
            - tax - ad_expense - acceptance - storage - deduction
    )

    margin = (net_profit / revenue * 100) if revenue > 0 else 0.0
    roi = (net_profit / sum_cost_price * 100) if sum_cost_price > 0 else 0.0
    buyout_percent = (sales_count / orders_qty * 100) if orders_qty > 0 else 0.0

    denom = sales_count - returns_qty
    logistics_unit = (logistics / denom) if denom > 0 else 0.0

    drr_orders = (ad_expense / orders_rub * 100) if orders_rub > 0 else 0.0
    drr = (ad_expense / sales_rub * 100) if sales_rub > 0 else 0.0

    return {
        'net_profit': round(net_profit, 2),
        'margin': round(margin, 1),
        'roi': round(roi, 1),
        'orders_rub': round(orders_rub, 2),
        'orders_qty': float(orders_qty),
        'sales_rub': round(sales_rub, 2),
        'sales_qty': float(sales_count),
        'buyout_percent': round(buyout_percent, 1),
        'returns_rub': round(returns_sum, 2),
        'returns_qty': float(returns_qty),
        'cancels_qty': float(cancels_qty),
        'cr_cart': round(cr_cart, 1),
        'cr_order': round(cr_order, 1),
        'ad_expense': round(ad_expense, 2),
        'drr_orders': round(drr_orders, 1),
        'drr': round(drr, 1),
        'logistics_rub': round(logistics, 2),
        'logistics_unit': round(logistics_unit, 2),
        'penalties': round(penalties, 2),
        'tax': round(tax, 2),
        'wb_commission': round(commission, 2),
        'turnover': round(turnover, 1),
        'stock_qty': float(stock_qty),
        'cost_price': round(sum_cost_price, 2),
    }


def _build_period_arrays(
        daily_report: List[Dict[str, Any]],
        daily_funnel: List[Dict[str, Any]],
        daily_advert: List[Dict[str, Any]],
) -> Dict[str, List[float]]:
    """Собирает массивы метрик по дням, объединяя 3 источника по индексу."""

    report_list = daily_report or []
    funnel_list = daily_funnel or []
    advert_list = daily_advert or []

    # Берём длину по ОСНОВНОМУ источнику (report), а не по максимуму
    num_days = max(len(report_list), len(funnel_list), len(advert_list))

    if num_days == 0:
        return {key: [] for key in ALL_METRIC_KEYS}

    result: Dict[str, List[float]] = {key: [] for key in ALL_METRIC_KEYS}

    for i in range(num_days):
        report_row = report_list[i] if i < len(report_list) else {}
        funnel_row = funnel_list[i] if i < len(funnel_list) else {}
        advert_row = advert_list[i] if i < len(advert_list) else {}

        day_metrics = _compute_day_metrics(report_row, funnel_row, advert_row)

        for key in ALL_METRIC_KEYS:
            result[key].append(day_metrics[key])

    return result


def build_dynamics(
        daily_report: List[Dict[str, Any]],
        daily_funnel: List[Dict[str, Any]],
        daily_advert: List[Dict[str, Any]],
        compare_daily_report: Optional[List[Dict[str, Any]]] = None,
        compare_daily_funnel: Optional[List[Dict[str, Any]]] = None,
        compare_daily_advert: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Формирует:
    {
        "primary_data": { "net_profit": [...], ... },
        "compare_data": { "net_profit": [...], ... }
    }
    """
    logger.debug(
        f"build_dynamics: report={len(daily_report or [])} days, "
        f"funnel={len(daily_funnel or [])} days, "
        f"advert={len(daily_advert or [])} days"
    )
    logger.debug(f"build_dynamics RES: report={daily_report} , "
                 f"funnel RES={daily_funnel}, "
                 f"advert RES={daily_advert}")

    primary_data = _build_period_arrays(
        daily_report or [], daily_funnel or [], daily_advert or [],
    )

    compare_data: Dict[str, List[float]] = {}
    if compare_daily_report is not None:
        compare_data = _build_period_arrays(
            compare_daily_report or [],
            compare_daily_funnel or [],
            compare_daily_advert or [],
        )

    return {
        "primary_data": primary_data,
        "compare_data": compare_data,
    }


def build_dynamics_old(
        daily_data: List[Dict[str, Any]],
        primary_start: str,
        primary_end: str
) -> List[dict]:
    """Формирует данные для графиков динамики."""

    if not daily_data:
        empty_series = DynamicsSeries(day=[], week=[], month=[])
        return [
            DynamicsMetric(
                id=key, name=name, trendStatus='flat',
                diffValue=0, unit=unit, series=empty_series
            ).model_dump()
            for key, name, unit in [
                ('net_profit', 'Чистая прибыль', 'currency'),
                ('revenue', 'Выручка', 'currency'),
                ('orders_count', 'Заказы', 'count'),
                ('returns_sum', 'Возвраты', 'currency'),
                ('expenses', 'Расходы', 'currency'),
            ]
        ]

    def make_series(key: str) -> DynamicsSeries:
        day_points = [
            DynamicsPoint(x=i, y=round(row.get(key, 0), 2))
            for i, row in enumerate(daily_data)
        ]
        week_points = []
        for i in range(0, len(daily_data), 7):
            chunk = daily_data[i:i + 7]
            total = sum(row.get(key, 0) for row in chunk)
            week_points.append(DynamicsPoint(x=len(week_points), y=round(total, 2)))
        month_points = []
        for i in range(0, len(daily_data), 30):
            chunk = daily_data[i:i + 30]
            total = sum(row.get(key, 0) for row in chunk)
            month_points.append(DynamicsPoint(x=len(month_points), y=round(total, 2)))
        return DynamicsSeries(day=day_points, week=week_points, month=month_points)

    def calc_diff(key: str) -> tuple:
        if len(daily_data) < 2:
            return ('flat', 0)
        first_half = daily_data[:len(daily_data) // 2]
        second_half = daily_data[len(daily_data) // 2:]
        avg_first = sum(row.get(key, 0) for row in first_half) / len(first_half)
        avg_second = sum(row.get(key, 0) for row in second_half) / len(second_half)
        return calculate_trend(avg_second, avg_first)

    metrics_def = [
        ('net_profit', 'Чистая прибыль', 'currency'),
        ('revenue', 'Выручка', 'currency'),
        ('orders_count', 'Заказы', 'count'),
        ('returns_sum', 'Возвраты', 'currency'),
        ('expenses', 'Расходы', 'currency'),
    ]

    result = []
    for key, name, unit in metrics_def:
        trend_status, diff_value = calc_diff(key)
        if key in ['returns_sum', 'expenses']:
            if trend_status == 'up':
                trend_status = 'down'
            elif trend_status == 'down':
                trend_status = 'up'

        result.append(DynamicsMetric(
            id=key, name=name, trendStatus=trend_status,
            diffValue=diff_value, unit=unit, series=make_series(key)
        ).model_dump())

    return result


# ═══════════════════════════════════════════════════════════════
# ЭНДПОИНТ 1: POST /api/dashboard/metrics
# ═══════════════════════════════════════════════════════════════

@router.post(
    "/dashboard/metrics",
    response_model=MetricsApiResponse,
    summary="Карточки показателей",
    description="Возвращает 24 метрики за указанный период с трендами",
    tags=["Dashboard"]
)
async def get_dashboard_metrics(request: DateRangeRequest):
    """
    Принимает:
    - primary: основной период {start, end}
    - compare: период сравнения (опционально)
    - brand: список для фильтра брендов (опционально)
    - category: список для фильтра категорий (опционально)
    - sa_name: список для фильтра артиклов (опционально)
    Возвращает:
    - metrics: массив из 24 карточек показателей
    """
    try:
        user_id = request.user_id
        if not user_id:
            raise ValueError(f"Пользователь не передан {request}")

        logger.info(
            f"[Metrics] user={user_id}, "
            f"primary={request.primary.start}-{request.primary.end}, "
            f"compare={request.compare}"
        )

        # Данные из отчёта
        primary_report = get_metrics_for_period_from_report(
            user_id=user_id,
            date_from=request.primary.start,
            date_to=request.primary.end
        )
        compare_report = None
        if request.compare:
            compare_report = get_metrics_for_period_from_report(
                user_id=user_id,
                date_from=request.compare.start,
                date_to=request.compare.end
            )

        # Данные из воронки
        primary_funnel = get_metrics_for_period_from_funnel(
            user_id=user_id,
            date_from=request.primary.start,
            date_to=request.primary.end
        )
        compare_funnel = None
        if request.compare:
            compare_funnel = get_metrics_for_period_from_funnel(
                user_id=user_id,
                date_from=request.compare.start,
                date_to=request.compare.end
            )

        # Данные из рекламной статистики
        primary_advert = get_metrics_for_period_from_advert_stats(
            user_id=user_id,
            date_from=request.primary.start,
            date_to=request.primary.end
        )
        compare_advert = None
        if request.compare:
            compare_advert = get_metrics_for_period_from_advert_stats(
                user_id=user_id,
                date_from=request.compare.start,
                date_to=request.compare.end
            )

        # Формируем метрики
        metrics_list = build_metrics(
            primary_report, compare_report,
            primary_funnel, compare_funnel,
            primary_advert, compare_advert
        )

        logger.info(f"[Metrics] Returning {len(metrics_list)} metrics")
        return MetricsApiResponse(metrics=metrics_list)

    except TypeError as e:
        logger.warning(
            f"[Metrics] Нет данных за период "
            f"{request.primary.start} - {request.primary.end} "
            f"для user_id={request.user_id}: {e}"
        )
        raise HTTPException(
            status_code=404,
            detail=(
                f"Нет данных за период "
                f"{request.primary.start} - {request.primary.end}"
            )
        )

    except Exception as e:
        logger.error(f"[Metrics] Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════
# ЭНДПОИНТ 2: POST /api/dashboard/dynamics
# ═══════════════════════════════════════════════════════════════

@router.post(
    "/dashboard/dynamics",
    response_model=DynamicsApiResponse,
    summary="Данные для графиков динамики",
    description="Возвращает массивы значений по дням для всех 24 метрик",
    tags=["Dashboard"]
)
async def get_dashboard_dynamics(request: DateRangeRequest):
    """
        Принимает:
        - primary: основной период {start, end}
        - compare: период сравнения (опционально)
        - brand: список для фильтра брендов (опционально)
        - category: список для фильтра категорий (опционально)
        - sa_name: список для фильтра артиклов (опционально)
        Возвращает:
        - compare_data: массив из 24 динамик за прошлый период
        - primary_data: массив из 24 динамик за текущий период
        """
    try:
        user_id = request.user_id
        if not user_id:
            raise ValueError(f"Пользователь не передан {request}")

        logger.info(
            f"[Dynamics] user={user_id}, "
            f"primary={request.primary.start}-{request.primary.end}, "
            f"compare={request.compare}"
        )

        # ===== Основной период =====
        primary_report = get_dynamic_for_period_from_report(
            user_id=user_id,
            date_from=request.primary.start,
            date_to=request.primary.end,
        )
        primary_funnel = get_dynamic_for_period_from_funnel(
            user_id=user_id,
            date_from=request.primary.start,
            date_to=request.primary.end, )
        primary_advert = get_dynamic_for_period_from_advert_stats(
            user_id=user_id,
            date_from=request.primary.start,
            date_to=request.primary.end,
        )

        # ===== Период сравнения =====
        compare_report = get_dynamic_for_period_from_report(
            user_id=user_id,
            date_from=request.compare.start,
            date_to=request.compare.end,
        )
        compare_funnel = get_dynamic_for_period_from_funnel(
            user_id=user_id,
            date_from=request.compare.start,
            date_to=request.compare.end, )
        compare_advert = get_dynamic_for_period_from_advert_stats(
            user_id=user_id,
            date_from=request.compare.start,
            date_to=request.compare.end,
        )

        # ===== Формируем результат =====

        # удалить потом
        logger.debug(
            f"[Dynamics debug] primary_report type={type(primary_report)}, len={len(primary_report) if primary_report else 0}")
        if primary_report and len(primary_report) > 0:
            logger.debug(f"[Dynamics debug] first row keys={list(primary_report[0].keys())}")
            logger.debug(f"[Dynamics debug] first row={primary_report[0]}")

        # конец удаления

        dynamics_result = build_dynamics(
            daily_report=primary_report,
            daily_funnel=primary_funnel,
            daily_advert=primary_advert,
            compare_daily_report=compare_report,
            compare_daily_funnel=compare_funnel,
            compare_daily_advert=compare_advert,
        )
        logger.debug(f"[Dynamics test] dynamics_result= {dynamics_result}")
        logger.info(
            f"[Dynamics] Returning "
            f"{len(dynamics_result['primary_data'].get('net_profit', []))} "
            f"primary points"
        )

        return DynamicsApiResponse(**dynamics_result)

    except Exception as e:
        logger.error(f"[Dynamics] Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════
# ЭНДПОИНТ 3: GET /api/dashboard/details
# ═══════════════════════════════════════════════════════════════

@router.post(
    "/dashboard/details",
    response_model=DetailsApiResponse,
    summary="Таблица детализации по товарам",
    description="Возвращает все товары с 24 показателями за период",
    tags=["Dashboard"]
)
async def get_dashboard_details(request: DateRangeRequest):
    """
    Принимает:
    - primary: основной период {start, end}
    - compare: период сравнения (опционально)
    - brand: список для фильтра брендов (опционально)
    - category: список для фильтра категорий (опционально)
    - sa_name: список для фильтра артиклов (опционально)
    Возвращает:
    - details: массив строк (один товар = одна строка с 24 MetricValue)
    """
    try:
        user_id = request.user_id
        if not user_id:
            raise ValueError(f"Пользователь не передан {request}")

        logger.info(
            f"[Details] user={user_id}, "
            f"primary={request.primary.start}-{request.primary.end}, "
            f"compare={request.compare}"
        )

        # ===== Основной период =====

        primary_report = get_detail_for_period_from_report(
            user_id=user_id,
            date_from=request.primary.start,
            date_to=request.primary.end,
        )

        primary_funnel = get_detail_for_period_from_funnel(
            user_id=user_id,
            date_from=request.primary.start,
            date_to=request.primary.end,
        )

        primary_advert = get_detail_for_period_from_advert_stats(
            user_id=user_id,
            date_from=request.primary.start,
            date_to=request.primary.end,
        )

        # ===== Период сравнения =====
        compare_report = None
        compare_funnel = None
        compare_advert = None

        if request.compare:
            compare_report = get_detail_for_period_from_report(
                user_id=user_id,
                date_from=request.compare.start,
                date_to=request.compare.end,
            )
            compare_funnel = get_detail_for_period_from_funnel(
                user_id=user_id,
                date_from=request.compare.start,
                date_to=request.compare.end,
            )
            compare_advert = get_detail_for_period_from_advert_stats(
            user_id=user_id,
            date_from=request.compare.start,
            date_to=request.compare.end,
        )

        # ===== Формируем результат =====
        details_result = build_details(
            report_data=primary_report,
            funnel_data=primary_funnel,
            advert_data=primary_advert,
            compare_report_data=compare_report,
            compare_funnel_data=compare_funnel,
            compare_advert_data=compare_advert,
        )

        logger.info(f"[Details] Returning {len(details_result['details'])} products")

        return DetailsApiResponse(**details_result)

    except Exception as e:
        logger.error(f"[Details] Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════
# ЭНДПОИНТ 4: GET /api/dashboard/costprices
# ═══════════════════════════════════════════════════════════════

@router.get(
    "/dashboard/costprices",
    response_model=CostPricesApiResponse,
    summary="Список товаров с себестоимостью",
    description="Возвращает все товары с сохранёнными значениями себестоимости и фулфилмента",
    tags=["Dashboard"]
)
async def get_dashboard_costprices(user_id: int = Query(..., description="ID пользователя")):
    """
    Запрос без тела. Пользователь определяется по авторизации.

    Возвращает:
    - items: массив товаров с costPrice и fulfillment (null = не указана)
    """
    try:
        logger.info(f"[CostPrices] user={user_id}")

        items = get_cost_price(user_id)

        logger.info(f"[CostPrices] Returning {len(items.items)} items")
        return items

    except Exception as e:
        logger.error(f"[CostPrices] Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════
# ЭНДПОИНТ 5: POST /api/dashboard/costprices/load
# ═══════════════════════════════════════════════════════════════

@router.post(
    "/dashboard/costprices/load",
    response_model=CostPriceSaveResponse,
    summary="Сохранить себестоимость товара",
    description="Сохраняет введённые значения себестоимости и фулфилмента для одного товара",
    tags=["Dashboard"]
)
async def save_costprice(request: CostPriceSaveRequest):
    """
    Принимает:
    - rowId: ID товара
    - costPrice: себестоимость (null = очистить)
    - fulfillment: фулфилмент (null = очистить)

    Возвращает:
    - success: true/false
    """
    try:
        user_id = request.user_id
        if not user_id:
            raise ValueError(f"Пользователь не передан {request}")

        logger.debug(
            f"[CostPrices/Save] user={user_id}, "
            f"nm_id={request.nm_id}, "
            f"costPrice={request.costPrice}, "
            f"fulfillment={request.fulfillment}"
        )

        insert_cost_price(user_id, request.nm_id, request.costPrice, request.fulfillment, )
        logger.info(f"[CostPrices/Save] Saved for nm_id={request.nm_id} user_id = {user_id}")
        return CostPriceSaveResponse(success=True)

    except Exception as e:
        logger.error(f"[CostPrices/Save] Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════
# ОСТАЛЬНЫЕ ЭНДПОИНТЫ (без изменений)
# ═══════════════════════════════════════════════════════════════

class UserLoadInfo(BaseModel):
    user_id: int
    username: Optional[str] = None
    records_count: int = 0
    first_record_date: Optional[str] = None
    last_record_date: Optional[str] = None


class UserLoadInfoResponse(BaseModel):
    success: bool = True
    data: List[UserLoadInfo]
    total_users: int
    total_records: int

class FilterResponse(BaseModel):
    """Ответ с уникальными значениями для фильтров"""
    sa_name: Optional[List[str]] = None
    brends: Optional[List[str]] = None
    category: Optional[List[str]] = None

@router.get("/dashboard/getfilter", response_model=FilterResponse)
async def get_filters(user_id: int = Query(..., description="ID пользователя")):
    """
    GET /api/dashboard/getfilter?user_id=3
    Возвращает уникальные значения для фильтров дашборда
    """
    try:
        if not user_id:
            raise ValueError(f"Пользователь не передан в get")
        result = get_filters_for_user(user_id)
        return result

    except Exception as e:
        logger.error(f"API error /api/dashboard/getfilter: {e}")
        raise HTTPException(status_code=500, detail=str(e))
@router.get(
    "/user_load_info",
    response_model=UserLoadInfoResponse,
    summary="Статистика загрузки по пользователям",
    tags=["Статистика"]
)
async def get_user_load_info():
    """Получить информацию о загрузке данных для всех пользователей."""
    try:
        users_info = get_users_load_info()
        total_records = sum(user["records_count"] for user in users_info)
        return UserLoadInfoResponse(
            success=True,
            data=[UserLoadInfo(**user) for user in users_info],
            total_users=len(users_info),
            total_records=total_records
        )
    except Exception as e:
        logger.error(f"API error /user_load_info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health", summary="Проверка здоровья", tags=["Система"])
async def health_check():
    """Проверка, что сервис работает"""
    return {"status": "ok", "service": "wb-sync"}


# ═══════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ — ДЕТАЛИЗАЦИЯ (по товарам)
# ═══════════════════════════════════════════════════════════════

def _build_metric_value(current: float, previous: float) -> dict:
    """Формирует MetricValue: value, change, changePercent."""
    change = current - previous
    if previous != 0:
        change_percent = round(((current - previous) / abs(previous)) * 100, 1)
    else:
        change_percent = 100.0 if current > 0 else 0.0
    return {
        "value": round(current, 2),
        "change": round(change, 2),
        "changePercent": change_percent,
    }


def _extract_product_info(
        nm_id: int,
        report_row: Dict[str, Any],
        funnel_row: Dict[str, Any],
) -> Dict[str, Any]:
    """Извлекает информацию о товаре из доступных источников."""
    logging.debug(f"[Details debug] ")
    return {
        "id": str(nm_id),
        "nmId": nm_id,
        "sa_name": report_row.get('sa_name') or funnel_row.get('vendor_code'),
        "barcode": report_row.get('barcode'),
        "productImageUrl": report_row.get('product_image_url') or funnel_row.get('product_image_url'),
        "productName": report_row.get('subject_name'),
        "brand": report_row.get('brand_name') or funnel_row.get('brand_name'),
        "category": report_row.get('subject_name'),
    }


def build_details(
        report_data: List[Dict[str, Any]],
        funnel_data: List[Dict[str, Any]],
        advert_data: List[Dict[str, Any]],
        compare_report_data: Optional[List[Dict[str, Any]]] = None,
        compare_funnel_data: Optional[List[Dict[str, Any]]] = None,
        compare_advert_data: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Формирует:
    {
        "details": [
            {
                "id": "1", "nmId": 123, "sa_name": "...",
                "net_profit": { "value": 250000, "change": 15000, "changePercent": 6 },
                ...
            },
        ]
    }
    """
    logger.debug(
        f"build_details: report={len(report_data or [])} products, "
        f"funnel={len(funnel_data or [])} products, "
        f"advert={len(advert_data or [])} products"
    )

    # ─── Индексируем по nm_id ───
    report_by_nm: Dict[int, Dict] = {}
    for row in (report_data or []):
        nm = row.get('nm_id')
        if nm is not None:
            report_by_nm[int(nm)] = row

    funnel_by_nm: Dict[int, Dict] = {}
    for row in (funnel_data or []):
        nm = row.get('nm_id')
        if nm is not None:
            funnel_by_nm[int(nm)] = row

    advert_by_nm: Dict[int, Dict] = {}
    for row in (advert_data or []):
        nm = row.get('nm_id')
        if nm is not None:
            advert_by_nm[int(nm)] = row

    # ─── Compare данные ───
    compare_report_by_nm: Dict[int, Dict] = {}
    for row in (compare_report_data or []):
        nm = row.get('nm_id')
        if nm is not None:
            compare_report_by_nm[int(nm)] = row

    compare_funnel_by_nm: Dict[int, Dict] = {}
    for row in (compare_funnel_data or []):
        nm = row.get('nm_id')
        if nm is not None:
            compare_funnel_by_nm[int(nm)] = row

    compare_advert_by_nm: Dict[int, Dict] = {}
    for row in (compare_advert_data or []):
        nm = row.get('nm_id')
        if nm is not None:
            compare_advert_by_nm[int(nm)] = row

    has_compare = compare_report_data is not None

    # ─── Все уникальные nm_id ───
    all_nm_ids = sorted(set(
        list(report_by_nm.keys())
        + list(funnel_by_nm.keys())
        + list(advert_by_nm.keys())
    ))

    if not all_nm_ids:
        return {"details": []}

    # ─── Собираем строки ───
    details: List[Dict[str, Any]] = []
    logger.debug(f"[Details debug] report_by_nm = {report_by_nm}")
    for nm_id in all_nm_ids:

        report_row = report_by_nm.get(nm_id, {})
        logger.debug(f"[Details debug] report_row = {report_row}")
        funnel_row = funnel_by_nm.get(nm_id, {})
        advert_row = advert_by_nm.get(nm_id, {})

        primary_metrics = _compute_day_metrics(report_row, funnel_row, advert_row)

        # Пропускаем товары, где все метрики = 0
        has_data = any(primary_metrics[key] != 0 for key in ALL_METRIC_KEYS)
        if not has_data:
            continue

        if has_compare:
            compare_metrics = _compute_day_metrics(
                compare_report_by_nm.get(nm_id, {}),
                compare_funnel_by_nm.get(nm_id, {}),
                compare_advert_by_nm.get(nm_id, {}),
            )
        else:
            compare_metrics = {key: 0.0 for key in ALL_METRIC_KEYS}

        row_data = _extract_product_info(nm_id, report_row, funnel_row)
        logger.debug(f"[Details debug] row_data = {row_data}")
        for key in ALL_METRIC_KEYS:
            row_data[key] = _build_metric_value(
                primary_metrics[key],
                compare_metrics[key],
            )

        details.append(row_data)

    return {"details": details}
