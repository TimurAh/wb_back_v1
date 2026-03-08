from .financial_report import FinancialReportRow, validate_wb_reports
from .metrics import MetricCard, MetricsCollection, MetricsApiResponse
from .funnel_product import FunnelProductRow, validate_funnel_products
from .cost_price import CostPriceRow, validate_cost_prices

# Новые модели для раздельных API-эндпоинтов дашборда
from .common import DateRange, DateRangeRequest
from .dynamics import DynamicsPoint, DynamicsSeries, DynamicsMetric, DynamicsApiResponse,DynamicsCollection
from .details import MetricValue, DetailRow, DetailsApiResponse
from .costprices import (
    CostPriceItem, CostPricesApiResponse,
    CostPriceSaveRequest, CostPriceSaveResponse,
)

__all__ = [
    # Financial Reports
    'FinancialReportRow',
    'validate_wb_reports',

    # Metrics
    'MetricCard',
    'MetricsCollection',
    'MetricsApiResponse',

    # Funnel Products
    'FunnelProductRow',
    'validate_funnel_products',

    # Cost Price (БД-модель)
    'CostPriceRow',
    'validate_cost_prices',

    # ===== Новые модели для API дашборда =====

    # Общие запросы
    'DateRange',
    'DateRangeRequest',

    # Динамика
    'DynamicsPoint',
    'DynamicsSeries',
    'DynamicsMetric',
    'DynamicsApiResponse',
    'DynamicsCollection',

    # Детализация
    'MetricValue',
    'DetailRow',
    'DetailsApiResponse',

    # Себестоимость (API-модели)
    'CostPriceItem',
    'CostPricesApiResponse',
    'CostPriceSaveRequest',
    'CostPriceSaveResponse',

    #Детализация.
    "DetailsApiResponse",
]