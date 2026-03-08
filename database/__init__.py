from .connection import get_connection, get_cursor, test_connection
from .init_schema import init_database
from .queries import (
    get_users_with_tokens,
    get_last_report_date,
    insert_financial_reports,
    cleanup_old_reports,
    get_users_load_info,
    # Dashboard
    get_metrics_for_period_from_report,
    get_metrics_for_period_from_funnel,
    get_details_by_product,
    # Воронка продаж
    get_last_funnel_date,
    insert_funnel_products,
    cleanup_old_funnel_data,
    get_funnel_stats_for_user,
    # Рекламная статистика
    insert_advert_stats,
    cleanup_old_advert_stats,
    get_advert_stats_for_user,
    get_metrics_for_period_from_advert_stats,
    # себестоимость
    load_nm_from_financial_reports_in_cost_price,
    get_cost_price,
    insert_cost_price,
    update_photos_in_cost_price,
    #динамика
    get_dynamic_for_period_from_report,
    get_dynamic_for_period_from_advert_stats,
get_dynamic_for_period_from_funnel,

#детализация
get_detail_for_period_from_report,
get_detail_for_period_from_advert_stats,
get_detail_for_period_from_funnel,

)

__all__ = [
    # Connection
    "get_connection",
    "get_cursor",
    "test_connection",
    "init_database",
    # Users & Reports
    "get_users_with_tokens",
    "get_last_report_date",
    "insert_financial_reports",
    "cleanup_old_reports",
    "get_users_load_info",
    # Dashboard
    "get_metrics_for_period_from_report",
    "get_metrics_for_period_from_funnel",
    "get_details_by_product",
    # Воронка продаж
    "get_last_funnel_date",
    "insert_funnel_products",
    "cleanup_old_funnel_data",
    "get_funnel_stats_for_user",
    # Рекламная статистика
    "insert_advert_stats",
    "cleanup_old_advert_stats",
    "get_advert_stats_for_user",
    "get_metrics_for_period_from_advert_stats",
    # себестоимость
    "load_nm_from_financial_reports_in_cost_price",
    "get_cost_price",
    "insert_cost_price",
    "update_photos_in_cost_price",
    # динамика
    "get_dynamic_for_period_from_report",
    "get_dynamic_for_period_from_advert_stats",
    "get_dynamic_for_period_from_funnel",
    #детализация
    "get_detail_for_period_from_report",
    "get_detail_for_period_from_advert_stats",
    "get_detail_for_period_from_funnel",
]