"""
SQL-запросы для работы с данными.

ЛОГИКА СИНХРОНИЗАЦИИ:
- financial_reports: Перезагружаем весь период, UPSERT по rrd_id
- funnel_product: Перезагружаем весь период, UPSERT по (user_id, nm_id, date_funnel)
"""
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional
from psycopg2.extras import execute_values
from database.connection import get_cursor
from config import config
from utils import logger
from models.financial_report import FinancialReportRow, validate_wb_reports
from models.costprices import CostPriceItem, CostPricesApiResponse
from crypto import decrypt_token
import json


# ═══════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════════════

def _build_filters(
        base_params: Dict[str, Any],
        brands: List[str] = None,
        categories: List[str] = None,
        sa_names: List[str] = None
) -> tuple:
    """
    Строит SQL условия для фильтров.
    """
    conditions = []
    params = base_params.copy()

    if brands and len(brands) > 0:
        conditions.append("brand_name = ANY(%(brands)s)")
        params["brands"] = brands

    if categories and len(categories) > 0:
        conditions.append("subject_name = ANY(%(categories)s)")
        params["categories"] = categories

    if sa_names and len(sa_names) > 0:
        conditions.append("sa_name = ANY(%(sa_names)s)")
        params["sa_names"] = sa_names

    sql = ""
    if conditions:
        # Добавляем перенос строки в конце!
        sql = "\n          AND " + "\n          AND ".join(conditions) + "\n"

    return sql, params


def _build_filters_funnel(
        base_params: Dict[str, Any],
        brands: List[str] = None,
        categories: List[str] = None,
        sa_names: List[str] = None
) -> tuple:
    """
    Строит SQL условия для фильтров (для funnel и advert).
    Использует 'category' вместо 'subject_name'.
    """
    conditions = []
    params = base_params.copy()

    if brands and len(brands) > 0:
        conditions.append("brand_name = ANY(%(brands)s)")
        params["brands"] = brands

    if categories and len(categories) > 0:
        conditions.append("category = ANY(%(categories)s)")
        params["categories"] = categories

    if sa_names and len(sa_names) > 0:
        conditions.append("sa_name = ANY(%(sa_names)s)")
        params["sa_names"] = sa_names

    sql = ""
    if conditions:
        # Добавляем перенос строки в конце!
        sql = "\n          AND " + "\n          AND ".join(conditions) + "\n"

    return sql, params


# ═══════════════════════════════════════════════════════════════
# ПОЛЬЗОВАТЕЛИ
# ═══════════════════════════════════════════════════════════════

def get_users_with_tokens() -> List[Dict[str, Any]]:
    """
    Получает всех пользователей с валидными wb_token
    """
    query = """
        SELECT user_id, username, wb_token
        FROM "user"
        WHERE wb_token IS NOT NULL
          AND wb_token != ''
    """

    with get_cursor() as cursor:
        cursor.execute(query)
        users = cursor.fetchall()

        decrypted_users = []
        for user in users:
            user_dict = dict(user)

            try:
                user_dict['wb_token'] = decrypt_token(user_dict['wb_token'])
                decrypted_users.append(user_dict)
            except Exception as e:
                logger.error(
                    f"Не удалось расшифровать токен для пользователя "
                    f"{user_dict['user_id']} ({user_dict['username']}): {e}"
                )
                continue

        logger.info(f"Найдено пользователей с валидными токенами: {len(decrypted_users)}")
        return decrypted_users


def get_last_report_date(user_id: int) -> Optional[date]:
    """
    Получает дату последнего отчёта для пользователя.
    """
    query = """
        SELECT MAX(date_to::date) as last_date
        FROM financial_reports
        WHERE user_id = %s
    """

    with get_cursor() as cursor:
        cursor.execute(query, (user_id,))
        result = cursor.fetchone()

        if result and result['last_date']:
            logger.debug(f"User {user_id}: последняя дата отчётов = {result['last_date']}")
            return result['last_date']

        logger.debug(f"User {user_id}: данных отчётов нет")
        return None


def get_users_load_info() -> List[Dict[str, Any]]:
    """
    Возвращает информацию о пользователях и количестве загруженных записей.
    """
    query = """
        SELECT 
            u.user_id,
            u.username,
            COALESCE(stats.records_count, 0) as records_count,
            stats.first_record_date,
            stats.last_record_date
        FROM "user" u
        LEFT JOIN (
            SELECT 
                user_id,
                COUNT(*) as records_count,
                MIN(date_from::date) as first_record_date,
                MAX(date_to::date) as last_record_date
            FROM financial_reports
            GROUP BY user_id
        ) stats ON u.user_id = stats.user_id
        WHERE u.wb_token IS NOT NULL
          AND u.wb_token != ''
        ORDER BY u.user_id
    """

    with get_cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()

        users_info = []
        for row in results:
            users_info.append({
                "user_id": row["user_id"],
                "username": row["username"],
                "records_count": row["records_count"],
                "first_record_date": (
                    row["first_record_date"].isoformat()
                    if row["first_record_date"] else None
                ),
                "last_record_date": (
                    row["last_record_date"].isoformat()
                    if row["last_record_date"] else None
                )
            })

        logger.info(f"Получена статистика для {len(users_info)} пользователей")
        return users_info


# ═══════════════════════════════════════════════════════════════
# ЗАПРОСЫ ДЛЯ DASHBOARD — REPORT (через VIEW)
# ═══════════════════════════════════════════════════════════════

def get_metrics_for_period_from_report(
    user_id: int,
    date_from: str,
    date_to: str,
    brands: List[str] = None,
    categories: List[str] = None,
    sa_names: List[str] = None
) -> Dict[str, Any]:
    """
    Рассчитывает агрегированные метрики за период из v_report_dashboard.
    """
    base_params = {
        "user_id": user_id,
        "date_from": date_from,
        "date_to": date_to
    }

    filter_sql, params = _build_filters(base_params, brands, categories, sa_names)

    query = f"""
        SELECT
            COALESCE(SUM(
                CASE WHEN doc_type_name = 'Продажа' 
                    THEN retail_price * COALESCE(quantity, 1) ELSE 0 END
            ), 0) as revenue,
            COALESCE(SUM(
                CASE WHEN doc_type_name = 'Возврат' 
                    THEN retail_price ELSE 0 END
            ), 0) as returns_sum,
            COALESCE(SUM(
                CASE WHEN bonus_type_name = 'От клиента при возврате' 
                    THEN COALESCE(return_amount, 1) ELSE 0 END
            ), 0) as returns_quantity,
            COALESCE(SUM(
                CASE WHEN bonus_type_name = 'От клиента при отмене' 
                    THEN ABS(retail_amount * COALESCE(return_amount, 1)) ELSE 0 END
            ), 0) as cancels_sum,
            COALESCE(SUM(
                CASE WHEN doc_type_name = 'Продажа' 
                    THEN retail_amount * COALESCE(quantity, 1)
                WHEN doc_type_name = 'Возврат' 
                    THEN -retail_amount * COALESCE(quantity, 1) ELSE 0 END
            ), 0) as sum_for_contribution,
            COALESCE(SUM(
                CASE WHEN bonus_type_name = 'От клиента при отмене' 
                    THEN COALESCE(return_amount, 1) ELSE 0 END
            ), 0) as cancels_quantity,
            COALESCE(SUM(
                CASE WHEN doc_type_name = 'Продажа' THEN ppvz_for_pay
                WHEN doc_type_name = 'Возврат' THEN -ppvz_for_pay ELSE 0 END
            ), 0) as ppvz_for_pay,
            COALESCE(AVG(NULLIF(commission_percent, 0)), 0) as commission,
            COALESCE(SUM(delivery_rub), 0) as logistics,
            COALESCE(SUM(penalty), 0) as penalties,
            COALESCE(SUM(storage_fee), 0) as storage,
            COALESCE(SUM(acceptance), 0) as acceptance,
            COALESCE(SUM(
                CASE WHEN supplier_oper_name = 'Продажа' 
                    THEN COALESCE(quantity, 1) ELSE 0 END
            ), 0) as sales_count,
            COALESCE(SUM(
                CASE WHEN doc_type_name = 'Продажа' 
                    THEN COALESCE(quantity, 1) * (c_price + fulfillment)
                WHEN doc_type_name = 'Возврат' 
                    THEN COALESCE(quantity, 1) * (c_price + fulfillment) * -1 ELSE 0 END
            ), 0) as sum_cost_price,
            COALESCE(SUM(
                CASE WHEN supplier_oper_name = 'Удержание' 
                    AND bonus_type_name NOT LIKE '%%WB Продвижение%%'
                    THEN COALESCE(deduction, 0) ELSE 0 END
            ), 0) as deduction
        FROM v_report_dashboard
        WHERE user_id = %(user_id)s
          AND date_to::date >= %(date_from)s::date
          AND date_to::date <= %(date_to)s::date
          {filter_sql}
    """

    with get_cursor() as cursor:
        cursor.execute(query, params)
        result = cursor.fetchone()
        return dict(result) if result else {}


def get_dynamic_for_period_from_report(
        user_id: int,
        date_from: str,
        date_to: str,
        brands: List[str] = None,
        categories: List[str] = None,
        sa_names: List[str] = None
) -> List[Dict[str, Any]]:
    """
    Рассчитывает данные для динамики за период из v_report_dashboard.
    """
    base_params = {
        "user_id": user_id,
        "date_from": date_from,
        "date_to": date_to
    }

    filter_sql, params = _build_filters(base_params, brands, categories, sa_names)
    query = f"""
        SELECT 
            date_to::date as date,
            COALESCE(SUM(
                CASE WHEN doc_type_name = 'Продажа' 
                    THEN retail_price * COALESCE(quantity, 1) ELSE 0 END
            ), 0) as revenue,
            COALESCE(SUM(
                CASE WHEN doc_type_name = 'Возврат' 
                    THEN retail_price ELSE 0 END
            ), 0) as returns_sum,
            COALESCE(SUM(
                CASE WHEN bonus_type_name = 'От клиента при возврате' 
                    THEN COALESCE(return_amount, 1) ELSE 0 END
            ), 0) as returns_quantity,
            COALESCE(SUM(
                CASE WHEN bonus_type_name = 'От клиента при отмене' 
                    THEN ABS(retail_amount * COALESCE(return_amount, 1)) ELSE 0 END
            ), 0) as cancels_sum,
            COALESCE(SUM(
                CASE WHEN doc_type_name = 'Продажа' 
                    THEN retail_amount * COALESCE(quantity, 1)
                WHEN doc_type_name = 'Возврат' 
                    THEN -retail_amount * COALESCE(quantity, 1) ELSE 0 END
            ), 0) as sum_for_contribution,
            COALESCE(SUM(
                CASE WHEN bonus_type_name = 'От клиента при отмене' 
                    THEN COALESCE(return_amount, 1) ELSE 0 END
            ), 0) as cancels_quantity,
            COALESCE(SUM(
                CASE WHEN doc_type_name = 'Продажа' THEN ppvz_for_pay
                WHEN doc_type_name = 'Возврат' THEN -ppvz_for_pay ELSE 0 END
            ), 0) as ppvz_for_pay,
            COALESCE(AVG(NULLIF(commission_percent, 0)), 0) as commission,
            COALESCE(SUM(delivery_rub), 0) as logistics,
            COALESCE(SUM(penalty), 0) as penalties,
            COALESCE(SUM(storage_fee), 0) as storage,
            COALESCE(SUM(acceptance), 0) as acceptance,
            COALESCE(SUM(
                CASE WHEN supplier_oper_name = 'Продажа' 
                    THEN COALESCE(quantity, 1) ELSE 0 END
            ), 0) as sales_count,
            COALESCE(SUM(
                CASE WHEN doc_type_name = 'Продажа' 
                    THEN COALESCE(quantity, 1) * (c_price + fulfillment)
                WHEN doc_type_name = 'Возврат' 
                    THEN COALESCE(quantity, 1) * (c_price + fulfillment) * -1 ELSE 0 END
            ), 0) as sum_cost_price,
            COALESCE(SUM(
                CASE WHEN supplier_oper_name = 'Удержание' 
                    AND bonus_type_name NOT LIKE '%%WB Продвижение%%'
                    THEN COALESCE(deduction, 0) ELSE 0 END
            ), 0) as deduction
        FROM v_report_dashboard
        WHERE user_id = %(user_id)s
          AND date_to::date >= %(date_from)s::date
          AND date_to::date <= %(date_to)s::date
        {filter_sql}
        GROUP BY date_to::date
        ORDER BY date_to::date ASC
    """

    with get_cursor() as cursor:
        cursor.execute(query, params)
        result = cursor.fetchall()
        return [dict(row) for row in result] if result else []


def get_detail_for_period_from_report(
    user_id: int,
    date_from: str,
    date_to: str,
    brands: List[str] = None,
    categories: List[str] = None,
    sa_names: List[str] = None
) -> List[Dict[str, Any]]:
    """
    Рассчитывает данные детализации за период из v_report_dashboard.
    """
    base_params = {
        "user_id": user_id,
        "date_from": date_from,
        "date_to": date_to
    }

    filter_sql, params = _build_filters(base_params, brands, categories, sa_names)

    query = f"""
        SELECT 
            COALESCE(SUM(
                CASE WHEN doc_type_name = 'Продажа' 
                    THEN retail_price * COALESCE(quantity, 1) ELSE 0 END
            ), 0) as revenue,
            COALESCE(SUM(
                CASE WHEN doc_type_name = 'Возврат' 
                    THEN retail_price ELSE 0 END
            ), 0) as returns_sum,
            COALESCE(SUM(
                CASE WHEN bonus_type_name = 'От клиента при возврате' 
                    THEN COALESCE(return_amount, 1) ELSE 0 END
            ), 0) as returns_quantity,
            COALESCE(SUM(
                CASE WHEN bonus_type_name = 'От клиента при отмене' 
                    THEN ABS(retail_amount * COALESCE(return_amount, 1)) ELSE 0 END
            ), 0) as cancels_sum,
            COALESCE(SUM(
                CASE WHEN doc_type_name = 'Продажа' 
                    THEN retail_amount * COALESCE(quantity, 1)
                WHEN doc_type_name = 'Возврат' 
                    THEN -retail_amount * COALESCE(quantity, 1) ELSE 0 END
            ), 0) as sum_for_contribution,
            COALESCE(SUM(
                CASE WHEN bonus_type_name = 'От клиента при отмене' 
                    THEN COALESCE(return_amount, 1) ELSE 0 END
            ), 0) as cancels_quantity,
            COALESCE(SUM(
                CASE WHEN doc_type_name = 'Продажа' THEN ppvz_for_pay
                WHEN doc_type_name = 'Возврат' THEN -ppvz_for_pay ELSE 0 END
            ), 0) as ppvz_for_pay,
            COALESCE(AVG(NULLIF(commission_percent, 0)), 0) as commission,
            COALESCE(SUM(delivery_rub), 0) as logistics,
            COALESCE(SUM(penalty), 0) as penalties,
            COALESCE(SUM(storage_fee), 0) as storage,
            COALESCE(SUM(acceptance), 0) as acceptance,
            COALESCE(SUM(
                CASE WHEN supplier_oper_name = 'Продажа' 
                    THEN COALESCE(quantity, 1) ELSE 0 END
            ), 0) as sales_count,
            COALESCE(SUM(
                CASE WHEN doc_type_name = 'Продажа' 
                    THEN COALESCE(quantity, 1) * (c_price + fulfillment)
                WHEN doc_type_name = 'Возврат' 
                    THEN COALESCE(quantity, 1) * (c_price + fulfillment) * -1 ELSE 0 END
            ), 0) as sum_cost_price,
            COALESCE(SUM(
                CASE WHEN supplier_oper_name = 'Удержание' 
                    AND bonus_type_name NOT LIKE '%%WB Продвижение%%'
                    THEN COALESCE(deduction, 0) ELSE 0 END
            ), 0) as deduction,
            nm_id,
            MAX(product_image_url) as product_image_url,
            MAX(COALESCE(cp_sa_name, sa_name)) as sa_name
        FROM v_report_dashboard
        WHERE user_id = %(user_id)s
          AND date_to::date >= %(date_from)s::date
          AND date_to::date <= %(date_to)s::date
          AND nm_id > 0
          {filter_sql}
        GROUP BY nm_id 
        ORDER BY nm_id ASC
    """

    with get_cursor() as cursor:
        cursor.execute(query, params)
        result = cursor.fetchall()
        return [dict(row) for row in result] if result else []


def get_filters_for_user(user_id: int) -> Dict[str, Any]:
    """
    Получает уникальные значения для фильтров дашборда из v_report_dashboard.
    """
    query = """
        SELECT
            array_agg(DISTINCT sa_name) FILTER (WHERE sa_name IS NOT NULL AND sa_name != '') AS sa_name,
            array_agg(DISTINCT brand_name) FILTER (WHERE brand_name IS NOT NULL AND brand_name != '') AS brends,
            array_agg(DISTINCT subject_name) FILTER (WHERE subject_name IS NOT NULL AND subject_name != '') AS category
        FROM v_report_dashboard
        WHERE user_id = %s
    """

    with get_cursor() as cursor:
        cursor.execute(query, (user_id,))
        result = cursor.fetchone()

        if result:
            return {
                "sa_name": result["sa_name"],
                "brends": result["brends"],
                "category": result["category"],
            }

        return {
            "sa_name": None,
            "brends": None,
            "category": None,
        }


def get_details_by_product(
    user_id: int,
    date_from: str,
    date_to: str,
    brands: List[str] = None,
    categories: List[str] = None,
    sa_names: List[str] = None
) -> List[Dict[str, Any]]:
    """
    Получает детализацию по товарам (артикулам).
    """
    base_params = {
        "user_id": user_id,
        "date_from": date_from,
        "date_to": date_to
    }

    filter_sql, params = _build_filters(base_params, brands, categories, sa_names)

    query = f"""
        SELECT 
            nm_id,
            MAX(sa_name) as product_name,
            MAX(brand_name) as brand_name,
            COALESCE(SUM(ppvz_for_pay), 0) as net_profit_rub,
            COALESCE(SUM(
                CASE 
                    WHEN supplier_oper_name = 'Продажа' 
                        THEN retail_price * COALESCE(quantity, 1) 
                    ELSE 0 
                END
            ), 0) as sales_rub,
            COALESCE(SUM(
                CASE 
                    WHEN supplier_oper_name = 'Продажа' 
                        THEN retail_amount 
                    ELSE 0 
                END
            ), 0) as orders_rub,
            COALESCE(SUM(delivery_rub), 0) as logistics_total,
            COALESCE(SUM(
                CASE 
                    WHEN supplier_oper_name = 'Продажа' 
                        THEN COALESCE(quantity, 1) 
                    ELSE 0 
                END
            ), 0) as sales_count,
            COALESCE(SUM(
                CASE 
                    WHEN supplier_oper_name = 'Возврат' 
                        THEN COALESCE(quantity, 1) 
                    ELSE 0 
                END
            ), 0) as returns_count
        FROM v_report_dashboard
        WHERE user_id = %(user_id)s
          AND date_to::date >= %(date_from)s::date
          AND date_to::date <= %(date_to)s::date
          AND nm_id IS NOT NULL
          {filter_sql}
        GROUP BY nm_id
        ORDER BY SUM(ppvz_for_pay) DESC NULLS LAST
        LIMIT 50
    """

    with get_cursor() as cursor:
        cursor.execute(query, params)
        results = cursor.fetchall()

        details = []
        for i, row in enumerate(results):
            sales_count = int(row['sales_count'] or 1)
            net_profit = float(row['net_profit_rub'] or 0)
            sales_rub = float(row['sales_rub'] or 0)
            orders_rub = float(row['orders_rub'] or 0)
            logistics_total = float(row['logistics_total'] or 0)

            logistics_per_unit = logistics_total / sales_count if sales_count > 0 else 0
            margin_percent = (net_profit / sales_rub * 100) if sales_rub > 0 else 0
            roi_percent = (net_profit / (sales_rub - net_profit) * 100) if (sales_rub - net_profit) > 0 else 0

            details.append({
                'id': str(i + 1),
                'productImageUrl': '',
                'productName': row['product_name'] or f"Артикул {row['nm_id']}",
                'nmId': row['nm_id'],
                'brandName': row['brand_name'],
                'netProfitRub': round(net_profit, 2),
                'netProfitChange': 0,
                'netProfitChangePercent': 0,
                'marginPercent': round(margin_percent, 1),
                'marginChange': 0,
                'marginChangePercent': 0,
                'roiPercent': round(roi_percent, 1),
                'roiChange': 0,
                'roiChangePercent': 0,
                'ordersRub': round(orders_rub, 2),
                'ordersChange': 0,
                'ordersChangePercent': 0,
                'salesRub': round(sales_rub, 2),
                'salesChange': 0,
                'salesChangePercent': 0,
                'logisticsPerUnitRub': round(logistics_per_unit, 2),
                'logisticsChange': 0,
                'logisticsChangePercent': 0,
                'crOrderPercent': 0,
                'crOrderChange': 0,
                'crOrderChangePercent': 0,
                'crCartPercent': 0,
                'crCartChange': 0,
                'crCartChangePercent': 0,
            })

        return details


# ═══════════════════════════════════════════════════════════════
# ЗАПРОСЫ ДЛЯ DASHBOARD — FUNNEL (через VIEW)
# ═══════════════════════════════════════════════════════════════

def get_metrics_for_period_from_funnel(
    user_id: int,
    date_from: str,
    date_to: str,
    brands: List[str] = None,
    categories: List[str] = None,
    sa_names: List[str] = None
) -> Dict[str, Any]:
    """
    Рассчитывает агрегированные метрики за период из v_funnel_dashboard.
    """
    base_params = {
        "user_id": user_id,
        "date_from": date_from,
        "date_to": date_to
    }

    filter_sql, params = _build_filters_funnel(base_params, brands, categories, sa_names)

    query = f"""
        WITH daily AS (
            SELECT 
                date_funnel,
                SUM(order_sum) AS order_sum,
                SUM(order_count) AS order_count,
                SUM(stocks_balance) AS stocks_balance_sum,
                SUM(open_count) AS open_count,
                SUM(cart_count) AS cart_count,
                SUM(cancel_sum) AS cancel_sum,
                SUM(cancel_count) AS cancel_count,
                AVG(NULLIF(conversions_buyout_percent, 0)) AS conversions_buyout_percent
            FROM v_funnel_dashboard
            WHERE user_id = %(user_id)s
              AND date_funnel >= %(date_from)s::date
              AND date_funnel <= %(date_to)s::date
              {filter_sql}
            GROUP BY date_funnel
        )
        SELECT 
            COALESCE(SUM(order_sum), 0) AS order_sum,
            COALESCE(SUM(order_count), 0) AS order_count,
            (SELECT MAX(stocks_balance_sum) FROM daily) AS stocks_balance_sum,
            COALESCE(SUM(open_count), 0) AS open_count,
            COALESCE(SUM(cancel_sum), 0) AS cancel_sum,
            COALESCE(SUM(cancel_count), 0) AS cancel_count,
            CASE 
                WHEN SUM(open_count) > 0 
                THEN SUM(cart_count) * 100.0 / SUM(open_count) 
                ELSE 0 
            END AS conversions_add_to_cart_percent,
            CASE 
                WHEN SUM(cart_count) > 0 
                THEN SUM(order_count) * 100.0 / SUM(cart_count) 
                ELSE 0 
            END AS conversions_cart_to_order_percent,
            AVG(NULLIF(conversions_buyout_percent, 0)) AS conversions_buyout_percent
        FROM v_funnel_dashboard
        WHERE user_id = %(user_id)s
          AND date_funnel >= %(date_from)s::date
          AND date_funnel <= %(date_to)s::date
          {filter_sql}
    """

    with get_cursor() as cursor:
        cursor.execute(query, params)
        result = cursor.fetchone()
        return dict(result) if result else {}


def get_dynamic_for_period_from_funnel(
    user_id: int,
    date_from: str,
    date_to: str,
    brands: List[str] = None,
    categories: List[str] = None,
    sa_names: List[str] = None
) -> List[Dict[str, Any]]:
    """
    Рассчитывает метрики из v_funnel_dashboard с группировкой по дням.
    """
    base_params = {
        "user_id": user_id,
        "date_from": date_from,
        "date_to": date_to
    }

    filter_sql, params = _build_filters_funnel(base_params, brands, categories, sa_names)

    query = f"""
        SELECT 
            date_funnel AS date,
            COALESCE(SUM(order_sum), 0) AS order_sum,
            COALESCE(SUM(order_count), 0) AS order_count,
            COALESCE(SUM(stocks_balance), 0) AS stocks_balance_sum,
            COALESCE(SUM(open_count), 0) AS open_count,
            COALESCE(SUM(cart_count), 0) AS cart_count,
            COALESCE(SUM(cancel_sum), 0) AS cancel_sum,
            COALESCE(SUM(cancel_count), 0) AS cancel_count,
            CASE 
                WHEN SUM(open_count) > 0 
                THEN SUM(cart_count) * 100.0 / SUM(open_count) 
                ELSE 0 
            END AS conversions_add_to_cart_percent,
            CASE 
                WHEN SUM(cart_count) > 0 
                THEN SUM(order_count) * 100.0 / SUM(cart_count) 
                ELSE 0 
            END AS conversions_cart_to_order_percent,
            AVG(NULLIF(conversions_buyout_percent, 0)) AS conversions_buyout_percent
        FROM v_funnel_dashboard
        WHERE user_id = %(user_id)s
          AND date_funnel >= %(date_from)s::date
          AND date_funnel <= %(date_to)s::date
          {filter_sql}
        GROUP BY date_funnel
        ORDER BY date_funnel
    """

    with get_cursor() as cursor:
        cursor.execute(query, params)
        result = cursor.fetchall()
        return [dict(row) for row in result] if result else []


def get_detail_for_period_from_funnel(
    user_id: int,
    date_from: str,
    date_to: str,
    brands: List[str] = None,
    categories: List[str] = None,
    sa_names: List[str] = None
) -> List[Dict[str, Any]]:
    """
    Рассчитывает метрики из v_funnel_dashboard с группировкой по nm_id.
    """
    base_params = {
        "user_id": user_id,
        "date_from": date_from,
        "date_to": date_to
    }

    filter_sql, params = _build_filters_funnel(base_params, brands, categories, sa_names)

    query = f"""
        SELECT 
            nm_id,
            COALESCE(SUM(order_sum), 0) AS order_sum,
            COALESCE(SUM(order_count), 0) AS order_count,
            COALESCE(SUM(stocks_balance), 0) AS stocks_balance_sum,
            COALESCE(SUM(open_count), 0) AS open_count,
            COALESCE(SUM(cart_count), 0) AS cart_count,
            COALESCE(SUM(cancel_sum), 0) AS cancel_sum,
            COALESCE(SUM(cancel_count), 0) AS cancel_count,
            CASE 
                WHEN SUM(open_count) > 0 
                THEN SUM(cart_count) * 100.0 / SUM(open_count) 
                ELSE 0 
            END AS conversions_add_to_cart_percent,
            CASE 
                WHEN SUM(cart_count) > 0 
                THEN SUM(order_count) * 100.0 / SUM(cart_count) 
                ELSE 0 
            END AS conversions_cart_to_order_percent,
            AVG(NULLIF(conversions_buyout_percent, 0)) AS conversions_buyout_percent,
            MAX(vendor_code) AS vendor_code,
            MAX(product_image_url) AS product_image_url,
            MAX(sa_name) AS sa_name,
            MAX(category) AS category
        FROM v_funnel_dashboard
        WHERE user_id = %(user_id)s
          AND date_funnel >= %(date_from)s::date
          AND date_funnel <= %(date_to)s::date
          {filter_sql}
        GROUP BY nm_id
        ORDER BY nm_id
    """

    with get_cursor() as cursor:
        cursor.execute(query, params)
        result = cursor.fetchall()
        return [dict(row) for row in result] if result else []


# ═══════════════════════════════════════════════════════════════
# ЗАПРОСЫ ДЛЯ DASHBOARD — ADVERT (через VIEW)
# ═══════════════════════════════════════════════════════════════

def get_metrics_for_period_from_advert_stats(
    user_id: int,
    date_from: str,
    date_to: str,
    brands: List[str] = None,
    categories: List[str] = None,
    sa_names: List[str] = None
) -> Dict[str, Any]:
    """
    Рассчитывает агрегированные метрики за период из v_advert_dashboard.
    """
    base_params = {
        "user_id": user_id,
        "date_from": date_from,
        "date_to": date_to
    }

    filter_sql, params = _build_filters_funnel(base_params, brands, categories, sa_names)

    query = f"""
        SELECT 
            COALESCE(SUM(ad_expense), 0) AS ad_expense
        FROM v_advert_dashboard
        WHERE user_id = %(user_id)s
          AND date_stat >= %(date_from)s::date
          AND date_stat <= %(date_to)s::date
          {filter_sql}
    """

    with get_cursor() as cursor:
        cursor.execute(query, params)
        result = cursor.fetchone()
        return dict(result) if result else {"ad_expense": 0}


def get_dynamic_for_period_from_advert_stats(
    user_id: int,
    date_from: str,
    date_to: str,
    brands: List[str] = None,
    categories: List[str] = None,
    sa_names: List[str] = None
) -> List[Dict[str, Any]]:
    """
    Рассчитывает данные для динамики за период из v_advert_dashboard.
    """
    base_params = {
        "user_id": user_id,
        "date_from": date_from,
        "date_to": date_to
    }

    filter_sql, params = _build_filters_funnel(base_params, brands, categories, sa_names)

    query = f"""
        SELECT 
            date_stat AS date,
            COALESCE(SUM(ad_expense), 0) AS ad_expense
        FROM v_advert_dashboard
        WHERE user_id = %(user_id)s
          AND date_stat >= %(date_from)s::date
          AND date_stat <= %(date_to)s::date
          {filter_sql}
        GROUP BY date_stat
        ORDER BY date_stat ASC
    """

    with get_cursor() as cursor:
        cursor.execute(query, params)
        result = cursor.fetchall()
        return [dict(row) for row in result] if result else []


def get_detail_for_period_from_advert_stats(
    user_id: int,
    date_from: str,
    date_to: str,
    brands: List[str] = None,
    categories: List[str] = None,
    sa_names: List[str] = None
) -> List[Dict[str, Any]]:
    """
    Рассчитывает данные детализации за период из v_advert_dashboard.
    """
    base_params = {
        "user_id": user_id,
        "date_from": date_from,
        "date_to": date_to
    }

    filter_sql, params = _build_filters_funnel(base_params, brands, categories, sa_names)

    query = f"""
        SELECT 
            nm_id,
            COALESCE(SUM(ad_expense), 0) AS ad_expense,
            MAX(product_image_url) AS product_image_url,
            MAX(sa_name) AS sa_name,
            MAX(category) AS category,
            MAX(brand_name) AS brand_name
        FROM v_advert_dashboard
        WHERE user_id = %(user_id)s
          AND date_stat >= %(date_from)s::date
          AND date_stat <= %(date_to)s::date
          {filter_sql}
        GROUP BY nm_id
        ORDER BY nm_id ASC
    """

    with get_cursor() as cursor:
        cursor.execute(query, params)
        result = cursor.fetchall()
        return [dict(row) for row in result] if result else []


# ═══════════════════════════════════════════════════════════════
# ФИНАНСОВЫЕ ОТЧЁТЫ — ПОЛНАЯ СИНХРОНИЗАЦИЯ С UPSERT
# ═══════════════════════════════════════════════════════════════

def insert_financial_reports(user_id: int, reports: List[Dict[str, Any]]) -> int:
    """
    Вставляет или обновляет финансовые отчёты в БД.
    """
    if not reports:
        return 0

    validated_reports = validate_wb_reports(reports)

    if not validated_reports:
        logger.warning(f"User {user_id}: все записи отклонены при валидации")
        return 0

    logger.info(
        f"User {user_id}: валидировано {len(validated_reports)} из {len(reports)} записей"
    )

    del reports

    fields = [
        "rrd_id", "user_id", "realizationreport_id", "date_from", "date_to", "create_dt",
        "currency_name", "suppliercontract_code", "gi_id", "dlv_prc",
        "fix_tariff_date_from", "fix_tariff_date_to", "subject_name", "nm_id",
        "brand_name", "sa_name", "ts_name", "barcode", "doc_type_name",
        "quantity", "retail_price", "retail_amount", "sale_percent",
        "commission_percent", "office_name", "supplier_oper_name", "order_dt",
        "sale_dt", "rr_dt", "shk_id", "retail_price_withdisc_rub",
        "delivery_amount", "return_amount", "delivery_rub", "gi_box_type_name",
        "product_discount_for_report", "supplier_promo", "rid", "ppvz_spp_prc",
        "ppvz_kvw_prc_base", "ppvz_kvw_prc", "sup_rating_prc_up", "is_kgvp_v2",
        "ppvz_sales_commission", "ppvz_for_pay", "ppvz_reward", "acquiring_fee",
        "acquiring_percent", "payment_processing", "acquiring_bank", "ppvz_vw",
        "ppvz_vw_nds", "ppvz_office_name", "ppvz_office_id", "ppvz_supplier_id",
        "ppvz_supplier_name", "ppvz_inn", "declaration_number", "bonus_type_name",
        "sticker_id", "site_country", "srv_dbs", "penalty", "additional_payment",
        "rebill_logistic_cost", "rebill_logistic_org", "storage_fee", "deduction",
        "acceptance", "assembly_id", "kiz", "srid", "report_type", "is_legal_entity",
        "trbx_id", "installment_cofinancing_amount", "wibes_wb_discount_percent",
        "cashback_amount", "cashback_discount", "cashback_commission_change", "order_uid"
    ]

    fields_str = ", ".join(fields)
    update_fields = [f for f in fields if f != "rrd_id"]
    update_str = ", ".join([f"{f} = EXCLUDED.{f}" for f in update_fields])

    query = f"""
        INSERT INTO financial_reports ({fields_str})
        VALUES %s
        ON CONFLICT (rrd_id) 
        DO UPDATE SET {update_str}
    """

    def prepare_row(row_dict: dict) -> tuple:
        values = []
        for field in fields:
            value = row_dict.get(field)
            if field == "suppliercontract_code" and value is not None:
                if not isinstance(value, str):
                    value = json.dumps(value)
            values.append(value)
        return tuple(values)

    BATCH_SIZE = 5000
    total_affected = 0
    skipped_total = 0

    with get_cursor(commit=True) as cursor:
        for i in range(0, len(validated_reports), BATCH_SIZE):
            batch = validated_reports[i:i + BATCH_SIZE]

            db_rows = [report.to_db_dict(user_id) for report in batch]
            values = [prepare_row(row) for row in db_rows]

            valid_values = [v for v in values if v[0] is not None]
            skipped_total += len(values) - len(valid_values)

            if valid_values:
                try:
                    execute_values(cursor, query, valid_values, page_size=1000)
                    total_affected += cursor.rowcount
                except Exception as e:
                    logger.error(f"User {user_id}: ошибка batch {i // BATCH_SIZE + 1}: {e}")
                    raise

            del batch, db_rows, values, valid_values

        del validated_reports

    if skipped_total > 0:
        logger.warning(f"User {user_id}: пропущено {skipped_total} записей без rrd_id")

    logger.info(f"User {user_id}: отчёты — вставлено/обновлено {total_affected} записей")
    return total_affected


def cleanup_old_reports(months: int = None) -> int:
    """
    Удаляет записи старше указанного количества месяцев.
    """
    if months is None:
        months = config.DATA_RETENTION_MONTHS

    cutoff_date = date.today() - timedelta(days=months * 30)

    query = """
        DELETE FROM financial_reports
        WHERE date_to::date < %s
    """

    with get_cursor(commit=True) as cursor:
        cursor.execute(query, (cutoff_date,))
        deleted_count = cursor.rowcount

        if deleted_count > 0:
            logger.info(
                f"Удалено старых записей отчётов: {deleted_count} (старше {cutoff_date})"
            )

        return deleted_count


# ═══════════════════════════════════════════════════════════════
# ВОРОНКА ПРОДАЖ — ПОЛНАЯ СИНХРОНИЗАЦИЯ С UPSERT
# ═══════════════════════════════════════════════════════════════

def get_last_funnel_date(user_id: int) -> Optional[date]:
    """
    Получает дату последней записи воронки для пользователя.
    """
    query = """
        SELECT MAX(date_funnel) as last_date
        FROM funnel_product
        WHERE user_id = %s
    """

    with get_cursor() as cursor:
        cursor.execute(query, (user_id,))
        result = cursor.fetchone()

        if result and result['last_date']:
            logger.debug(f"User {user_id}: последняя дата воронки = {result['last_date']}")
            return result['last_date']

        logger.debug(f"User {user_id}: данных воронки нет")
        return None


def insert_funnel_products(
    user_id: int,
    products: List[Dict[str, Any]],
    extract_both_periods: bool = True
) -> int:
    """
    Вставляет или обновляет данные воронки продаж в БД.
    """
    from models.funnel_product import (
        validate_funnel_products,
        extract_both_periods as extract_both
    )

    if not products:
        return 0

    if extract_both_periods:
        validated_products = extract_both(products)
        logger.info(
            f"User {user_id}: валидировано воронки {len(validated_products)} записей "
            f"(selected + past из {len(products)} товаров)"
        )
    else:
        validated_products = validate_funnel_products(products, period_type='selected')
        logger.info(
            f"User {user_id}: валидировано воронки {len(validated_products)} "
            f"из {len(products)} записей"
        )

    if not validated_products:
        logger.warning(f"User {user_id}: все записи воронки отклонены при валидации")
        return 0

    del products

    fields = [
        "user_id", "nm_id", "vendor_code", "brand_name",
        "stocks_wb", "stocks_mp", "stocks_balance_sum",
        "date_funnel", "open_count", "cart_count",
        "order_count", "order_sum", "cancel_count", "cancel_sum",
        "avg_price", "time_to_ready_days", "time_to_ready_hours",
        "time_to_ready_mins", "localization_percent",
        "conversions_add_to_cart_percent", "conversions_cart_to_order_percent",
        "conversions_buyout_percent"
    ]

    fields_str = ", ".join(fields)
    update_fields = [f for f in fields if f not in ("user_id", "nm_id", "date_funnel")]
    update_str = ", ".join([f"{f} = EXCLUDED.{f}" for f in update_fields])

    query = f"""
        INSERT INTO funnel_product ({fields_str})
        VALUES %s
        ON CONFLICT (user_id, nm_id, date_funnel) 
        DO UPDATE SET {update_str}
    """

    def prepare_row(row_dict: dict) -> tuple:
        return tuple(row_dict.get(field) for field in fields)

    BATCH_SIZE = 5000
    total_affected = 0

    with get_cursor(commit=True) as cursor:
        for i in range(0, len(validated_products), BATCH_SIZE):
            batch = validated_products[i:i + BATCH_SIZE]

            db_rows = [product.to_db_dict(user_id) for product in batch]
            values = [prepare_row(row) for row in db_rows]

            if values:
                try:
                    execute_values(cursor, query, values, page_size=1000)
                    total_affected += cursor.rowcount
                except Exception as e:
                    logger.error(f"User {user_id}: ошибка batch воронки {i // BATCH_SIZE + 1}: {e}")
                    raise

            del batch, db_rows, values

        del validated_products

    logger.info(f"User {user_id}: воронка — вставлено/обновлено {total_affected} записей")
    return total_affected


def cleanup_old_funnel_data(months: int = None) -> int:
    """
    Удаляет старые записи воронки.
    """
    if months is None:
        months = config.DATA_RETENTION_MONTHS

    cutoff_date = date.today() - timedelta(days=months * 30)

    query = """
        DELETE FROM funnel_product
        WHERE date_funnel < %s
    """

    with get_cursor(commit=True) as cursor:
        cursor.execute(query, (cutoff_date,))
        deleted_count = cursor.rowcount

        if deleted_count > 0:
            logger.info(
                f"Удалено старых записей воронки: {deleted_count} (старше {cutoff_date})"
            )

        return deleted_count


def get_funnel_stats_for_user(user_id: int) -> Dict[str, Any]:
    """
    Возвращает статистику воронки для пользователя.
    """
    query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT nm_id) as unique_products,
            MIN(date_funnel) as first_date,
            MAX(date_funnel) as last_date,
            COUNT(DISTINCT date_funnel) as days_count
        FROM funnel_product
        WHERE user_id = %s
    """

    with get_cursor() as cursor:
        cursor.execute(query, (user_id,))
        result = cursor.fetchone()

        if result:
            return {
                "total_records": result["total_records"],
                "unique_products": result["unique_products"],
                "first_date": (
                    result["first_date"].isoformat()
                    if result["first_date"] else None
                ),
                "last_date": (
                    result["last_date"].isoformat()
                    if result["last_date"] else None
                ),
                "days_count": result["days_count"]
            }

        return {
            "total_records": 0,
            "unique_products": 0,
            "first_date": None,
            "last_date": None,
            "days_count": 0
        }


# ═══════════════════════════════════════════════════════════════
# РЕКЛАМНАЯ СТАТИСТИКА — ПОЛНАЯ СИНХРОНИЗАЦИЯ С UPSERT
# ═══════════════════════════════════════════════════════════════

def insert_advert_stats(
    user_id: int,
    stats: List[Any]
) -> int:
    """
    Вставляет или обновляет статистику рекламных кампаний.
    """
    if not stats:
        return 0

    db_rows = [stat.to_db_dict(user_id) for stat in stats]

    fields = [
        "user_id", "advert_id", "date_stat", "app_type", "nm_id", "sum"
    ]

    def prepare_row(row_dict: dict) -> tuple:
        return tuple(row_dict.get(field) for field in fields)

    values = [prepare_row(row) for row in db_rows]

    fields_str = ", ".join(fields)

    query = f"""
        INSERT INTO advert_fullstats ({fields_str}, updated_at)
        VALUES %s
        ON CONFLICT (user_id, advert_id, date_stat, app_type, nm_id) 
        DO UPDATE SET 
            sum = EXCLUDED.sum,
            updated_at = NOW()
    """

    values_with_timestamp = [v + (datetime.now(),) for v in values]

    with get_cursor(commit=True) as cursor:
        try:
            execute_values(
                cursor,
                query,
                values_with_timestamp,
                template="(%s, %s, %s, %s, %s, %s, %s)",
                page_size=1000
            )
            affected_count = cursor.rowcount
            logger.info(
                f"User {user_id}: advert stats — вставлено/обновлено {affected_count} записей"
            )
            return affected_count
        except Exception as e:
            logger.error(f"User {user_id}: ошибка вставки advert stats: {e}")
            raise


def cleanup_old_advert_stats(months: int = None) -> int:
    """
    Удаляет старые записи статистики рекламы.
    """
    if months is None:
        months = config.DATA_RETENTION_MONTHS

    cutoff_date = date.today() - timedelta(days=months * 30)

    query = """
        DELETE FROM advert_fullstats
        WHERE date_stat < %s
    """

    with get_cursor(commit=True) as cursor:
        cursor.execute(query, (cutoff_date,))
        deleted_count = cursor.rowcount

        if deleted_count > 0:
            logger.info(
                f"Удалено старых записей advert stats: {deleted_count} "
                f"(старше {cutoff_date})"
            )

        return deleted_count


def get_advert_stats_for_user(user_id: int) -> Dict[str, Any]:
    """
    Возвращает статистику по рекламе для пользователя.
    """
    query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT advert_id) as unique_adverts,
            COUNT(DISTINCT nm_id) as unique_products,
            MIN(date_stat) as first_date,
            MAX(date_stat) as last_date,
            COUNT(DISTINCT date_stat) as days_count,
            COALESCE(SUM(sum), 0) as total_sum
        FROM advert_fullstats
        WHERE user_id = %s
    """

    with get_cursor() as cursor:
        cursor.execute(query, (user_id,))
        result = cursor.fetchone()

        if result:
            return {
                "total_records": result["total_records"],
                "unique_adverts": result["unique_adverts"],
                "unique_products": result["unique_products"],
                "first_date": (
                    result["first_date"].isoformat()
                    if result["first_date"] else None
                ),
                "last_date": (
                    result["last_date"].isoformat()
                    if result["last_date"] else None
                ),
                "days_count": result["days_count"],
                "total_sum": float(result["total_sum"])
            }

        return {
            "total_records": 0,
            "unique_adverts": 0,
            "unique_products": 0,
            "first_date": None,
            "last_date": None,
            "days_count": 0,
            "total_sum": 0.0
        }


# ═══════════════════════════════════════════════════════════════
# COST PRICE
# ═══════════════════════════════════════════════════════════════

def load_nm_from_financial_reports_in_cost_price(user_id: int) -> Dict[str, Any]:
    query = """
        INSERT INTO cost_price (nm_id, user_id, sa_name, url_photo)
        SELECT DISTINCT ON (nm_id) nm_id,
                                   user_id,
                                   sa_name,
                                   'https://img.icons8.com/?size=100&id=118959&format=png&color=000000'
        FROM (
            SELECT nm_id, user_id, sa_name
            FROM financial_reports
            WHERE user_id = %s
              AND nm_id IS NOT NULL
              AND nm_id != 0

            UNION

            SELECT nm_id, user_id, vendor_code
            FROM funnel_product
            WHERE user_id = %s
              AND nm_id IS NOT NULL
              AND nm_id != 0
        ) AS combined
        ON CONFLICT (nm_id, user_id) DO NOTHING;
    """

    with get_cursor(commit=True) as cursor:
        cursor.execute(query, (user_id, user_id))
        inserted_count = cursor.rowcount

    return {
        "user_id": user_id,
        "inserted_count": inserted_count
    }


def update_photos_in_cost_price(user_id: int, photos: Dict[int, str]) -> int:
    """
    Обновляет url_photo в таблице cost_price для указанного пользователя.
    """
    if not photos:
        return 0

    values = [(url, nm_id, user_id) for nm_id, url in photos.items()]

    query = """
        UPDATE cost_price
        SET url_photo = data.url_photo,
            updated_at = NOW()
        FROM (VALUES %s) AS data(url_photo, nm_id, user_id)
        WHERE cost_price.nm_id = data.nm_id::bigint
          AND cost_price.user_id = data.user_id::bigint
          AND (cost_price.url_photo IS NULL 
               OR cost_price.url_photo = 'https://img.icons8.com/?size=100&id=118959&format=png&color=000000'
               OR cost_price.url_photo != data.url_photo)
    """

    with get_cursor(commit=True) as cursor:
        try:
            execute_values(
                cursor,
                query,
                values,
                template="(%s, %s, %s)",
                page_size=500
            )
            affected_count = cursor.rowcount
            logger.info(
                f"User {user_id}: обновлено фото в cost_price: {affected_count} "
                f"из {len(photos)} карточек"
            )
            return affected_count
        except Exception as e:
            logger.error(f"User {user_id}: ошибка обновления фото: {e}")
            raise


def get_cost_price(user_id: int) -> CostPricesApiResponse:
    logger.debug(f"Запуск get_cost_price user_id = {user_id}")
    query = """
        SELECT
            nm_id,
            sa_name,
            url_photo,
            c_price,
            fulfillment
        FROM cost_price
        WHERE user_id = %s
        ORDER BY nm_id
    """

    with get_cursor(commit=False) as cursor:
        cursor.execute(query, (user_id,))
        rows = cursor.fetchall()

    items: List[CostPriceItem] = []

    for row in rows:
        cost_price_value = row['c_price']
        fulfillment = row['fulfillment']
        if isinstance(cost_price_value, str):
            cost_price_value = float(cost_price_value)
        if isinstance(fulfillment, str):
            fulfillment = float(fulfillment)

        item = CostPriceItem(
            id=str(row['nm_id']),
            nmId=row['nm_id'],
            sa_name=row['sa_name'] or '',
            productImageUrl=row['url_photo'] or '',
            costPrice=cost_price_value,
            fulfillment=fulfillment
        )
        items.append(item)

    return CostPricesApiResponse(items=items)


def insert_cost_price(user_id: int, nm_id: int, cost_price: float, fulfillment: float):
    """
    Вставляет или обновляет себестоимость и фулфилмент для товара.
    """
    query = """
        INSERT INTO cost_price (nm_id, user_id, c_price, fulfillment)
        VALUES (%s, %s, %s, %s) ON CONFLICT (nm_id, user_id) 
        DO UPDATE SET
            c_price = COALESCE(EXCLUDED.c_price, cost_price.c_price), 
            fulfillment = COALESCE(EXCLUDED.fulfillment, cost_price.fulfillment), 
            updated_at = NOW()
    """

    with get_cursor(commit=True) as cursor:
        try:
            cursor.execute(query, (nm_id, user_id, cost_price, fulfillment))
            affected_count = cursor.rowcount
            logger.info(f"User {user_id}: nm_id={nm_id} — вставлено/обновлено {affected_count} записей")
            return affected_count
        except Exception as e:
            logger.error(f"User {user_id}: ошибка вставки данных: {e}")
            raise