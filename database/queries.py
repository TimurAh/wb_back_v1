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

        # Расшифровываем токены
        decrypted_users = []
        for user in users:
            user_dict = dict(user)

            try:
                # Расшифровываем токен
                user_dict['wb_token'] = decrypt_token(user_dict['wb_token'])
                decrypted_users.append(user_dict)
            except Exception as e:
                # Если токен не удалось расшифровать — пропускаем пользователя
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

    ПРИМЕЧАНИЕ: Эта функция теперь используется только для информации.
    Синхронизация всегда перезагружает весь период.
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
# ЗАПРОСЫ ДЛЯ DASHBOARD
# ═══════════════════════════════════════════════════════════════

def get_metrics_for_period_from_report(
    user_id: int,
    date_from: str,
    date_to: str
) -> Dict[str, Any]:
    """
    Рассчитывает агрегированные метрики за период из financial_reports.
    revenue - продажи руб. Напрямую идут в карточки
    sales_count - продажи шт. Напрямую идут в карточки
    returns_sum - возврат руб. Напрямую идут в карточки ( нет подтверждения, что это сумма корректна с количеством)
    returns_quantity - возврат шт.Напрямую идут в карточки
    cancels_sum - отмены руб.Напрямую идут в карточки
    cancels_quantity - отмены шт.Напрямую идут в карточки
    ppvz_for_pay - Перечисление продавцу. Используется для расчета чистой прибыли.
    commission  -                                                                  неправильный расчет. Потом переделать
    logistics - логистика. Напрямую идут в карточки и для расчета логистику за ед.
    penalties - Штрафы. Напрямую идут в карточки
    storage - Хранение. Используется для расчета чистой прибыли.
    acceptance - Платная приемка. Используется для расчета чистой прибыли.
    sum_cost_price - сумма себестоимости. Используется для расчета чистой прибыли и ROI.
    sum_for_contribution - сумма для налога. Используется для расчета чистой прибыли.
    deduction - Удержание без рекламы. Используется для расчета чистой прибыли.
    """
    query = """
        SELECT
            COALESCE(SUM(
                CASE 
                    WHEN doc_type_name = 'Продажа' 
                        THEN retail_price * COALESCE(quantity, 1) 
                    ELSE 0 
                END
            ), 0) as revenue,
            COALESCE(SUM(
                CASE 
                    WHEN doc_type_name = 'Возврат' 
                        THEN retail_price 
                    ELSE 0 
                END
            ), 0) as returns_sum,
            
            COALESCE(SUM(
                CASE 
                    WHEN bonus_type_name = 'От клиента при возврате' 
                        THEN COALESCE(return_amount, 1) 
                    ELSE 0 
                END
            ), 0) as returns_quantity,
            
            COALESCE(SUM(
                CASE 
                    WHEN bonus_type_name = 'От клиента при отмене' 
                        THEN ABS(retail_amount * COALESCE(return_amount, 1)) 
                    ELSE 0 
                END
            ), 0) as cancels_sum,
            COALESCE(SUM(
                CASE 
                    WHEN doc_type_name = 'Продажа' 
                        THEN retail_amount * COALESCE(quantity, 1) 
                    WHEN doc_type_name = 'Возврат' 
                        THEN -retail_amount * COALESCE(quantity, 1) 
                    ELSE 0 
                END
            ), 0) as sum_for_contribution,
            COALESCE(SUM(
                CASE 
                    WHEN bonus_type_name = 'От клиента при отмене' 
                        THEN COALESCE(return_amount, 1) 
                    ELSE 0 
                END
            ), 0) as cancels_quantity,
            COALESCE(SUM(CASE 
                    WHEN doc_type_name = 'Продажа' 
                        THEN ppvz_for_pay
                    WHEN doc_type_name = 'Возврат' 
                        THEN -ppvz_for_pay
                    ELSE 0 
                END), 0) as ppvz_for_pay,
            COALESCE(AVG(NULLIF(commission_percent, 0)), 0) as commission,
            COALESCE(SUM(delivery_rub), 0) as logistics,
            COALESCE(SUM(penalty), 0) as penalties,
            COALESCE(SUM(storage_fee), 0) as storage,
            COALESCE(SUM(acceptance), 0) as acceptance,
            COALESCE(SUM(
                CASE 
                    WHEN supplier_oper_name = 'Продажа' 
                        THEN COALESCE(quantity, 1) 
                    ELSE 0 
                END
            ), 0) as sales_count,
            COALESCE(SUM(
                CASE 
                    WHEN doc_type_name = 'Продажа' 
                        THEN COALESCE(quantity, 1) * (COALESCE(c_price,0) + COALESCE(fulfillment,0))
                    WHEN doc_type_name = 'Возврат' 
                        THEN COALESCE(quantity, 1) * (COALESCE(c_price,0) + COALESCE(fulfillment,0)) *-1
                    ELSE 0 
                END
            ), 0) as sum_cost_price,
            COALESCE(SUM(
                CASE 
                    WHEN supplier_oper_name = 'Удержание' and bonus_type_name NOT LIKE '%%WB Продвижение%%'
                        THEN COALESCE(deduction, 0)
                    ELSE 0 
                END
            ), 0) as deduction
        FROM financial_reports
            left join cost_price on cost_price.nm_id = financial_reports.nm_id
        WHERE financial_reports.user_id = %s
          AND date_to::date >= %s::date
          AND date_to::date <= %s::date
    """

    with get_cursor() as cursor:
        cursor.execute(query, (user_id, date_from, date_to))
        result = cursor.fetchone()

        if result:
            return dict(result)

        return {
        }


def get_dynamic_for_period_from_report(
        user_id: int,
        date_from: str,
        date_to: str
) -> Dict[str, Any]:
    """
    Рассчитывает данные для динамики за период из financial_reports.
    revenue - продажи руб. Напрямую идут в карточки
    sales_count - продажи шт. Напрямую идут в карточки
    returns_sum - возврат руб. Напрямую идут в карточки ( нет подтверждения, что это сумма корректна с количеством)
    returns_quantity - возврат шт.Напрямую идут в карточки
    cancels_sum - отмены руб.Напрямую идут в карточки
    cancels_quantity - отмены шт.Напрямую идут в карточки
    ppvz_for_pay - Перечисление продавцу. Используется для расчета чистой прибыли.
    commission  -                                                                  неправильный расчет. Потом переделать
    logistics - логистика. Напрямую идут в карточки и для расчета логистику за ед.
    penalties - Штрафы. Напрямую идут в карточки
    storage - Хранение. Используется для расчета чистой прибыли.
    acceptance - Платная приемка. Используется для расчета чистой прибыли.
    sum_cost_price - сумма себестоимости. Используется для расчета чистой прибыли и ROI.
    sum_for_contribution - сумма для налога. Используется для расчета чистой прибыли.
    deduction - Удержание без рекламы. Используется для расчета чистой прибыли.
    """
    query = """
            SELECT COALESCE(SUM( \
                                    CASE \
                                        WHEN doc_type_name = 'Продажа' \
                                            THEN retail_price * COALESCE(quantity, 1) \
                                        ELSE 0 \
                                        END \
                            ), 0)                                  as revenue, \
                   COALESCE(SUM( \
                                    CASE \
                                        WHEN doc_type_name = 'Возврат' \
                                            THEN retail_price \
                                        ELSE 0 \
                                        END \
                            ), 0)                                  as returns_sum, \

                   COALESCE(SUM( \
                                    CASE \
                                        WHEN bonus_type_name = 'От клиента при возврате' \
                                            THEN COALESCE(return_amount, 1) \
                                        ELSE 0 \
                                        END \
                            ), 0)                                  as returns_quantity, \

                   COALESCE(SUM( \
                                    CASE \
                                        WHEN bonus_type_name = 'От клиента при отмене' \
                                            THEN ABS(retail_amount * COALESCE(return_amount, 1)) \
                                        ELSE 0 \
                                        END \
                            ), 0)                                  as cancels_sum, \
                   COALESCE(SUM( \
                                    CASE \
                                        WHEN doc_type_name = 'Продажа' \
                                            THEN retail_amount * COALESCE(quantity, 1) \
                                        WHEN doc_type_name = 'Возврат' \
                                            THEN -retail_amount * COALESCE(quantity, 1) \
                                        ELSE 0 \
                                        END \
                            ), 0)                                  as sum_for_contribution, \
                   COALESCE(SUM( \
                                    CASE \
                                        WHEN bonus_type_name = 'От клиента при отмене' \
                                            THEN COALESCE(return_amount, 1) \
                                        ELSE 0 \
                                        END \
                            ), 0)                                  as cancels_quantity, \
                   COALESCE(SUM(CASE \
                                    WHEN doc_type_name = 'Продажа' \
                                        THEN ppvz_for_pay \
                                    WHEN doc_type_name = 'Возврат' \
                                        THEN -ppvz_for_pay \
                                    ELSE 0 \
                       END), 0)                                    as ppvz_for_pay, \
                   COALESCE(AVG(NULLIF(commission_percent, 0)), 0) as commission, \
                   COALESCE(SUM(delivery_rub), 0)                  as logistics, \
                   COALESCE(SUM(penalty), 0)                       as penalties, \
                   COALESCE(SUM(storage_fee), 0)                   as storage, \
                   COALESCE(SUM(acceptance), 0)                    as acceptance, \
                   COALESCE(SUM( \
                                    CASE \
                                        WHEN supplier_oper_name = 'Продажа' \
                                            THEN COALESCE(quantity, 1) \
                                        ELSE 0 \
                                        END \
                            ), 0)                                  as sales_count, \
                   COALESCE(SUM( \
                                    CASE \
                                        WHEN doc_type_name = 'Продажа' \
                                            THEN COALESCE(quantity, 1) * \
                                                 (COALESCE(c_price, 0) + COALESCE(fulfillment, 0)) \
                                        WHEN doc_type_name = 'Возврат' \
                                            THEN COALESCE(quantity, 1) * \
                                                 (COALESCE(c_price, 0) + COALESCE(fulfillment, 0)) * -1 \
                                        ELSE 0 \
                                        END \
                            ), 0)                                  as sum_cost_price, \
                   COALESCE(SUM( \
                                    CASE \
                                        WHEN supplier_oper_name = 'Удержание' and \
                                             bonus_type_name NOT LIKE '%%WB Продвижение%%' \
                                            THEN COALESCE(deduction, 0) \
                                        ELSE 0 \
                                        END \
                            ), 0)                                  as deduction,
                date_to as date
            FROM financial_reports
                     left join cost_price on cost_price.nm_id = financial_reports.nm_id
            WHERE financial_reports.user_id = %s
              AND date_to::date >= %s:: date
              AND date_to:: date <= %s:: date \
            GROUP BY date_to ORDER BY date_to asc
            """

    with get_cursor() as cursor:
        cursor.execute(query, (user_id, date_from, date_to))
        result = cursor.fetchall()

        if result:
            return [dict(row) for row in result]

        return {
        }

def get_detail_for_period_from_report(
        user_id: int,
        date_from: str,
        date_to: str
) -> Dict[str, Any]:
    """
    Рассчитывает данные для динамики за период из financial_reports.
    revenue - продажи руб. Напрямую идут в карточки
    sales_count - продажи шт. Напрямую идут в карточки
    returns_sum - возврат руб. Напрямую идут в карточки ( нет подтверждения, что это сумма корректна с количеством)
    returns_quantity - возврат шт.Напрямую идут в карточки
    cancels_sum - отмены руб.Напрямую идут в карточки
    cancels_quantity - отмены шт.Напрямую идут в карточки
    ppvz_for_pay - Перечисление продавцу. Используется для расчета чистой прибыли.
    commission  -                                                                  неправильный расчет. Потом переделать
    logistics - логистика. Напрямую идут в карточки и для расчета логистику за ед.
    penalties - Штрафы. Напрямую идут в карточки
    storage - Хранение. Используется для расчета чистой прибыли.
    acceptance - Платная приемка. Используется для расчета чистой прибыли.
    sum_cost_price - сумма себестоимости. Используется для расчета чистой прибыли и ROI.
    sum_for_contribution - сумма для налога. Используется для расчета чистой прибыли.
    deduction - Удержание без рекламы. Используется для расчета чистой прибыли.
    """
    query = """
            SELECT COALESCE(SUM( \
                                    CASE \
                                        WHEN doc_type_name = 'Продажа' \
                                            THEN retail_price * COALESCE(quantity, 1) \
                                        ELSE 0 \
                                        END \
                            ), 0)                                  as revenue, \
                   COALESCE(SUM( \
                                    CASE \
                                        WHEN doc_type_name = 'Возврат' \
                                            THEN retail_price \
                                        ELSE 0 \
                                        END \
                            ), 0)                                  as returns_sum, \

                   COALESCE(SUM( \
                                    CASE \
                                        WHEN bonus_type_name = 'От клиента при возврате' \
                                            THEN COALESCE(return_amount, 1) \
                                        ELSE 0 \
                                        END \
                            ), 0)                                  as returns_quantity, \

                   COALESCE(SUM( \
                                    CASE \
                                        WHEN bonus_type_name = 'От клиента при отмене' \
                                            THEN ABS(retail_amount * COALESCE(return_amount, 1)) \
                                        ELSE 0 \
                                        END \
                            ), 0)                                  as cancels_sum, \
                   COALESCE(SUM( \
                                    CASE \
                                        WHEN doc_type_name = 'Продажа' \
                                            THEN retail_amount * COALESCE(quantity, 1) \
                                        WHEN doc_type_name = 'Возврат' \
                                            THEN -retail_amount * COALESCE(quantity, 1) \
                                        ELSE 0 \
                                        END \
                            ), 0)                                  as sum_for_contribution, \
                   COALESCE(SUM( \
                                    CASE \
                                        WHEN bonus_type_name = 'От клиента при отмене' \
                                            THEN COALESCE(return_amount, 1) \
                                        ELSE 0 \
                                        END \
                            ), 0)                                  as cancels_quantity, \
                   COALESCE(SUM(CASE \
                                    WHEN doc_type_name = 'Продажа' \
                                        THEN ppvz_for_pay \
                                    WHEN doc_type_name = 'Возврат' \
                                        THEN -ppvz_for_pay \
                                    ELSE 0 \
                       END), 0)                                    as ppvz_for_pay, \
                   COALESCE(AVG(NULLIF(commission_percent, 0)), 0) as commission, \
                   COALESCE(SUM(delivery_rub), 0)                  as logistics, \
                   COALESCE(SUM(penalty), 0)                       as penalties, \
                   COALESCE(SUM(storage_fee), 0)                   as storage, \
                   COALESCE(SUM(acceptance), 0)                    as acceptance, \
                   COALESCE(SUM( \
                                    CASE \
                                        WHEN supplier_oper_name = 'Продажа' \
                                            THEN COALESCE(quantity, 1) \
                                        ELSE 0 \
                                        END \
                            ), 0)                                  as sales_count, \
                   COALESCE(SUM( \
                                    CASE \
                                        WHEN doc_type_name = 'Продажа' \
                                            THEN COALESCE(quantity, 1) * \
                                                 (COALESCE(c_price, 0) + COALESCE(fulfillment, 0)) \
                                        WHEN doc_type_name = 'Возврат' \
                                            THEN COALESCE(quantity, 1) * \
                                                 (COALESCE(c_price, 0) + COALESCE(fulfillment, 0)) * -1 \
                                        ELSE 0 \
                                        END \
                            ), 0)                                  as sum_cost_price, \
                   COALESCE(SUM( \
                                    CASE \
                                        WHEN supplier_oper_name = 'Удержание' and \
                                             bonus_type_name NOT LIKE '%%WB Продвижение%%' \
                                            THEN COALESCE(deduction, 0) \
                                        ELSE 0 \
                                        END \
                            ), 0)                                  as deduction,
                financial_reports.nm_id as nm_id,
                Max(cost_price.url_photo) as product_image_url,
                Max(cost_price.sa_name) as sa_name
            FROM financial_reports
                     left join cost_price on cost_price.nm_id = financial_reports.nm_id
            WHERE financial_reports.user_id = %s
              AND date_to::date >= %s:: date
              AND date_to:: date <= %s:: date \

            GROUP BY financial_reports.nm_id 
            ORDER BY financial_reports.nm_id asc
            """

    with get_cursor() as cursor:
        cursor.execute(query, (user_id, date_from, date_to))
        result = cursor.fetchall()

        if result:
            return [dict(row) for row in result]

        return {
        }

def get_dynamic_for_period_from_funnel(
    user_id: int,
    date_from: str,
    date_to: str
) -> List[Dict[str, Any]]:
    """
    Рассчитывает метрики из funnel_product с группировкой по дням.
    """
    query = """
        SELECT 
            fp.date_funnel                                AS date,
            COALESCE(SUM(fp.order_sum), 0)                AS order_sum,
            COALESCE(SUM(fp.order_count), 0)              AS order_count,
            COALESCE(SUM(fp.stocks_wb + fp.stocks_mp), 0) AS stocks_balance_sum,
            COALESCE(SUM(fp.open_count), 0)               AS open_count,
            COALESCE(SUM(fp.cart_count), 0)                AS cart_count,
            COALESCE(SUM(fp.cancel_sum), 0)               AS cancel_sum,
            COALESCE(SUM(fp.cancel_count), 0)              AS cancel_count,
            CASE 
                WHEN SUM(fp.open_count) > 0 
                THEN SUM(fp.cart_count) * 100.0 / SUM(fp.open_count) 
                ELSE 0 
            END AS conversions_add_to_cart_percent,
            CASE 
                WHEN SUM(fp.cart_count) > 0 
                THEN SUM(fp.order_count) * 100.0 / SUM(fp.cart_count) 
                ELSE 0 
            END AS conversions_cart_to_order_percent,
            AVG(NULLIF(fp.conversions_buyout_percent, 0)) AS conversions_buyout_percent
        FROM funnel_product fp
        WHERE fp.user_id = %(user_id)s
          AND fp.date_funnel >= %(date_from)s::date
          AND fp.date_funnel <= %(date_to)s::date
        GROUP BY fp.date_funnel
        ORDER BY fp.date_funnel
    """

    with get_cursor() as cursor:
        cursor.execute(query, {
            "user_id": user_id,
            "date_from": date_from,
            "date_to": date_to
        })
        result = cursor.fetchall()

        if result:
            return [dict(row) for row in result]

        return []

def get_detail_for_period_from_funnel(
    user_id: int,
    date_from: str,
    date_to: str
) -> List[Dict[str, Any]]:
    """
    Рассчитывает метрики из funnel_product с группировкой по дням.
    """
    query = """
        SELECT 
            fp.nm_id                                AS nm_id,
            COALESCE(SUM(fp.order_sum), 0)                AS order_sum,
            COALESCE(SUM(fp.order_count), 0)              AS order_count,
            COALESCE(SUM(fp.stocks_wb + fp.stocks_mp), 0) AS stocks_balance_sum,
            COALESCE(SUM(fp.open_count), 0)               AS open_count,
            COALESCE(SUM(fp.cart_count), 0)                AS cart_count,
            COALESCE(SUM(fp.cancel_sum), 0)               AS cancel_sum,
            COALESCE(SUM(fp.cancel_count), 0)              AS cancel_count,
            CASE 
                WHEN SUM(fp.open_count) > 0 
                THEN SUM(fp.cart_count) * 100.0 / SUM(fp.open_count) 
                ELSE 0 
            END AS conversions_add_to_cart_percent,
            CASE 
                WHEN SUM(fp.cart_count) > 0 
                THEN SUM(fp.order_count) * 100.0 / SUM(fp.cart_count) 
                ELSE 0 
            END AS conversions_cart_to_order_percent,
            AVG(NULLIF(fp.conversions_buyout_percent, 0)) AS conversions_buyout_percent,
            Max(fp.vendor_code) as vendor_code,
            Max(cost_price.url_photo) as product_image_url,
            Max(cost_price.sa_name) as sa_name
        FROM funnel_product fp
            left join cost_price on cost_price.nm_id = fp.nm_id
        WHERE fp.user_id = %(user_id)s
          AND fp.date_funnel >= %(date_from)s::date
          AND fp.date_funnel <= %(date_to)s::date
        GROUP BY fp.nm_id
        ORDER BY fp.nm_id
    """

    with get_cursor() as cursor:
        cursor.execute(query, {
            "user_id": user_id,
            "date_from": date_from,
            "date_to": date_to
        })
        result = cursor.fetchall()

        if result:
            return [dict(row) for row in result]

        return []


def get_metrics_for_period_from_advert_stats(
    user_id: int,
    date_from: str,
    date_to: str
) -> Dict[str, Any]:
    """
    Рассчитывает агрегированные метрики за период из advert_stats.
    """
    query = """
        SELECT 
             SUM(sum) as ad_expense
            FROM advert_fullstats
            WHERE user_id = %(user_id)s
              AND date_stat >= %(date_from)s::date
              AND date_stat <= %(date_to)s::date
    """

    with get_cursor() as cursor:
        cursor.execute(query, {
            "user_id":user_id,
            "date_from":date_from,
            "date_to":date_to
        })
        result = cursor.fetchone()

        if result:
            return dict(result)

        return {
            'ad_expense': 0
        }
def get_dynamic_for_period_from_advert_stats(
    user_id: int,
    date_from: str,
    date_to: str
) -> Dict[str, Any]:
    """
    Рассчитывает данные для динамики за период из advert_stats.
    """
    query = """
        SELECT 
             SUM(sum) as ad_expense,
            date_stat as date
            FROM advert_fullstats
            WHERE user_id = %(user_id)s
              AND date_stat >= %(date_from)s::date
              AND date_stat <= %(date_to)s::date
            group by date_stat order by date_stat asc
    """

    with get_cursor() as cursor:
        cursor.execute(query, {
            "user_id":user_id,
            "date_from":date_from,
            "date_to":date_to
        })
        result = cursor.fetchall()

        if result:
            return [dict(row) for row in result]

        return {
            'ad_expense': 0
        }


def get_detail_for_period_from_advert_stats(
    user_id: int,
    date_from: str,
    date_to: str
) -> Dict[str, Any]:
    """
    Рассчитывает данные для динамики за период из advert_stats.
    """
    query = """
        SELECT 
             SUM(sum) as ad_expense,
            nm_id as nm_id
            FROM advert_fullstats
            WHERE user_id = %(user_id)s
              AND date_stat >= %(date_from)s::date
              AND date_stat <= %(date_to)s::date
            group by nm_id order by nm_id asc
    """

    with get_cursor() as cursor:
        cursor.execute(query, {
            "user_id":user_id,
            "date_from":date_from,
            "date_to":date_to
        })
        result = cursor.fetchall()

        if result:
            return [dict(row) for row in result]

        return {
            'ad_expense': 0
        }

def get_metrics_for_period_from_funnel(
    user_id: int,
    date_from: str,
    date_to: str
) -> Dict[str, Any]:
    """
    Рассчитывает агрегированные метрики за период из funnel_product.
    """
    query = """
        WITH daily AS (
            SELECT 
                date_funnel,
                SUM(order_sum)                             AS order_sum,
                SUM(order_count)                           AS order_count,
                SUM(stocks_wb + stocks_mp)                 AS stocks_balance_sum,
                SUM(open_count)                            AS open_count,
                SUM(cart_count)                             AS cart_count,
                SUM(cancel_sum)                            AS cancel_sum,
                SUM(cancel_count)                          AS cancel_count,
                AVG(NULLIF(conversions_buyout_percent, 0)) AS conversions_buyout_percent
            FROM funnel_product
            WHERE user_id = %(user_id)s
              AND date_funnel >= %(date_from)s::date
              AND date_funnel <= %(date_to)s::date
            GROUP BY date_funnel
        )
        SELECT 
            COALESCE(SUM(fp.order_sum), 0)       AS order_sum,
            COALESCE(SUM(fp.order_count), 0)     AS order_count,
            (Select Max(stocks_balance_sum) from daily) AS stocks_balance_sum,
            COALESCE(SUM(fp.open_count), 0)      AS open_count,
            COALESCE(SUM(fp.cancel_sum), 0)      AS cancel_sum,
            COALESCE(SUM(fp.cancel_count), 0)    AS cancel_count,
            CASE 
                WHEN SUM(fp.open_count) > 0 
                THEN SUM(fp.cart_count) * 100.0 / SUM(fp.open_count) 
                ELSE 0 
            END AS conversions_add_to_cart_percent,
            CASE 
                WHEN SUM(fp.cart_count) > 0 
                THEN SUM(fp.order_count) * 100.0 / SUM(fp.cart_count) 
                ELSE 0 
            END AS conversions_cart_to_order_percent,
            AVG(NULLIF(fp.conversions_buyout_percent, 0)) AS conversions_buyout_percent
        FROM funnel_product fp
            WHERE fp.user_id = %(user_id)s
              AND fp.date_funnel >= %(date_from)s::date
              AND fp.date_funnel <= %(date_to)s::date
    """

    with get_cursor() as cursor:
        cursor.execute(query, {
            "user_id":user_id,
            "date_from":date_from,
            "date_to":date_to
        })
        result = cursor.fetchone()

        if result:
            return dict(result)

        return {

        }

def get_details_by_product(
    user_id: int,
    date_from: str,
    date_to: str
) -> List[Dict[str, Any]]:
    """
    Получает детализацию по товарам (артикулам).
    """
    query = """
        SELECT 
            nm_id,
            sa_name as product_name,
            brand_name,
            COALESCE(SUM(ppvz_for_pay), 0) as net_profit_rub,
            COALESCE(SUM(
                CASE 
                    WHEN supplier_oper_name = 'Продажа' 
                        THEN retail_price_withdisc_rub * COALESCE(quantity, 1) 
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
        FROM financial_reports
        WHERE user_id = %s
          AND date_to::date >= %s::date
          AND date_to::date <= %s::date
          AND nm_id IS NOT NULL
        GROUP BY nm_id, sa_name, brand_name
        ORDER BY COALESCE(SUM(ppvz_for_pay), 0) DESC
        LIMIT 50
    """

    with get_cursor() as cursor:
        cursor.execute(query, (user_id, date_from, date_to))
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
# ФИНАНСОВЫЕ ОТЧЁТЫ — ПОЛНАЯ СИНХРОНИЗАЦИЯ С UPSERT
# ═══════════════════════════════════════════════════════════════

def insert_financial_reports(user_id: int, reports: List[Dict[str, Any]]) -> int:
    """
    Вставляет или обновляет финансовые отчёты в БД.

    ЛОГИКА: UPSERT по rrd_id
    - Если запись с таким rrd_id есть — обновляем все поля
    - Если нет — вставляем новую

    Это позволяет синхронизировать изменённые данные от WB.
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

    db_rows = [report.to_db_dict(user_id) for report in validated_reports]

    # Все поля для вставки (rrd_id теперь обязательное!)
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

    def prepare_row(row_dict: dict) -> tuple:
        import json
        values = []
        for field in fields:
            value = row_dict.get(field)
            if field == "suppliercontract_code" and value is not None:
                if not isinstance(value, str):
                    value = json.dumps(value)
            values.append(value)
        return tuple(values)

    values = [prepare_row(row) for row in db_rows]

    # Фильтруем записи без rrd_id (они не могут быть вставлены)
    valid_values = [v for v in values if v[0] is not None]
    skipped = len(values) - len(valid_values)

    if skipped > 0:
        logger.warning(f"User {user_id}: пропущено {skipped} записей без rrd_id")

    if not valid_values:
        logger.warning(f"User {user_id}: нет записей с rrd_id для вставки")
        return 0

    fields_str = ", ".join(fields)

    # Поля для обновления (все кроме rrd_id)
    update_fields = [f for f in fields if f != "rrd_id"]
    update_str = ", ".join([f"{f} = EXCLUDED.{f}" for f in update_fields])

    query = f"""
        INSERT INTO financial_reports ({fields_str})
        VALUES %s
        ON CONFLICT (rrd_id) 
        DO UPDATE SET {update_str}
    """

    with get_cursor(commit=True) as cursor:
        try:
            execute_values(cursor, query, valid_values, page_size=1000)
            affected_count = cursor.rowcount
            logger.info(
                f"User {user_id}: отчёты — вставлено/обновлено {affected_count} записей"
            )
            return affected_count
        except Exception as e:
            logger.error(f"User {user_id}: ошибка вставки данных: {e}")
            raise


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

    ПРИМЕЧАНИЕ: Эта функция теперь используется только для информации.
    Синхронизация всегда перезагружает весь период.
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

    ЛОГИКА: UPSERT по (user_id, nm_id, date_funnel)
    - Если запись существует — обновляем все поля
    - Если нет — вставляем новую

    Args:
        user_id: ID пользователя
        products: Сырые данные из WB API
        extract_both_periods: Извлекать оба периода (selected + past)

    Returns:
        Количество вставленных/обновлённых записей
    """
    from models.funnel_product import (
        validate_funnel_products,
        extract_both_periods as extract_both
    )

    if not products:
        return 0

    # ===== ШАГ 1: Валидация =====
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

    # ===== ШАГ 2: Преобразование в словари для БД =====
    db_rows = [product.to_db_dict(user_id) for product in validated_products]

    # ===== ШАГ 3: Формируем список полей =====
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

    # ===== ШАГ 4: Подготовка значений =====
    def prepare_row(row_dict: dict) -> tuple:
        values = []
        for field in fields:
            value = row_dict.get(field)
            values.append(value)
        return tuple(values)

    values = [prepare_row(row) for row in db_rows]

    # ===== ШАГ 5: INSERT с ON CONFLICT DO UPDATE =====
    fields_str = ", ".join(fields)

    update_fields = [f for f in fields if f not in ("user_id", "nm_id", "date_funnel")]
    update_str = ", ".join([f"{f} = EXCLUDED.{f}" for f in update_fields])

    query = f"""
        INSERT INTO funnel_product ({fields_str})
        VALUES %s
        ON CONFLICT (user_id, nm_id, date_funnel) 
        DO UPDATE SET {update_str}
    """

    with get_cursor(commit=True) as cursor:
        try:
            execute_values(cursor, query, values, page_size=1000)
            affected_count = cursor.rowcount
            logger.info(
                f"User {user_id}: воронка — вставлено/обновлено {affected_count} записей"
            )
            return affected_count
        except Exception as e:
            logger.error(f"User {user_id}: ошибка вставки воронки: {e}")
            raise


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
        stats: List[Any]  # List[AdvertStatsRow]
) -> int:
    """
    Вставляет или обновляет статистику рекламных кампаний.

    ЛОГИКА: UPSERT по (user_id, advert_id, date_stat, app_type, nm_id)
    - Если запись существует — обновляем sum
    - Если нет — вставляем новую

    Args:
        user_id: ID пользователя
        stats: Список AdvertStatsRow из парсинга API

    Returns:
        Количество вставленных/обновлённых записей
    """
    if not stats:
        return 0

    # Преобразуем в словари для БД
    db_rows = [stat.to_db_dict(user_id) for stat in stats]

    # Поля для вставки
    fields = [
        "user_id", "advert_id", "date_stat", "app_type", "nm_id", "sum"
    ]

    # Подготовка значений
    def prepare_row(row_dict: dict) -> tuple:
        return tuple(row_dict.get(field) for field in fields)

    values = [prepare_row(row) for row in db_rows]

    fields_str = ", ".join(fields)

    # При конфликте обновляем sum и updated_at
    query = f"""
        INSERT INTO advert_fullstats ({fields_str}, updated_at)
        VALUES %s
        ON CONFLICT (user_id, advert_id, date_stat, app_type, nm_id) 
        DO UPDATE SET 
            sum = EXCLUDED.sum,
            updated_at = NOW()
    """

    # Добавляем updated_at к значениям
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
            DELETE \
            FROM advert_fullstats
            WHERE date_stat < %s \
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
            SELECT COUNT(*)                  as total_records, \
                   COUNT(DISTINCT advert_id) as unique_adverts, \
                   COUNT(DISTINCT nm_id)     as unique_products, \
                   MIN(date_stat)            as first_date, \
                   MAX(date_stat)            as last_date, \
                   COUNT(DISTINCT date_stat) as days_count, \
                   COALESCE(SUM(sum), 0)     as total_sum
            FROM advert_fullstats
            WHERE user_id = %s \
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


def load_nm_from_financial_reports_in_cost_price(user_id: int) -> Dict[str, Any]:
    query = """
            INSERT INTO cost_price (nm_id, user_id, sa_name, url_photo)
            SELECT DISTINCT \
            ON (nm_id)
                nm_id,
                user_id,
                sa_name,
                'https://img.icons8.com/?size=100&id=118959&format=png&color=000000'
            FROM financial_reports
            WHERE user_id = %s
              AND nm_id IS NOT NULL
              AND nm_id != 0
            ON CONFLICT (nm_id, user_id) DO NOTHING;
            """

    with get_cursor(commit=True) as cursor:
        cursor.execute(query, (user_id,))
        # rowcount покажет количество реально ВСТАВЛЕННЫХ строк
        # (те, что попали в ON CONFLICT DO NOTHING, не учитываются)
        inserted_count = cursor.rowcount

    return {
        "user_id": user_id,
        "inserted_count": inserted_count
    }

def update_photos_in_cost_price(user_id: int, photos: Dict[int, str]) -> int:
    """
    Обновляет url_photo в таблице cost_price для указанного пользователя.

    Args:
        user_id: ID пользователя
        photos: Словарь {nm_id: url_photo}

    Returns:
        Количество обновлённых записей
    """
    if not photos:
        return 0

    # Формируем список значений для batch update
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
        # Безопасное получение cost_price
        cost_price_value = row['c_price']
        fulfillment = row['fulfillment']
        if isinstance(cost_price_value, str):
            cost_price_value = float(cost_price_value)  # Если строка — fulfillment
        if isinstance(fulfillment, str):
            fulfillment = float(fulfillment)   # Если строка — преобразуем

        item = CostPriceItem(
            id=str(row['nm_id']),  # nm_id как уникальный id
            nmId=row['nm_id'],
            sa_name=row['sa_name'] or '',
            productImageUrl=row['url_photo'] or '',
            costPrice=cost_price_value,
            fulfillment=fulfillment
        )
        items.append(item)

    return CostPricesApiResponse(items=items)

def insert_cost_price(user_id: int,nm_id: int, cost_price: float, fulfillment:float):
    """
       Вставляет или обновляет себестоимость и фулфилмент для товара.

       Args:
           user_id: ID пользователя
           nm_id: ID номенклатуры (артикул WB)
           cost_price: Себестоимость (может быть None)
           fulfillment: Фулфилмент (может быть None)
       """
    query = """
            INSERT INTO cost_price (nm_id, user_id, c_price, fulfillment)
            VALUES (%s, %s, %s, %s) ON CONFLICT (nm_id, user_id) 
            DO 
            UPDATE SET
                c_price = COALESCE (EXCLUDED.c_price, cost_price.c_price), 
                fulfillment = COALESCE (EXCLUDED.fulfillment, cost_price.fulfillment), 
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