"""
Инициализация схемы базы данных
"""
from utils import logger
from .connection import get_cursor


# ═══════════════════════════════════════════════════════════════
# SQL для создания таблиц (разбитый на части)
# ═══════════════════════════════════════════════════════════════

CREATE_USER_TABLE = """
CREATE TABLE IF NOT EXISTS "user" (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(255),
    password VARCHAR(255),
    wb_token VARCHAR(1000),
    subscription VARCHAR(50) NOT NULL DEFAULT 'free'
);
"""

CREATE_FINANCIAL_REPORTS_TABLE = """
CREATE TABLE IF NOT EXISTS financial_reports (
    rrd_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    realizationreport_id BIGINT,
    date_from TIMESTAMPTZ,
    date_to TIMESTAMPTZ,
    create_dt TIMESTAMPTZ,
    currency_name TEXT,
    suppliercontract_code JSONB,
    gi_id BIGINT,
    dlv_prc NUMERIC,
    fix_tariff_date_from DATE,
    fix_tariff_date_to DATE,
    subject_name TEXT,
    nm_id BIGINT,
    brand_name TEXT,
    sa_name TEXT,
    ts_name TEXT,
    barcode TEXT,
    doc_type_name TEXT,
    quantity BIGINT,
    retail_price NUMERIC,
    retail_amount NUMERIC,
    sale_percent INTEGER,
    commission_percent NUMERIC,
    office_name TEXT,
    supplier_oper_name TEXT,
    order_dt TIMESTAMPTZ,
    sale_dt TIMESTAMPTZ,
    rr_dt TIMESTAMPTZ,
    shk_id BIGINT,
    retail_price_withdisc_rub NUMERIC,
    delivery_amount INTEGER,
    return_amount INTEGER,
    delivery_rub NUMERIC,
    gi_box_type_name TEXT,
    product_discount_for_report NUMERIC,
    supplier_promo NUMERIC,
    rid BIGINT,
    ppvz_spp_prc NUMERIC,
    ppvz_kvw_prc_base NUMERIC,
    ppvz_kvw_prc NUMERIC,
    sup_rating_prc_up NUMERIC,
    is_kgvp_v2 NUMERIC,
    ppvz_sales_commission NUMERIC,
    ppvz_for_pay NUMERIC,
    ppvz_reward NUMERIC,
    acquiring_fee NUMERIC,
    acquiring_percent NUMERIC,
    payment_processing TEXT,
    acquiring_bank TEXT,
    ppvz_vw NUMERIC,
    ppvz_vw_nds NUMERIC,
    ppvz_office_name TEXT,
    ppvz_office_id BIGINT,
    ppvz_supplier_id BIGINT,
    ppvz_supplier_name TEXT,
    ppvz_inn TEXT,
    declaration_number TEXT,
    bonus_type_name TEXT,
    sticker_id TEXT,
    site_country TEXT,
    srv_dbs BOOLEAN,
    penalty NUMERIC,
    additional_payment NUMERIC,
    rebill_logistic_cost NUMERIC,
    rebill_logistic_org TEXT,
    storage_fee NUMERIC,
    deduction NUMERIC,
    acceptance NUMERIC,
    assembly_id BIGINT,
    kiz TEXT,
    srid TEXT,
    report_type SMALLINT,
    is_legal_entity BOOLEAN,
    trbx_id TEXT,
    installment_cofinancing_amount NUMERIC,
    wibes_wb_discount_percent INTEGER,
    cashback_amount NUMERIC,
    cashback_discount NUMERIC,
    cashback_commission_change NUMERIC,
    order_uid TEXT
);
"""

CREATE_FUNNEL_TABLE = """
CREATE TABLE IF NOT EXISTS funnel_product (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    nm_id BIGINT NOT NULL,
    vendor_code VARCHAR(255),
    brand_name VARCHAR(255),
    stocks_wb INTEGER DEFAULT 0,
    stocks_mp INTEGER DEFAULT 0,
    stocks_balance_sum BIGINT DEFAULT 0,
    date_funnel DATE NOT NULL,
    open_count INTEGER DEFAULT 0,
    cart_count INTEGER DEFAULT 0,
    order_count INTEGER DEFAULT 0,
    order_sum BIGINT DEFAULT 0,
    cancel_count INTEGER DEFAULT 0,
    cancel_sum BIGINT DEFAULT 0,
    avg_price NUMERIC(12, 2) DEFAULT 0,
    time_to_ready_days INTEGER DEFAULT 0,
    time_to_ready_hours INTEGER DEFAULT 0,
    time_to_ready_mins INTEGER DEFAULT 0,
    localization_percent INTEGER DEFAULT 0,
    conversions_add_to_cart_percent NUMERIC(5, 2) DEFAULT 0,
    conversions_cart_to_order_percent NUMERIC(5, 2) DEFAULT 0,
    conversions_buyout_percent NUMERIC(5, 2) DEFAULT 0,
    CONSTRAINT funnel_product_user_nm_date_key UNIQUE (user_id, nm_id, date_funnel)
);
"""

CREATE_ADVERT_TABLE = """
CREATE TABLE IF NOT EXISTS advert_fullstats (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    advert_id BIGINT NOT NULL,
    date_stat DATE NOT NULL,
    app_type INTEGER NOT NULL,
    nm_id BIGINT NOT NULL,
    sum NUMERIC(12, 2) DEFAULT 0 NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_advert_fullstats UNIQUE (user_id, advert_id, date_stat, app_type, nm_id)
);
"""

CREATE_COST_PRICE_TABLE = """
CREATE TABLE IF NOT EXISTS cost_price (
    nm_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    c_price NUMERIC,
    url_photo TEXT,
    fulfillment NUMERIC,
    sa_name TEXT,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT cost_price_pkey PRIMARY KEY (nm_id, user_id)
);
"""

CREATE_INDEXES = """
-- Индексы для funnel_product
CREATE INDEX IF NOT EXISTS idx_funnel_product_user_id ON funnel_product (user_id);
CREATE INDEX IF NOT EXISTS idx_funnel_product_date ON funnel_product (date_funnel);

-- Индексы для advert_fullstats
CREATE INDEX IF NOT EXISTS idx_advert_fullstats_user_date ON advert_fullstats (user_id, date_stat);
CREATE INDEX IF NOT EXISTS idx_advert_fullstats_advert ON advert_fullstats (advert_id);
CREATE INDEX IF NOT EXISTS idx_advert_fullstats_nm ON advert_fullstats (nm_id);

-- Индексы для cost_price
CREATE INDEX IF NOT EXISTS idx_cost_price_user_id ON cost_price (user_id);

-- Индексы для financial_reports
CREATE INDEX IF NOT EXISTS idx_financial_reports_user_id ON financial_reports (user_id);
CREATE INDEX IF NOT EXISTS idx_financial_reports_nm_id ON financial_reports (nm_id);
CREATE INDEX IF NOT EXISTS idx_financial_reports_sale_dt ON financial_reports (sale_dt);
"""

CREATE_VIEWS = """
-- View для дашборда (financial_reports + cost_price)
CREATE OR REPLACE VIEW v_report_dashboard AS
SELECT
    fr.rrd_id,
    fr.user_id,
    fr.nm_id,
    fr.date_to,
    fr.doc_type_name,
    fr.supplier_oper_name,
    fr.bonus_type_name,
    fr.retail_price,
    fr.retail_amount,
    fr.quantity,
    fr.return_amount,
    fr.ppvz_for_pay,
    fr.commission_percent,
    fr.delivery_rub,
    fr.penalty,
    fr.storage_fee,
    fr.acceptance,
    fr.deduction,
    fr.sa_name,
    fr.brand_name,
    fr.subject_name,
    COALESCE(cp.c_price, 0)     AS c_price,
    COALESCE(cp.fulfillment, 0) AS fulfillment,
    cp.url_photo                AS product_image_url,
    cp.sa_name                  AS cp_sa_name
FROM financial_reports fr
LEFT JOIN cost_price cp
    ON cp.nm_id = fr.nm_id
   AND cp.user_id = fr.user_id;
"""

# Список всех SQL запросов для выполнения по порядку
SQL_STATEMENTS = [
    ("user", CREATE_USER_TABLE),
    ("financial_reports", CREATE_FINANCIAL_REPORTS_TABLE),
    ("funnel_product", CREATE_FUNNEL_TABLE),
    ("advert_fullstats", CREATE_ADVERT_TABLE),
    ("cost_price", CREATE_COST_PRICE_TABLE),
    ("views", CREATE_VIEWS),
    ("indexes", CREATE_INDEXES),
]


# ═══════════════════════════════════════════════════════════════
# Функция инициализации
# ═══════════════════════════════════════════════════════════════

def init_database() -> bool:
    """
    Инициализирует базу данных: создаёт таблицы если их нет

    Returns:
        bool: True если инициализация успешна, False иначе
    """
    try:
        logger.info("Инициализация базы данных...")

        # Выполняем каждый SQL отдельно (новое соединение для каждого)
        for name, sql in SQL_STATEMENTS:
            try:
                with get_cursor(commit=True) as cursor:
                    cursor.execute(sql)
                    logger.info(f"  ✓ {name}")
            except Exception as e:
                # Если таблица уже существует — это ОК
                if "already exists" in str(e):
                    logger.info(f"  ✓ {name} (уже существует)")
                else:
                    raise

        # Проверяем созданные таблицы
        with get_cursor() as cursor:
            cursor.execute("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = current_schema()
                ORDER BY tablename;
            """)

            tables = cursor.fetchall()
            table_names = [table['tablename'] for table in tables]

            logger.info(f"Всего таблиц в БД: {len(table_names)}")

        logger.info("✓ База данных инициализирована успешно")
        return True

    except Exception as e:
        logger.error(f"✗ Ошибка инициализации БД: {e}", exc_info=True)
        return False


__all__ = ['init_database']