"""
Модель для валидации ответа WB API финансового отчёта.
Преобразует пустые строки в None для корректной записи в БД.
"""

from pydantic import BaseModel, field_validator, model_validator
from typing import Optional, Any, List
from datetime import datetime, date
from decimal import Decimal


class FinancialReportRow(BaseModel):
    """
    Одна строка финансового отчёта из WB API.

    Валидирует и преобразует данные:
    - Пустые строки "" → None
    - Строковые даты → datetime/date или None
    - Числа в строках → числа или None
    """

    # ===== Идентификаторы =====
    realizationreport_id: Optional[int] = None
    rrd_id: Optional[int] = None  # Это приходит от WB API!

    # ===== Даты =====
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    create_dt: Optional[datetime] = None
    order_dt: Optional[datetime] = None
    sale_dt: Optional[datetime] = None
    rr_dt: Optional[datetime] = None
    fix_tariff_date_from: Optional[date] = None
    fix_tariff_date_to: Optional[date] = None

    # ===== Информация о товаре =====
    nm_id: Optional[int] = None
    subject_name: Optional[str] = None
    brand_name: Optional[str] = None
    sa_name: Optional[str] = None
    ts_name: Optional[str] = None
    barcode: Optional[str] = None

    # ===== Финансовые данные =====
    currency_name: Optional[str] = None
    suppliercontract_code: Optional[Any] = None  # Может быть dict или str
    gi_id: Optional[int] = None
    dlv_prc: Optional[Decimal] = None

    # ===== Документ и операция =====
    doc_type_name: Optional[str] = None
    supplier_oper_name: Optional[str] = None
    office_name: Optional[str] = None

    # ===== Количество и цены =====
    quantity: Optional[int] = None
    retail_price: Optional[Decimal] = None
    retail_amount: Optional[Decimal] = None
    retail_price_withdisc_rub: Optional[Decimal] = None
    sale_percent: Optional[int] = None
    commission_percent: Optional[Decimal] = None

    # ===== Доставка и возвраты =====
    delivery_amount: Optional[int] = None
    return_amount: Optional[int] = None
    delivery_rub: Optional[Decimal] = None
    gi_box_type_name: Optional[str] = None

    # ===== Скидки и промо =====
    product_discount_for_report: Optional[Decimal] = None
    supplier_promo: Optional[Decimal] = None

    # ===== Идентификаторы заказа =====
    rid: Optional[int] = None
    shk_id: Optional[int] = None
    srid: Optional[str] = None
    order_uid: Optional[str] = None

    # ===== PPVZ (к перечислению) =====
    ppvz_spp_prc: Optional[Decimal] = None
    ppvz_kvw_prc_base: Optional[Decimal] = None
    ppvz_kvw_prc: Optional[Decimal] = None
    sup_rating_prc_up: Optional[Decimal] = None
    is_kgvp_v2: Optional[Decimal] = None
    ppvz_sales_commission: Optional[Decimal] = None
    ppvz_for_pay: Optional[Decimal] = None
    ppvz_reward: Optional[Decimal] = None
    ppvz_vw: Optional[Decimal] = None
    ppvz_vw_nds: Optional[Decimal] = None
    ppvz_office_name: Optional[str] = None
    ppvz_office_id: Optional[int] = None
    ppvz_supplier_id: Optional[int] = None
    ppvz_supplier_name: Optional[str] = None
    ppvz_inn: Optional[str] = None

    # ===== Эквайринг =====
    acquiring_fee: Optional[Decimal] = None
    acquiring_percent: Optional[Decimal] = None
    acquiring_bank: Optional[str] = None
    payment_processing: Optional[str] = None

    # ===== Бонусы и штрафы =====
    bonus_type_name: Optional[str] = None
    penalty: Optional[Decimal] = None
    additional_payment: Optional[Decimal] = None

    # ===== Логистика =====
    rebill_logistic_cost: Optional[Decimal] = None
    rebill_logistic_org: Optional[str] = None

    # ===== Хранение и приёмка =====
    storage_fee: Optional[Decimal] = None
    deduction: Optional[Decimal] = None
    acceptance: Optional[Decimal] = None

    # ===== Прочее =====
    declaration_number: Optional[str] = None
    sticker_id: Optional[str] = None
    site_country: Optional[str] = None
    srv_dbs: Optional[bool] = None
    assembly_id: Optional[int] = None
    kiz: Optional[str] = None
    report_type: Optional[int] = None
    is_legal_entity: Optional[bool] = None
    trbx_id: Optional[str] = None

    # ===== Рассрочка и кэшбэк =====
    installment_cofinancing_amount: Optional[Decimal] = None
    wibes_wb_discount_percent: Optional[int] = None
    cashback_amount: Optional[Decimal] = None
    cashback_discount: Optional[Decimal] = None
    cashback_commission_change: Optional[Decimal] = None

    class Config:
        # Разрешаем дополнительные поля (если WB добавит новые)
        extra = 'ignore'

    # ===== Валидаторы для преобразования пустых строк =====

    @model_validator(mode='before')
    @classmethod
    def convert_empty_strings_to_none(cls, data: Any) -> Any:
        """
        Преобразует все пустые строки в None перед валидацией.
        Это главный валидатор, который решает проблему с пустыми датами.
        """
        if not isinstance(data, dict):
            return data

        result = {}
        for key, value in data.items():
            # Пустая строка → None
            if value == "" or value == "":
                result[key] = None
            # Строка только из пробелов → None
            elif isinstance(value, str) and value.strip() == "":
                result[key] = None
            else:
                result[key] = value

        return result

    @field_validator(
        'date_from', 'date_to', 'create_dt', 'order_dt', 'sale_dt', 'rr_dt',
        mode='before'
    )
    @classmethod
    def parse_datetime(cls, value: Any) -> Optional[datetime]:
        """Парсит datetime из строки или возвращает None."""
        if value is None or value == "":
            return None

        if isinstance(value, datetime):
            return value

        if isinstance(value, str):
            # Пробуем разные форматы
            formats = [
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue

            # Если ни один формат не подошёл
            return None

        return None

    @field_validator(
        'fix_tariff_date_from', 'fix_tariff_date_to',
        mode='before'
    )
    @classmethod
    def parse_date(cls, value: Any) -> Optional[date]:
        """Парсит date из строки или возвращает None."""
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
        'realizationreport_id', 'nm_id', 'gi_id', 'quantity',
        'delivery_amount', 'return_amount', 'rid', 'shk_id',
        'sale_percent', 'ppvz_office_id', 'ppvz_supplier_id',
        'assembly_id', 'report_type', 'wibes_wb_discount_percent',
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
        'dlv_prc', 'retail_price', 'retail_amount', 'retail_price_withdisc_rub',
        'commission_percent', 'delivery_rub', 'product_discount_for_report',
        'supplier_promo', 'ppvz_spp_prc', 'ppvz_kvw_prc_base', 'ppvz_kvw_prc',
        'sup_rating_prc_up', 'is_kgvp_v2', 'ppvz_sales_commission', 'ppvz_for_pay',
        'ppvz_reward', 'ppvz_vw', 'ppvz_vw_nds', 'acquiring_fee', 'acquiring_percent',
        'penalty', 'additional_payment', 'rebill_logistic_cost', 'storage_fee',
        'deduction', 'acceptance', 'installment_cofinancing_amount',
        'cashback_amount', 'cashback_discount', 'cashback_commission_change',
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
            except:
                return None

        return None

    @field_validator('srv_dbs', 'is_legal_entity', mode='before')
    @classmethod
    def parse_bool(cls, value: Any) -> Optional[bool]:
        """Парсит bool из разных форматов."""
        if value is None or value == "":
            return None

        if isinstance(value, bool):
            return value

        if isinstance(value, (int, float)):
            return bool(value)

        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'да')

        return None

    def to_db_dict(self, user_id: int) -> dict:
        """
        Преобразует модель в словарь для вставки в БД.
        НЕ исключаем rrd_id — он нужен для UPSERT!
        """
        data = self.model_dump()  # Без exclude={'rrd_id'}!
        data['user_id'] = user_id

        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = float(value)

        return data


def validate_wb_reports(raw_reports: List[dict]) -> List[FinancialReportRow]:
    """
    Валидирует список отчётов из WB API.

    Args:
        raw_reports: Сырые данные из API

    Returns:
        Список валидированных моделей
    """
    validated = []
    errors = []

    for i, raw in enumerate(raw_reports):
        try:
            report = FinancialReportRow.model_validate(raw)
            validated.append(report)
        except Exception as e:
            errors.append({
                'index': i,
                'error': str(e),
                'data': raw
            })

    if errors:
        from utils import logger
        logger.warning(f"Ошибки валидации: {len(errors)} из {len(raw_reports)} записей")
        for err in errors[:5]:  # Логируем первые 5 ошибок
            logger.debug(f"Ошибка в записи {err['index']}: {err['error']}")

    return validated