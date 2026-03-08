from pydantic import BaseModel, field_validator, model_validator
from typing import Optional, Any, List
from decimal import Decimal


class CostPriceRow(BaseModel):
    """
    Модель строки таблицы cost_price.

    Поля:
        nm_id       — уникальный идентификатор номенклатуры (PK)
        user_id     — идентификатор пользователя
        c_price     — себестоимость товара
        url_photo   — ссылка на фото товара
        fulfillment — стоимость фулфилмента
        sa_name     — артикул поставщика
    """

    # ===== Идентификаторы =====
    nm_id: int  # PK — обязательное поле
    user_id: int  # NOT NULL — обязательное поле

    # ===== Финансовые данные =====
    c_price: Optional[Decimal] = None
    fulfillment: Optional[Decimal] = None

    # ===== Информация о товаре =====
    url_photo: Optional[str] = "https://media.istockphoto.com/id/1433122206/ru/%D0%B2%D0%B5%D0%BA%D1%82%D0%BE%D1%80%D0%BD%D0%B0%D1%8F/%D0%B2%D0%BE%D0%BF%D1%80%D0%BE%D1%81-%D0%BF%D1%83%D0%B7%D1%8B%D1%80%D1%8C%D0%BA%D0%BE%D0%B2-%D1%81%D1%82%D1%80%D0%BE%D0%BA%D0%B0-%D0%B8%D0%BA%D0%BE%D0%BD%D0%BA%D0%B0-%D0%B2%D0%B5%D0%BA%D1%82%D0%BE%D1%80.jpg?s=1024x1024&w=is&k=20&c=T7nCQw2SrJZIr6V5FgYH-cG7fvx888WoFHHdI0dOR8k="
    sa_name: Optional[str] = None

    class Config:
        extra = 'ignore'

    # ===== Валидаторы =====

    @model_validator(mode='before')
    @classmethod
    def convert_empty_strings_to_none(cls, data: Any) -> Any:
        """Пустые строки и строки из пробелов → None."""
        if not isinstance(data, dict):
            return data

        result = {}
        for key, value in data.items():
            if value == "" or (isinstance(value, str) and value.strip() == ""):
                result[key] = None
            else:
                result[key] = value

        return result

    @field_validator('nm_id', 'user_id', mode='before')
    @classmethod
    def parse_int_required(cls, value: Any) -> int:
        """Парсит обязательные int-поля. Не допускает None."""
        if value is None or value == "":
            raise ValueError("Поле обязательно и не может быть пустым")

        if isinstance(value, int):
            return value

        if isinstance(value, (float, Decimal)):
            return int(value)

        if isinstance(value, str):
            try:
                return int(float(value))
            except (ValueError, TypeError):
                raise ValueError(f"Невозможно преобразовать '{value}' в int")

        raise ValueError(f"Неподдерживаемый тип: {type(value)}")

    @field_validator('c_price', 'fulfillment', mode='before')
    @classmethod
    def parse_decimal(cls, value: Any) -> Optional[Decimal]:
        """Парсит Decimal из строки, int, float или возвращает None."""
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

    @field_validator('url_photo', 'sa_name', mode='before')
    @classmethod
    def parse_str(cls, value: Any) -> Optional[str]:
        """Парсит строковые поля."""
        if value is None or value == "":
            return None

        if isinstance(value, str):
            stripped = value.strip()
            return stripped if stripped else None

        # Приводим к строке, если пришёл другой тип
        return str(value)

    # ===== Методы =====

    def to_db_dict(self) -> dict:
        """
        Преобразует модель в словарь для вставки/обновления в БД.
        user_id уже содержится в модели, поэтому дополнительно не передаём.
        """
        data = self.model_dump()

        # Decimal → float для совместимости с драйверами БД
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = float(value)

        return data

    def to_db_dict_with_user(self, user_id: int) -> dict:
        """
        Преобразует модель в словарь для вставки в БД,
        перезаписывая user_id переданным значением.
        """
        data = self.to_db_dict()
        data['user_id'] = user_id
        return data


def validate_cost_prices(raw_items: List[dict]) -> List[CostPriceRow]:
    """
    Массовая валидация списка записей себестоимости.

    Args:
        raw_items: Список сырых словарей с данными.

    Returns:
        Список валидированных объектов CostPriceRow.
    """
    validated = []
    errors = []

    for i, raw in enumerate(raw_items):
        try:
            row = CostPriceRow.model_validate(raw)
            validated.append(row)
        except Exception as e:
            errors.append({
                'index': i,
                'error': str(e),
                'data': raw,
            })

    if errors:
        from utils import logger
        logger.warning(
            f"Ошибки валидации cost_price: {len(errors)} из {len(raw_items)} записей"
        )
        for err in errors[:5]:
            logger.debug(f"Ошибка в записи {err['index']}: {err['error']}")

    return validated