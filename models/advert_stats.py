"""
Модель для статистики рекламных кампаний WB.

Endpoint: /adv/v3/fullstats
"""
from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Dict, Any, Optional
from utils import logger


@dataclass
class AdvertStatsRow:
    """
    Одна строка статистики рекламы.

    Представляет: один товар (nmId) в одном типе приложения (appType)
    за один день (date) в одной рекламной кампании (advertId).
    """
    advert_id: int  # ID рекламной кампании
    date_stat: date  # Дата статистики
    app_type: int  # Тип приложения (1, 32, 64...)
    nm_id: int  # Артикул товара
    sum: float  # Сумма расходов на рекламу

    def to_db_dict(self, user_id: int) -> Dict[str, Any]:
        """Преобразует в словарь для вставки в БД"""
        return {
            "user_id": user_id,
            "advert_id": self.advert_id,
            "date_stat": self.date_stat,
            "app_type": self.app_type,
            "nm_id": self.nm_id,
            "sum": round(self.sum, 2)
        }


def parse_date(date_str: str) -> Optional[date]:
    """
    Парсит дату из формата WB API.

    Примеры форматов:
    - "2026-02-08T00:00:00Z"
    - "2026-02-08"
    """
    if not date_str:
        return None

    try:
        # Формат с временем и Z
        if "T" in date_str:
            dt_str = date_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(dt_str)
            return dt.date()
        else:
            # Просто дата
            return datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception as e:
        logger.warning(f"Не удалось распарсить дату '{date_str}': {e}")
        return None


def extract_advert_stats(
        fullstats_response: List[Dict[str, Any]]
) -> List[AdvertStatsRow]:
    """
    Извлекает нужные данные из ответа /adv/v3/fullstats.

    Структура ответа:
    [
        {
            "advertId": 33198074,
            "days": [
                {
                    "date": "2026-02-08T00:00:00Z",
                    "apps": [
                        {
                            "appType": 1,
                            "nms": [
                                {
                                    "nmId": 184024961,
                                    "sum": 48
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ]

    Returns:
        Список валидированных строк статистики
    """
    result: List[AdvertStatsRow] = []
    skipped = 0

    for advert_data in fullstats_response:
        advert_id = advert_data.get("advertId")

        if not advert_id:
            skipped += 1
            continue

        days = advert_data.get("days", [])

        for day_data in days:
            date_str = day_data.get("date")
            date_stat = parse_date(date_str)

            if not date_stat:
                skipped += 1
                continue

            apps = day_data.get("apps", [])

            for app_data in apps:
                app_type = app_data.get("appType")

                if app_type is None:
                    skipped += 1
                    continue

                nms = app_data.get("nms", [])

                for nm_data in nms:
                    nm_id = nm_data.get("nmId")
                    sum_value = nm_data.get("sum", 0)

                    if nm_id is None:
                        skipped += 1
                        continue

                    # Создаём валидную строку
                    row = AdvertStatsRow(
                        advert_id=int(advert_id),
                        date_stat=date_stat,
                        app_type=int(app_type),
                        nm_id=int(nm_id),
                        sum=float(sum_value) if sum_value else 0.0
                    )
                    result.append(row)

    if skipped > 0:
        logger.debug(f"Пропущено записей при парсинге advert stats: {skipped}")

    logger.debug(f"Извлечено записей advert stats: {len(result)}")

    return result


def extract_advert_ids(promotion_count_response: Dict[str, Any]) -> List[int]:
    """
    Извлекает список advertId из ответа /adv/v1/promotion/count.

    Структура ответа:
    {
        "adverts": [
            {
                "type": 9,
                "status": 9,
                "count": 10,
                "advert_list": [
                    {"advertId": 29749087, "changeTime": "..."},
                    {"advertId": 30027704, "changeTime": "..."}
                ]
            },
            ...
        ],
        "all": 412
    }

    Returns:
        Список всех advertId (без фильтрации по статусу/типу)
    """
    result: List[int] = []

    adverts = promotion_count_response.get("adverts", [])

    for advert_group in adverts:
        advert_list = advert_group.get("advert_list", [])

        for advert in advert_list:
            advert_id = advert.get("advertId")
            if advert_id is not None:
                result.append(int(advert_id))

    logger.debug(f"Извлечено advertId: {len(result)}")

    return result