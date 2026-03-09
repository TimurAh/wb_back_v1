"""
HTTP-клиент для Wildberries API
"""
import time
import httpx
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from config import config
from utils import logger


class InvalidTokenError(Exception):
    """Выбрасывается, когда API возвращает 401 Unauthorized."""
    pass


class WBApiClient:
    """
    Клиент для работы с WB API.

    Использует единый HTTP клиент с connection pooling
    для экономии памяти и ускорения запросов.
    """

    def __init__(self, token: str):
        self.token = token
        self.base_report_url = config.WB_API_REPORT_URL
        self.base_funnel_url = config.WB_API_FUNNEL_PRODUCT_URL
        self.base_advert_url = "https://advert-api.wildberries.ru"
        self.headers = {
            "Authorization": token,
            "Content-Type": "application/json"
        }
        self.max_days = config.WB_API_MAX_DAYS_PER_REQUEST
        self.retry_delay = config.WB_API_RETRY_DELAY

        # ═══════════════════════════════════════════════════════════
        # ⚡ ОПТИМИЗАЦИЯ: Единый HTTP клиент с connection pooling
        # ═══════════════════════════════════════════════════════════
        self._http_client = httpx.Client(
            timeout=httpx.Timeout(
                timeout=60.0,
                connect=10.0,
                read=60.0,
                write=10.0,
                pool=5.0
            ),
            limits=httpx.Limits(
                max_connections=10,
                max_keepalive_connections=5,
                keepalive_expiry=30.0
            ),
            headers=self.headers,
            follow_redirects=True
        )

    def close(self):
        """Закрывает HTTP клиент и освобождает соединения"""
        if hasattr(self, '_http_client') and self._http_client is not None:
            try:
                self._http_client.close()
            except Exception as e:
                logger.debug(f"Ошибка при закрытии HTTP клиента: {e}")
            finally:
                self._http_client = None

    def __enter__(self):
        """Поддержка контекстного менеджера"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Автоматическое закрытие при выходе из контекста"""
        self.close()

    def __del__(self):
        """Автоматическое закрытие при удалении объекта"""
        self.close()

    def _split_period(
        self,
        start_date: date,
        end_date: date
    ) -> List[Tuple[date, date]]:
        """Разбивает период на интервалы по max_days дней."""
        if start_date > end_date:
            raise ValueError("Начальная дата должна быть раньше конечной")

        intervals = []
        current = start_date

        while current <= end_date:
            next_date = min(current + timedelta(days=self.max_days - 1), end_date)
            intervals.append((current, next_date))
            current = next_date + timedelta(days=1)

        return intervals

    def _format_date(self, d: date) -> str:
        """Форматирует дату для API"""
        return d.strftime("%Y-%m-%d")

    # ═══════════════════════════════════════════════════════════════
    # ФИНАНСОВЫЕ ОТЧЁТЫ
    # ═══════════════════════════════════════════════════════════════

    def get_financial_reports(
        self,
        date_from: date,
        date_to: date,
        user_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Получает финансовые отчёты за период."""
        all_reports = []
        intervals = self._split_period(date_from, date_to)
        log_prefix = f"User {user_id}: " if user_id else ""

        logger.debug(
            f"{log_prefix}Запрос отчётов: {date_from} - {date_to}, "
            f"интервалов: {len(intervals)}"
        )

        for interval_start, interval_end in intervals:
            reports = self._fetch_reports_for_interval(
                interval_start,
                interval_end,
                user_id=user_id
            )
            if len(reports) > 95000:
                logger.warning(
                    f"{log_prefix}Количество строк критическое: {len(reports)}. "
                    f"Период: {interval_start} - {interval_end}"
                )
            all_reports.extend(reports)

            if len(intervals) > 1:
                time.sleep(1)

        return all_reports

    def _fetch_reports_for_interval(
        self,
        date_from: date,
        date_to: date,
        user_id: Optional[int] = None,
        max_retries: int = 3
    ) -> List[Dict[str, Any]]:
        """Получает отчёты за один интервал."""
        url = f"{self.base_report_url}/api/v5/supplier/reportDetailByPeriod"
        params = {
            "dateFrom": self._format_date(date_from),
            "dateTo": self._format_date(date_to),
            "period": "daily"
        }
        log_prefix = f"User {user_id}: " if user_id else ""

        for attempt in range(max_retries):
            try:
                # ✅ Используем переиспользуемый клиент
                response = self._http_client.get(url, params=params)

                if response.status_code == 200:
                    data = response.json()

                    if isinstance(data, list):
                        logger.debug(
                            f"{log_prefix}Получено {len(data)} записей "
                            f"за {date_from} - {date_to}"
                        )
                        return data
                    elif isinstance(data, dict):
                        return data.get("data", [])

                    return []

                elif response.status_code == 429:
                    retry_after = response.headers.get("Retry-After", self.retry_delay)
                    wait_time = int(retry_after) if retry_after else self.retry_delay
                    logger.warning(
                        f"{log_prefix}Rate limit (429), ожидание {wait_time}с... "
                        f"(попытка {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    continue

                elif response.status_code == 401:
                    logger.error(f"{log_prefix}Невалидный токен (401): {response.text}")
                    return []

                elif response.status_code == 204:
                    logger.debug(f"{log_prefix}Нет данных за период {date_from} - {date_to}")
                    return []

                else:
                    logger.error(
                        f"{log_prefix}Ошибка API: {response.status_code} - {response.text}"
                    )
                    return []

            except httpx.TimeoutException:
                logger.warning(
                    f"{log_prefix}Таймаут запроса (попытка {attempt + 1}/{max_retries})"
                )
                time.sleep(5)
                continue

            except httpx.RequestError as e:
                logger.error(f"{log_prefix}Ошибка сети: {e}")
                time.sleep(5)
                continue

        logger.error(
            f"{log_prefix}Не удалось получить данные после {max_retries} попыток"
        )
        return []

    # ═══════════════════════════════════════════════════════════════
    # ВОРОНКА ПРОДАЖ
    # ═══════════════════════════════════════════════════════════════

    def get_funnel_products(
        self,
        selected_date: date,
        past_date: Optional[date] = None,
        user_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Получает воронку продаж по товарам."""
        if past_date is None:
            past_date = selected_date - timedelta(days=1)

        all_products = []
        offset = 0
        limit = 1000

        log_prefix = f"User {user_id}: " if user_id else ""
        logger.debug(
            f"{log_prefix}Запрос воронки: selected={selected_date}, past={past_date}"
        )

        # ⚡ Защита от бесконечного цикла
        max_pages = 1000
        page_count = 0

        while page_count < max_pages:
            page_count += 1

            products, has_more = self._fetch_funnel_page(
                selected_date=selected_date,
                past_date=past_date,
                offset=offset,
                limit=limit,
                user_id=user_id
            )

            if products:
                all_products.extend(products)

                if len(products) >= 990:
                    logger.warning(
                        f"{log_prefix}Воронка: получено {len(products)} записей "
                        f"(близко к лимиту 1000!)"
                    )

                logger.debug(
                    f"{log_prefix}Воронка: получено {len(products)} товаров "
                    f"(offset={offset}, всего={len(all_products)})"
                )

            if not has_more or len(products) < limit:
                break

            offset += limit
            time.sleep(0.5)

        if page_count >= max_pages:
            logger.error(
                f"{log_prefix}Достигнут лимит страниц ({max_pages}) — "
                f"возможна некорректная пагинация"
            )

        logger.info(
            f"{log_prefix}Воронка за {past_date} и {selected_date}: "
            f"всего {len(all_products)} товаров"
        )

        return all_products

    def _fetch_funnel_page(
        self,
        selected_date: date,
        past_date: date,
        offset: int = 0,
        limit: int = 1000,
        user_id: Optional[int] = None,
        max_retries: int = 3
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """Получает одну страницу воронки продаж."""
        base_url = self.base_funnel_url.rstrip('/')
        url = f"{base_url}/api/analytics/v3/sales-funnel/products"
        log_prefix = f"User {user_id}: " if user_id else ""

        payload = {
            "selectedPeriod": {
                "start": self._format_date(selected_date),
                "end": self._format_date(selected_date)
            },
            "pastPeriod": {
                "start": self._format_date(past_date),
                "end": self._format_date(past_date)
            },
            "offset": offset,
            "limit": limit,
            "skipDeletedNm": True
        }

        for attempt in range(max_retries):
            try:
                # ✅ Используем переиспользуемый клиент
                response = self._http_client.post(url, json=payload)

                if response.status_code == 200:
                    data = response.json()

                    if isinstance(data, dict):
                        products = data.get("data", {}).get("products", [])
                        has_more = len(products) >= limit
                        return products, has_more

                    return [], False

                elif response.status_code == 429:
                    retry_after = response.headers.get("Retry-After", self.retry_delay)
                    wait_time = int(retry_after) if retry_after else self.retry_delay
                    logger.warning(
                        f"{log_prefix}Rate limit (429), ожидание {wait_time}с... "
                        f"(попытка {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    continue

                elif response.status_code == 401:
                    error_text = response.text

                    try:
                        error_data = response.json()
                        error_detail = error_data.get("detail", "")

                        if "scope" in error_detail.lower():
                            logger.error(f"{log_prefix}Ошибка прав (Scope): {error_detail}")
                            raise PermissionError(f"Token scope not allowed: {error_detail}")

                        logger.error(f"{log_prefix}Критическая ошибка токена (401): {error_text}")
                        raise InvalidTokenError(f"Unauthorized: {error_detail or error_text}")

                    except (PermissionError, InvalidTokenError):
                        raise

                    except Exception:
                        logger.error(f"{log_prefix}Невалидный токен (401): {error_text}")
                        raise InvalidTokenError(f"Unauthorized: {error_text}")

                elif response.status_code == 204:
                    logger.debug(f"{log_prefix}Нет данных воронки за {selected_date}")
                    return [], False

                else:
                    logger.error(
                        f"{log_prefix}Ошибка API воронки: "
                        f"{response.status_code} - {response.text}"
                    )
                    return [], False

            except PermissionError:
                raise

            except httpx.TimeoutException:
                logger.warning(
                    f"{log_prefix}Таймаут запроса воронки "
                    f"(попытка {attempt + 1}/{max_retries})"
                )
                time.sleep(5)
                continue

            except httpx.RequestError as e:
                logger.error(f"{log_prefix}Ошибка сети при запросе воронки: {e}")
                time.sleep(5)
                continue

        logger.error(
            f"{log_prefix}Не удалось получить воронку после {max_retries} попыток"
        )
        return [], False

    # ═══════════════════════════════════════════════════════════════
    # РЕКЛАМНАЯ СТАТИСТИКА
    # ═══════════════════════════════════════════════════════════════

    def get_promotion_advert_ids(
        self,
        user_id: Optional[int] = None
    ) -> List[int]:
        """Получает список ID рекламных кампаний."""
        from models.advert_stats import extract_advert_ids

        url = f"{self.base_advert_url}/adv/v1/promotion/count"
        log_prefix = f"User {user_id}: " if user_id else ""

        try:
            # ✅ Используем переиспользуемый клиент
            response = self._http_client.get(url)

            if response.status_code == 200:
                data = response.json()
                advert_ids = extract_advert_ids(data)
                logger.debug(
                    f"{log_prefix}Получено {len(advert_ids)} рекламных кампаний"
                )
                return advert_ids

            elif response.status_code == 401:
                logger.warning(f"{log_prefix}Нет доступа к рекламному API (401)")
                raise PermissionError("No access to advert API")

            elif response.status_code == 204:
                logger.debug(f"{log_prefix}Нет рекламных кампаний")
                return []

            else:
                logger.error(
                    f"{log_prefix}Ошибка API promotion/count: "
                    f"{response.status_code} - {response.text}"
                )
                return []

        except PermissionError:
            raise
        except Exception as e:
            logger.error(f"{log_prefix}Ошибка запроса promotion/count: {e}")
            return []

    def get_advert_fullstats(
        self,
        advert_ids: List[int],
        date_from: date,
        date_to: date,
        user_id: Optional[int] = None
    ) -> List[Any]:
        """Получает полную статистику по рекламным кампаниям."""
        from models.advert_stats import AdvertStatsRow

        if not advert_ids:
            return []

        log_prefix = f"User {user_id}: " if user_id else ""
        all_stats: List[AdvertStatsRow] = []

        intervals = self._split_period(date_from, date_to)

        logger.debug(
            f"{log_prefix}Период {date_from} - {date_to} разбит на "
            f"{len(intervals)} интервалов"
        )

        batch_size = 50
        batches = [
            advert_ids[i:i + batch_size]
            for i in range(0, len(advert_ids), batch_size)
        ]

        total_requests = len(intervals) * len(batches)

        logger.info(
            f"{log_prefix}Запрос fullstats: {len(advert_ids)} кампаний, "
            f"{len(batches)} пачек × {len(intervals)} интервалов = "
            f"{total_requests} запросов"
        )

        request_num = 0

        for interval_idx, (interval_start, interval_end) in enumerate(intervals):
            logger.debug(
                f"{log_prefix}Интервал {interval_idx + 1}/{len(intervals)}: "
                f"{interval_start} - {interval_end}"
            )

            for batch_idx, batch in enumerate(batches):
                request_num += 1

                batch_stats = self._fetch_fullstats_batch(
                    advert_ids=batch,
                    begin_date=interval_start,
                    end_date=interval_end,
                    user_id=user_id,
                    batch_num=request_num,
                    total_batches=total_requests
                )
                all_stats.extend(batch_stats)

                if request_num < total_requests:
                    time.sleep(1)

            if interval_idx < len(intervals) - 1:
                time.sleep(0.5)

        logger.info(f"{log_prefix}Получено записей advert stats: {len(all_stats)}")

        return all_stats

    def _fetch_fullstats_batch(
        self,
        advert_ids: List[int],
        begin_date: date,
        end_date: date,
        user_id: Optional[int] = None,
        batch_num: int = 1,
        total_batches: int = 1,
        max_retries: int = 3
    ) -> List[Any]:
        """Получает статистику для одной пачки advertId."""
        from models.advert_stats import extract_advert_stats

        url = f"{self.base_advert_url}/adv/v3/fullstats"
        log_prefix = f"User {user_id}: " if user_id else ""

        params = {
            "ids": ",".join(str(id) for id in advert_ids),
            "beginDate": self._format_date(begin_date),
            "endDate": self._format_date(end_date)
        }

        for attempt in range(max_retries):
            try:
                # ✅ Используем переиспользуемый клиент (таймаут 60.0 сек)
                response = self._http_client.get(
                    url,
                    params=params,
                    timeout=60.0
                )

                if response.status_code == 200:
                    data = response.json()

                    if isinstance(data, list):
                        stats = extract_advert_stats(data)
                        logger.debug(
                            f"{log_prefix}Пачка {batch_num}/{total_batches}: "
                            f"{len(stats)} записей"
                        )
                        return stats

                    return []

                elif response.status_code == 429:
                    retry_after = response.headers.get("Retry-After", self.retry_delay)
                    wait_time = int(retry_after) if retry_after else self.retry_delay
                    logger.warning(
                        f"{log_prefix}Rate limit (429), ожидание {wait_time}с... "
                        f"(попытка {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    continue

                elif response.status_code == 401:
                    logger.warning(f"{log_prefix}Нет доступа к fullstats (401)")
                    raise PermissionError("No access to advert fullstats API")

                elif response.status_code == 204:
                    logger.debug(f"{log_prefix}Пачка {batch_num}/{total_batches}: нет данных")
                    return []

                elif response.status_code == 400:
                    logger.warning(
                        f"{log_prefix}Пачка {batch_num}/{total_batches}: "
                        f"ошибка запроса (400) - {response.text}"
                    )
                    return []

                else:
                    logger.error(
                        f"{log_prefix}Ошибка API fullstats: "
                        f"{response.status_code} - {response.text}"
                    )
                    return []

            except PermissionError:
                raise

            except httpx.TimeoutException:
                logger.warning(
                    f"{log_prefix}Таймаут fullstats "
                    f"(попытка {attempt + 1}/{max_retries})"
                )
                time.sleep(5)
                continue

            except httpx.RequestError as e:
                logger.error(f"{log_prefix}Ошибка сети fullstats: {e}")
                time.sleep(5)
                continue

        logger.error(
            f"{log_prefix}Пачка {batch_num}/{total_batches}: "
            f"не удалось получить данные после {max_retries} попыток"
        )
        return []

    # ═══════════════════════════════════════════════════════════════
    # КАРТОЧКИ ТОВАРОВ
    # ═══════════════════════════════════════════════════════════════

    def get_cards_list(
        self,
        user_id: Optional[int] = None
    ) -> Dict[int, str]:
        """Получает список карточек товаров с фотографиями."""
        url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
        log_prefix = f"User {user_id}: " if user_id else ""

        all_cards: Dict[int, str] = {}
        cursor_nm_id = 0
        cursor_updated_at = ""
        page = 0

        logger.debug(f"{log_prefix}Запрос карточек товаров (фото)")

        # ⚡ Защита от бесконечного цикла
        max_pages = 1000

        while page < max_pages:
            page += 1
            cards_batch, next_cursor = self._fetch_cards_page(
                cursor_nm_id=cursor_nm_id,
                cursor_updated_at=cursor_updated_at,
                user_id=user_id,
                page=page
            )

            if cards_batch:
                all_cards.update(cards_batch)

            if next_cursor is None or len(cards_batch) < 100:
                break

            cursor_nm_id = next_cursor["nmID"]
            cursor_updated_at = next_cursor["updatedAt"]
            time.sleep(0.5)

        if page >= max_pages:
            logger.error(
                f"{log_prefix}Достигнут лимит страниц карточек ({max_pages})"
            )

        logger.info(
            f"{log_prefix}Получено карточек с фото: {len(all_cards)} "
            f"(страниц: {page})"
        )

        return all_cards

    def _fetch_cards_page(
        self,
        cursor_nm_id: int = 0,
        cursor_updated_at: str = "",
        user_id: Optional[int] = None,
        page: int = 1,
        max_retries: int = 3
    ) -> Tuple[Dict[int, str], Optional[Dict]]:
        """Получает одну страницу карточек товаров."""
        url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
        log_prefix = f"User {user_id}: " if user_id else ""

        payload = {
            "settings": {
                "sort": {"ascending": True},
                "cursor": {"limit": 100},
                "filter": {"withPhoto": -1}
            }
        }

        if cursor_nm_id > 0:
            payload["settings"]["cursor"]["nmID"] = cursor_nm_id
            payload["settings"]["cursor"]["updatedAt"] = cursor_updated_at

        for attempt in range(max_retries):
            try:
                # ✅ Используем переиспользуемый клиент
                response = self._http_client.post(url, json=payload)

                if response.status_code == 200:
                    data = response.json()
                    cards = data.get("cards", [])
                    cursor_data = data.get("cursor", {})

                    result: Dict[int, str] = {}
                    for card in cards:
                        nm_id = card.get("nmID")
                        photos = card.get("photos", [])

                        if nm_id and photos:
                            first_photo = photos[0]
                            photo_url = first_photo.get("c246x328", "")
                            if photo_url:
                                result[nm_id] = photo_url

                    logger.debug(
                        f"{log_prefix}Карточки стр.{page}: "
                        f"{len(result)} с фото из {len(cards)} карточек"
                    )

                    next_cursor = None
                    if cards and cursor_data:
                        next_cursor = {
                            "nmID": cursor_data.get("nmID", 0),
                            "updatedAt": cursor_data.get("updatedAt", "")
                        }

                    return result, next_cursor

                elif response.status_code == 429:
                    retry_after = response.headers.get("Retry-After", self.retry_delay)
                    wait_time = int(retry_after) if retry_after else self.retry_delay
                    logger.warning(
                        f"{log_prefix}Rate limit (429) cards, ожидание {wait_time}с... "
                        f"(попытка {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    continue

                elif response.status_code == 401:
                    logger.warning(f"{log_prefix}Нет доступа к content API (401)")
                    raise PermissionError("No access to content API")

                else:
                    logger.error(
                        f"{log_prefix}Ошибка API cards/list: "
                        f"{response.status_code} - {response.text}"
                    )
                    return {}, None

            except PermissionError:
                raise

            except httpx.TimeoutException:
                logger.warning(
                    f"{log_prefix}Таймаут cards/list "
                    f"(попытка {attempt + 1}/{max_retries})"
                )
                time.sleep(5)
                continue

            except httpx.RequestError as e:
                logger.error(f"{log_prefix}Ошибка сети cards/list: {e}")
                time.sleep(5)
                continue

        logger.error(
            f"{log_prefix}Не удалось получить карточки после {max_retries} попыток"
        )
        return {}, None


def create_client(token: str) -> WBApiClient:
    """Фабрика для создания клиента"""
    return WBApiClient(token)