"""
Задачи для выполнения по расписанию.

ЛОГИКА СИНХРОНИЗАЦИИ:
- Всегда перезагружаем ВЕСЬ период (DATA_RETENTION_MONTHS месяцев)
- Используем UPSERT для обновления изменённых записей

МНОГОПОТОЧНОСТЬ:
- Каждая задача (отчёты/воронка/реклама) для каждого пользователя — отдельный поток
- Ограничения настраиваются через config

ПОРЯДОК ВЫПОЛНЕНИЯ:
- COSTPRICE запускается сразу после успешного завершения REPORTS для каждого пользователя
"""
from datetime import date, timedelta
from typing import Optional, Dict, List, Set
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from dataclasses import dataclass
from enum import Enum
import threading
import time


from database import (
    get_users_with_tokens,
    insert_financial_reports,
    cleanup_old_reports,
    insert_funnel_products,
    cleanup_old_funnel_data,
    insert_advert_stats,
    cleanup_old_advert_stats,
    load_nm_from_financial_reports_in_cost_price,
    update_photos_in_cost_price
)
from wb_api import create_client
from config import config
from utils import logger


# ═══════════════════════════════════════════════════════════════
# ТИПЫ ЗАДАЧ
# ═══════════════════════════════════════════════════════════════

class TaskType(str, Enum):
    REPORTS = "reports"
    FUNNEL = "funnel"
    ADVERT = "advert"
    COSTPRICE = "costprice"


@dataclass
class TaskResult:
    """Результат выполнения одной задачи"""
    user_id: int
    username: str
    task_type: TaskType
    success: bool
    records_count: int
    no_access: bool
    error: Optional[str]
    duration_seconds: float


@dataclass
class SyncTask:
    """Описание задачи для выполнения"""
    user_id: int
    username: str
    wb_token: Optional[str]  # Optional, т.к. COSTPRICE не требует токена
    task_type: TaskType


# ═══════════════════════════════════════════════════════════════
# ВЫЧИСЛЕНИЕ ПЕРИОДА
# ═══════════════════════════════════════════════════════════════

def calculate_full_period() -> tuple[date, date]:
    """
    Возвращает полный период для синхронизации.
    От (сегодня - DATA_RETENTION_MONTHS месяцев) до вчера.
    """
    months = config.DATA_RETENTION_MONTHS
    date_from = date.today() - timedelta(days=months * 30)
    date_to = date.today() - timedelta(days=1)
    return date_from, date_to


# ═══════════════════════════════════════════════════════════════
# ФУНКЦИИ СИНХРОНИЗАЦИИ ДЛЯ КАЖДОГО ТИПА ЗАДАЧИ
# ═══════════════════════════════════════════════════════════════

def sync_user_reports(user_id: int, username: str, wb_token: str) -> TaskResult:
    """Синхронизирует финансовые отчёты для одного пользователя."""
    start_time = time.time()
    thread_name = threading.current_thread().name
    date_from, date_to = calculate_full_period()

    logger.info(
        f"[{thread_name}] User {user_id}: → Отчёты за {date_from} - {date_to}"
    )

    try:
        client = create_client(wb_token)
        reports = client.get_financial_reports(date_from, date_to, user_id=user_id)

        if not reports:
            logger.info(f"[{thread_name}] User {user_id}: нет данных отчётов")
            return TaskResult(
                user_id=user_id,
                username=username,
                task_type=TaskType.REPORTS,
                success=True,
                records_count=0,
                no_access=False,
                error=None,
                duration_seconds=time.time() - start_time
            )

        inserted = insert_financial_reports(user_id, reports)

        logger.info(
            f"[{thread_name}] User {user_id}: ← Отчёты синхронизированы: {inserted}"
        )

        return TaskResult(
            user_id=user_id,
            username=username,
            task_type=TaskType.REPORTS,
            success=True,
            records_count=inserted,
            no_access=False,
            error=None,
            duration_seconds=time.time() - start_time
        )

    except Exception as e:
        logger.error(f"[{thread_name}] User {user_id}: ошибка отчётов: {e}")
        return TaskResult(
            user_id=user_id,
            username=username,
            task_type=TaskType.REPORTS,
            success=False,
            records_count=0,
            no_access=False,
            error=str(e),
            duration_seconds=time.time() - start_time
        )


def sync_user_funnel(user_id: int, username: str, wb_token: str) -> TaskResult:
    """Синхронизирует воронку продаж для одного пользователя."""
    start_time = time.time()
    thread_name = threading.current_thread().name
    date_from, date_to = calculate_full_period()
    total_days = (date_to - date_from).days + 1

    logger.info(
        f"[{thread_name}] User {user_id}: → Воронка за {date_from} - {date_to}"
    )

    try:
        client = create_client(wb_token)
        total_inserted = 0
        requests_count = 0

        if total_days == 1:
            products = client.get_funnel_products(
                selected_date=date_from,
                past_date=date_from - timedelta(days=1),
                user_id=user_id
            )
            requests_count += 1
            if products:
                inserted = insert_funnel_products(user_id, products, extract_both_periods=False)
                total_inserted += inserted
        else:
            current_date = date_from + timedelta(days=1)
            while current_date <= date_to:
                past_date = current_date - timedelta(days=1)
                products = client.get_funnel_products(
                    selected_date=current_date,
                    past_date=past_date,
                    user_id=user_id
                )
                requests_count += 1
                if products:
                    inserted = insert_funnel_products(user_id, products, extract_both_periods=True)
                    total_inserted += inserted
                current_date += timedelta(days=2)

            last_covered_selected = current_date - timedelta(days=2)
            if last_covered_selected < date_to:
                products = client.get_funnel_products(
                    selected_date=date_to,
                    past_date=date_to - timedelta(days=1),
                    user_id=user_id
                )
                requests_count += 1
                if products:
                    inserted = insert_funnel_products(user_id, products, extract_both_periods=False)
                    total_inserted += inserted

        logger.info(
            f"[{thread_name}] User {user_id}: ← Воронка синхронизирована: "
            f"{total_inserted} (запросов: {requests_count})"
        )

        return TaskResult(
            user_id=user_id,
            username=username,
            task_type=TaskType.FUNNEL,
            success=True,
            records_count=total_inserted,
            no_access=False,
            error=None,
            duration_seconds=time.time() - start_time
        )

    except PermissionError:
        logger.warning(
            f"[{thread_name}] User {user_id}: нет доступа к воронке (scope)"
        )
        return TaskResult(
            user_id=user_id,
            username=username,
            task_type=TaskType.FUNNEL,
            success=True,
            records_count=0,
            no_access=True,
            error=None,
            duration_seconds=time.time() - start_time
        )

    except Exception as e:
        logger.error(f"[{thread_name}] User {user_id}: ошибка воронки: {e}")
        return TaskResult(
            user_id=user_id,
            username=username,
            task_type=TaskType.FUNNEL,
            success=False,
            records_count=0,
            no_access=False,
            error=str(e),
            duration_seconds=time.time() - start_time
        )


def sync_user_advert_stats(user_id: int, username: str, wb_token: str) -> TaskResult:
    """Синхронизирует рекламную статистику для одного пользователя."""
    start_time = time.time()
    thread_name = threading.current_thread().name
    date_from, date_to = calculate_full_period()

    logger.info(
        f"[{thread_name}] User {user_id}: → Реклама за {date_from} - {date_to}"
    )

    try:
        client = create_client(wb_token)

        advert_ids = client.get_promotion_advert_ids(user_id=user_id)

        if not advert_ids:
            logger.info(f"[{thread_name}] User {user_id}: нет рекламных кампаний")
            return TaskResult(
                user_id=user_id,
                username=username,
                task_type=TaskType.ADVERT,
                success=True,
                records_count=0,
                no_access=False,
                error=None,
                duration_seconds=time.time() - start_time
            )

        stats = client.get_advert_fullstats(
            advert_ids=advert_ids,
            date_from=date_from,
            date_to=date_to,
            user_id=user_id
        )

        if not stats:
            logger.info(f"[{thread_name}] User {user_id}: нет данных рекламы")
            return TaskResult(
                user_id=user_id,
                username=username,
                task_type=TaskType.ADVERT,
                success=True,
                records_count=0,
                no_access=False,
                error=None,
                duration_seconds=time.time() - start_time
            )

        inserted = insert_advert_stats(user_id, stats)

        logger.info(
            f"[{thread_name}] User {user_id}: ← Реклама синхронизирована: {inserted}"
        )

        return TaskResult(
            user_id=user_id,
            username=username,
            task_type=TaskType.ADVERT,
            success=True,
            records_count=inserted,
            no_access=False,
            error=None,
            duration_seconds=time.time() - start_time
        )

    except PermissionError:
        logger.warning(
            f"[{thread_name}] User {user_id}: нет доступа к рекламному API"
        )
        return TaskResult(
            user_id=user_id,
            username=username,
            task_type=TaskType.ADVERT,
            success=True,
            records_count=0,
            no_access=True,
            error=None,
            duration_seconds=time.time() - start_time
        )

    except Exception as e:
        logger.error(f"[{thread_name}] User {user_id}: ошибка рекламы: {e}")
        return TaskResult(
            user_id=user_id,
            username=username,
            task_type=TaskType.ADVERT,
            success=False,
            records_count=0,
            no_access=False,
            error=str(e),
            duration_seconds=time.time() - start_time
        )


def sync_cost_price(user_id: int, username: str, wb_token: Optional[str] = None) -> TaskResult:
    """
    Синхронизирует таблицу себестоимости:
    1. Загружает nm_id из financial_reports в cost_price
    2. Получает фото карточек из WB Content API и обновляет url_photo
    """
    start_time = time.time()
    thread_name = threading.current_thread().name

    logger.info(
        f"[{thread_name}] User {user_id}: → синхронизация себестоимости из репортов"
    )

    try:
        # ШАГ 1: Загрузка nm_id из financial_reports
        result = load_nm_from_financial_reports_in_cost_price(user_id)

        inserted_count = 0
        if result:
            inserted_count = result['inserted_count']
            logger.info(
                f"[{thread_name}] User {user_id}: "
                f"таблица себестоимости: {inserted_count} новых nm_id"
            )

        # ШАГ 2: Обновление фото из WB Content API
        photos_updated = 0
        if wb_token:
            try:
                client = create_client(wb_token)
                photos = client.get_cards_list(user_id=user_id)

                if photos:
                    photos_updated = update_photos_in_cost_price(user_id, photos)
                    logger.info(
                        f"[{thread_name}] User {user_id}: "
                        f"обновлено фото: {photos_updated}"
                    )
                else:
                    logger.info(
                        f"[{thread_name}] User {user_id}: "
                        f"нет карточек с фото в Content API"
                    )

            except PermissionError:
                logger.warning(
                    f"[{thread_name}] User {user_id}: "
                    f"нет доступа к Content API (scope), фото не обновлены"
                )
            except Exception as e:
                # Ошибка фото не должна ломать всю задачу
                logger.error(
                    f"[{thread_name}] User {user_id}: "
                    f"ошибка получения фото: {e}"
                )

        total_records = inserted_count + photos_updated

        logger.info(
            f"[{thread_name}] User {user_id}: ← Себестоимость синхронизирована: "
            f"{inserted_count} новых nm_id, {photos_updated} фото обновлено"
        )

        return TaskResult(
            user_id=user_id,
            username=username,
            task_type=TaskType.COSTPRICE,
            success=True,
            records_count=total_records,
            no_access=False,
            error=None,
            duration_seconds=time.time() - start_time
        )

    except Exception as e:
        logger.error(f"[{thread_name}] User {user_id}: ошибка себестоимости: {e}")
        return TaskResult(
            user_id=user_id,
            username=username,
            task_type=TaskType.COSTPRICE,
            success=False,
            records_count=0,
            no_access=False,
            error=str(e),
            duration_seconds=time.time() - start_time
        )


# ═══════════════════════════════════════════════════════════════
# ДИСПЕТЧЕР ЗАДАЧ
# ═══════════════════════════════════════════════════════════════

def execute_task(task: SyncTask) -> TaskResult:
    """
    Выполняет одну задачу синхронизации.
    Роутер для разных типов задач.
    """
    if task.task_type == TaskType.REPORTS:
        return sync_user_reports(task.user_id, task.username, task.wb_token)
    elif task.task_type == TaskType.FUNNEL:
        return sync_user_funnel(task.user_id, task.username, task.wb_token)
    elif task.task_type == TaskType.ADVERT:
        return sync_user_advert_stats(task.user_id, task.username, task.wb_token)
    elif task.task_type == TaskType.COSTPRICE:
        return sync_cost_price(task.user_id, task.username, task.wb_token)
    else:
        raise ValueError(f"Unknown task type: {task.task_type}")


# ═══════════════════════════════════════════════════════════════
# ГЛАВНАЯ ФУНКЦИЯ СИНХРОНИЗАЦИИ
# ═══════════════════════════════════════════════════════════════

def sync_all_users() -> dict:
    """
    Основная задача: синхронизирует ВСЕ данные для всех пользователей.

    Порядок выполнения:
    - REPORTS, FUNNEL, ADVERT выполняются параллельно
    - COSTPRICE запускается СРАЗУ после успешного завершения REPORTS
      для конкретного пользователя (не дожидаясь FUNNEL/ADVERT)
    """
    date_from, date_to = calculate_full_period()
    total_days = (date_to - date_from).days + 1

    logger.info("=" * 70)
    logger.info("ЗАПУСК ПОЛНОЙ СИНХРОНИЗАЦИИ")
    logger.info(f"Период: {date_from} - {date_to} ({total_days} дней)")
    logger.info(f"Окружение: {config.WB_ENV.value}")
    logger.info("-" * 70)
    logger.info("НАСТРОЙКИ МНОГОПОТОЧНОСТИ:")
    logger.info(f"  MAX_TOTAL_WORKERS: {config.MAX_TOTAL_WORKERS}")
    logger.info(f"  MAX_WORKERS_PER_TASK_TYPE: {config.MAX_WORKERS_PER_TASK_TYPE}")
    logger.info(f"  PARALLEL_USER_TASKS: {config.PARALLEL_USER_TASKS}")
    logger.info("=" * 70)

    # Статистика
    stats = {
        "users_total": 0,
        "users_success": 0,
        "users_partial": 0,
        "users_failed": 0,
        "tasks_total": 0,
        "tasks_success": 0,
        "tasks_failed": 0,
        # По типам задач
        "reports_inserted": 0,
        "reports_no_access": 0,
        "funnel_inserted": 0,
        "funnel_no_access": 0,
        "advert_inserted": 0,
        "advert_no_access": 0,
        "costprice_inserted": 0,
        # Очистка
        "reports_deleted": 0,
        "funnel_deleted": 0,
        "advert_deleted": 0,
        # Время
        "total_duration_seconds": 0
    }

    start_time = time.time()

    # Получаем пользователей
    users = get_users_with_tokens()
    stats["users_total"] = len(users)

    if not users:
        logger.warning("Нет пользователей с токенами WB")
        return stats

    logger.info(f"Найдено пользователей: {len(users)}")

    # Маппинг user_id -> данные пользователя
    user_map: Dict[int, dict] = {u["user_id"]: u for u in users}

    # ═══════════════════════════════════════════════════════════
    # Создаём начальные задачи (REPORTS, FUNNEL, ADVERT)
    # ═══════════════════════════════════════════════════════════
    initial_tasks: List[SyncTask] = []

    for user in users:
        for task_type in [TaskType.REPORTS, TaskType.FUNNEL, TaskType.ADVERT]:
            initial_tasks.append(SyncTask(
                user_id=user["user_id"],
                username=user["username"],
                wb_token=user["wb_token"],
                task_type=task_type
            ))

    logger.info(
        f"Создано начальных задач: {len(initial_tasks)} "
        f"({len(users)} пользователей × 3 типа)"
    )

    # Результаты по пользователям
    results_by_user: Dict[int, List[TaskResult]] = {}

    # Отслеживаем, для каких пользователей уже запустили COSTPRICE
    costprice_submitted: Set[int] = set()

    # Счётчик задач (включая динамически добавленные COSTPRICE)
    tasks_submitted = len(initial_tasks)
    tasks_completed = 0

    with ThreadPoolExecutor(
            max_workers=config.MAX_TOTAL_WORKERS,
            thread_name_prefix="sync"
    ) as executor:

        # Отправляем начальные задачи
        future_to_task: Dict[Future, SyncTask] = {
            executor.submit(execute_task, task): task
            for task in initial_tasks
        }

        # Обрабатываем результаты по мере готовности
        while future_to_task:
            # Ждём завершения любой задачи
            done_futures = set()
            for future in as_completed(future_to_task):
                done_futures.add(future)
                break  # Обрабатываем по одной, чтобы сразу добавлять COSTPRICE

            for future in done_futures:
                task = future_to_task.pop(future)
                tasks_completed += 1

                try:
                    result = future.result()

                    # Сохраняем результат
                    if result.user_id not in results_by_user:
                        results_by_user[result.user_id] = []
                    results_by_user[result.user_id].append(result)

                    # Обновляем статистику
                    if result.success:
                        stats["tasks_success"] += 1
                    else:
                        stats["tasks_failed"] += 1

                    # Статистика по типам
                    if result.task_type == TaskType.REPORTS:
                        stats["reports_inserted"] += result.records_count
                        if result.no_access:
                            stats["reports_no_access"] += 1

                        # ═══════════════════════════════════════════════
                        # КЛЮЧЕВАЯ ЛОГИКА: запускаем COSTPRICE сразу
                        # после успешного завершения REPORTS
                        # ═══════════════════════════════════════════════
                        if result.success and result.user_id not in costprice_submitted:
                            costprice_submitted.add(result.user_id)
                            user_data = user_map[result.user_id]

                            costprice_task = SyncTask(
                                user_id=result.user_id,
                                username=user_data["username"],
                                wb_token=user_data["wb_token"],
                                task_type=TaskType.COSTPRICE
                            )

                            new_future = executor.submit(execute_task, costprice_task)
                            future_to_task[new_future] = costprice_task
                            tasks_submitted += 1

                            logger.debug(
                                f"User {result.user_id}: COSTPRICE запущен после REPORTS"
                            )

                    elif result.task_type == TaskType.FUNNEL:
                        stats["funnel_inserted"] += result.records_count
                        if result.no_access:
                            stats["funnel_no_access"] += 1

                    elif result.task_type == TaskType.ADVERT:
                        stats["advert_inserted"] += result.records_count
                        if result.no_access:
                            stats["advert_no_access"] += 1

                    elif result.task_type == TaskType.COSTPRICE:
                        stats["costprice_inserted"] += result.records_count

                except Exception as e:
                    stats["tasks_failed"] += 1
                    logger.error(
                        f"Task {task.task_type.value} for user {task.user_id}: "
                        f"Ошибка получения результата: {e}"
                    )

    stats["tasks_total"] = tasks_submitted

    # ═══════════════════════════════════════════════════════════
    # Анализируем результаты по пользователям
    # ═══════════════════════════════════════════════════════════
    for user_id, user_results in results_by_user.items():
        success_count = sum(1 for r in user_results if r.success)
        total_count = len(user_results)

        if success_count == total_count:
            stats["users_success"] += 1
        elif success_count > 0:
            stats["users_partial"] += 1
        else:
            stats["users_failed"] += 1

    # ═══════════════════════════════════════════════════════════
    # Очистка устаревших данных
    # ═══════════════════════════════════════════════════════════
    logger.info("Очистка устаревших данных...")
    stats["reports_deleted"] = cleanup_old_reports()
    stats["funnel_deleted"] = cleanup_old_funnel_data()
    stats["advert_deleted"] = cleanup_old_advert_stats()

    stats["total_duration_seconds"] = round(time.time() - start_time, 2)

    # ═══════════════════════════════════════════════════════════
    # Итоговый отчёт
    # ═══════════════════════════════════════════════════════════
    logger.info("=" * 70)
    logger.info("СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА")
    logger.info("-" * 70)
    logger.info(f"Время выполнения: {stats['total_duration_seconds']} сек.")
    logger.info("-" * 70)
    logger.info("ПОЛЬЗОВАТЕЛИ:")
    logger.info(f"  Всего: {stats['users_total']}")
    logger.info(f"  Успешно (все задачи): {stats['users_success']}")
    logger.info(f"  Частично (часть задач): {stats['users_partial']}")
    logger.info(f"  С ошибками: {stats['users_failed']}")
    logger.info("-" * 70)
    logger.info("ЗАДАЧИ:")
    logger.info(f"  Всего: {stats['tasks_total']}")
    logger.info(f"  Успешно: {stats['tasks_success']}")
    logger.info(f"  С ошибками: {stats['tasks_failed']}")
    logger.info("-" * 70)
    logger.info("ЗАПИСИ ВСТАВЛЕНО/ОБНОВЛЕНО:")
    logger.info(f"  Отчёты: {stats['reports_inserted']}")
    logger.info(f"  Воронка: {stats['funnel_inserted']}")
    logger.info(f"  Реклама: {stats['advert_inserted']}")
    logger.info(f"  Себестоимость: {stats['costprice_inserted']}")
    logger.info("-" * 70)
    logger.info("БЕЗ ДОСТУПА (scope):")
    if stats["reports_no_access"] > 0:
        logger.info(f"  Отчёты: {stats['reports_no_access']}")
    if stats["funnel_no_access"] > 0:
        logger.info(f"  Воронка: {stats['funnel_no_access']}")
    if stats["advert_no_access"] > 0:
        logger.info(f"  Реклама: {stats['advert_no_access']}")
    logger.info("-" * 70)
    logger.info("УДАЛЕНО УСТАРЕВШИХ:")
    logger.info(
        f"  Отчёты: {stats['reports_deleted']}, "
        f"Воронка: {stats['funnel_deleted']}, "
        f"Реклама: {stats['advert_deleted']}"
    )
    logger.info("=" * 70)

    return stats


# ═══════════════════════════════════════════════════════════════
# ОТДЕЛЬНЫЕ ФУНКЦИИ ДЛЯ РУЧНОГО ЗАПУСКА
# ═══════════════════════════════════════════════════════════════

def sync_reports_only() -> dict:
    """Синхронизирует только финансовые отчёты для всех пользователей."""
    logger.info("=" * 70)
    logger.info("ЗАПУСК СИНХРОНИЗАЦИИ: ТОЛЬКО ОТЧЁТЫ")
    logger.info("=" * 70)

    stats = {"users": 0, "success": 0, "failed": 0, "records": 0}
    users = get_users_with_tokens()
    stats["users"] = len(users)

    tasks = [
        SyncTask(
            user_id=u["user_id"],
            username=u["username"],
            wb_token=u["wb_token"],
            task_type=TaskType.REPORTS
        )
        for u in users
    ]

    with ThreadPoolExecutor(
            max_workers=config.MAX_WORKERS_PER_TASK_TYPE,
            thread_name_prefix="reports"
    ) as executor:
        futures = {executor.submit(execute_task, t): t for t in tasks}

        for future in as_completed(futures):
            try:
                result = future.result()
                if result.success:
                    stats["success"] += 1
                    stats["records"] += result.records_count
                else:
                    stats["failed"] += 1
            except Exception as e:
                stats["failed"] += 1
                logger.error(f"Ошибка: {e}")

    cleanup_old_reports()

    logger.info(f"ОТЧЁТЫ: {stats['records']} записей, {stats['success']}/{stats['users']} успешно")
    return stats


def sync_funnel_only() -> dict:
    """Синхронизирует только воронку для всех пользователей."""
    logger.info("=" * 70)
    logger.info("ЗАПУСК СИНХРОНИЗАЦИИ: ТОЛЬКО ВОРОНКА")
    logger.info("=" * 70)

    stats = {"users": 0, "success": 0, "failed": 0, "no_access": 0, "records": 0}
    users = get_users_with_tokens()
    stats["users"] = len(users)

    tasks = [
        SyncTask(
            user_id=u["user_id"],
            username=u["username"],
            wb_token=u["wb_token"],
            task_type=TaskType.FUNNEL
        )
        for u in users
    ]

    with ThreadPoolExecutor(
            max_workers=config.MAX_WORKERS_PER_TASK_TYPE,
            thread_name_prefix="funnel"
    ) as executor:
        futures = {executor.submit(execute_task, t): t for t in tasks}

        for future in as_completed(futures):
            try:
                result = future.result()
                if result.success:
                    stats["success"] += 1
                    stats["records"] += result.records_count
                    if result.no_access:
                        stats["no_access"] += 1
                else:
                    stats['failed'] += 1
            except Exception as e:
                stats["failed"] += 1
                logger.error(f"Ошибка: {e}")

    cleanup_old_funnel_data()

    logger.info(f"ВОРОНКА: {stats['records']} записей, {stats['success']}/{stats['users']} успешно")
    return stats


def sync_advert_only() -> dict:
    """Синхронизирует только рекламу для всех пользователей."""
    logger.info("=" * 70)
    logger.info("ЗАПУСК СИНХРОНИЗАЦИИ: ТОЛЬКО РЕКЛАМА")
    logger.info("=" * 70)

    stats = {"users": 0, "success": 0, "failed": 0, "no_access": 0, "records": 0}
    users = get_users_with_tokens()
    stats["users"] = len(users)

    tasks = [
        SyncTask(
            user_id=u["user_id"],
            username=u["username"],
            wb_token=u["wb_token"],
            task_type=TaskType.ADVERT
        )
        for u in users
    ]

    with ThreadPoolExecutor(
            max_workers=config.MAX_WORKERS_PER_TASK_TYPE,
            thread_name_prefix="advert"
    ) as executor:
        futures = {executor.submit(execute_task, t): t for t in tasks}

        for future in as_completed(futures):
            try:
                result = future.result()
                if result.success:
                    stats["success"] += 1
                    stats["records"] += result.records_count
                    if result.no_access:
                        stats["no_access"] += 1
                else:
                    stats["failed"] += 1
            except Exception as e:
                stats["failed"] += 1
                logger.error(f"Ошибка: {e}")

    cleanup_old_advert_stats()

    logger.info(f"РЕКЛАМА: {stats['records']} записей, {stats['success']}/{stats['users']} успешно")
    return stats


def sync_costprice_only() -> dict:
    """Синхронизирует только себестоимость для всех пользователей."""
    logger.info("=" * 70)
    logger.info("ЗАПУСК СИНХРОНИЗАЦИИ: ТОЛЬКО СЕБЕСТОИМОСТЬ")
    logger.info("=" * 70)

    stats = {"users": 0, "success": 0, "failed": 0, "records": 0}
    users = get_users_with_tokens()
    stats["users"] = len(users)

    tasks = [
        SyncTask(
            user_id=u["user_id"],
            username=u["username"],
            wb_token=u["wb_token"],  # ← ИЗМЕНЕНО: было None
            task_type=TaskType.COSTPRICE
        )
        for u in users
    ]

    with ThreadPoolExecutor(
            max_workers=config.MAX_WORKERS_PER_TASK_TYPE,
            thread_name_prefix="costprice"
    ) as executor:
        futures = {executor.submit(execute_task, t): t for t in tasks}

        for future in as_completed(futures):
            try:
                result = future.result()
                if result.success:
                    stats["success"] += 1
                    stats["records"] += result.records_count
                else:
                    stats["failed"] += 1
            except Exception as e:
                stats["failed"] += 1
                logger.error(f"Ошибка: {e}")

    logger.info(f"СЕБЕСТОИМОСТЬ: {stats['records']} записей, {stats['success']}/{stats['users']} успешно")
    return stats