"""
Задачи для выполнения по расписанию.

ОПТИМИЗАЦИЯ ПАМЯТИ:
- Streaming обработка (не накапливаем данные)
- Явная очистка через del и gc.collect()
- Ограниченный параллелизм

ЛОГИКА СИНХРОНИЗАЦИИ:
- Всегда перезагружаем ВЕСЬ период (DATA_RETENTION_MONTHS месяцев)
- Используем UPSERT для обновления изменённых записей
"""
from datetime import date, timedelta
from typing import Optional, Dict, List, Set
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from dataclasses import dataclass
from enum import Enum
import threading
import time
import gc


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
    wb_token: Optional[str]
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


def force_gc():
    """Принудительная сборка мусора"""
    gc.collect()


# ═══════════════════════════════════════════════════════════════
# ФУНКЦИИ СИНХРОНИЗАЦИИ — ОПТИМИЗИРОВАННЫЕ ПО ПАМЯТИ
# ═══════════════════════════════════════════════════════════════

def sync_user_reports(user_id: int, username: str, wb_token: str) -> TaskResult:
    """
    Синхронизирует финансовые отчёты для одного пользователя.
    STREAMING: получаем данные порциями и сразу вставляем в БД.
    """
    start_time = time.time()
    thread_name = threading.current_thread().name
    date_from, date_to = calculate_full_period()

    logger.info(
        f"[{thread_name}] User {user_id}: → Отчёты за {date_from} - {date_to}"
    )

    total_inserted = 0

    try:
        client = create_client(wb_token)

        # Получаем отчёты (API сам разбивает на интервалы)
        reports = client.get_financial_reports(date_from, date_to, user_id=user_id)

        if reports:
            # Вставляем сразу, не храним в памяти
            inserted = insert_financial_reports(user_id, reports)
            total_inserted = inserted

            logger.info(
                f"[{thread_name}] User {user_id}: ← Отчёты синхронизированы: {inserted}"
            )
        else:
            logger.info(f"[{thread_name}] User {user_id}: нет данных отчётов")

        # ⚡ ЯВНАЯ ОЧИСТКА ПАМЯТИ
        del reports
        del client
        force_gc()

        return TaskResult(
            user_id=user_id,
            username=username,
            task_type=TaskType.REPORTS,
            success=True,
            records_count=total_inserted,
            no_access=False,
            error=None,
            duration_seconds=time.time() - start_time
        )

    except Exception as e:
        logger.error(f"[{thread_name}] User {user_id}: ошибка отчётов: {e}")
        force_gc()
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
    """
    Синхронизирует воронку продаж для одного пользователя.
    STREAMING: каждую порцию сразу вставляем в БД, не накапливаем.
    """
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
                # ⚡ Сразу освобождаем
                del products
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
                    # ⚡ STREAMING: вставляем сразу, не накапливаем
                    inserted = insert_funnel_products(user_id, products, extract_both_periods=True)
                    total_inserted += inserted
                    # ⚡ Сразу освобождаем память
                    del products

                current_date += timedelta(days=2)

                # ⚡ Периодическая сборка мусора (каждые 10 запросов)
                if requests_count % 10 == 0:
                    force_gc()

            # Последняя дата
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
                    del products

        logger.info(
            f"[{thread_name}] User {user_id}: ← Воронка синхронизирована: "
            f"{total_inserted} (запросов: {requests_count})"
        )

        # ⚡ Финальная очистка
        del client
        force_gc()

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
        force_gc()
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
        force_gc()
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
    """
    Синхронизирует рекламную статистику для одного пользователя.
    """
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
            del client
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

        inserted = 0
        if stats:
            inserted = insert_advert_stats(user_id, stats)
            logger.info(
                f"[{thread_name}] User {user_id}: ← Реклама синхронизирована: {inserted}"
            )
        else:
            logger.info(f"[{thread_name}] User {user_id}: нет данных рекламы")

        # ⚡ ЯВНАЯ ОЧИСТКА
        del stats
        del advert_ids
        del client
        force_gc()

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
        force_gc()
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
        force_gc()
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
    Синхронизирует таблицу себестоимости.
    """
    start_time = time.time()
    thread_name = threading.current_thread().name

    logger.info(
        f"[{thread_name}] User {user_id}: → синхронизация себестоимости"
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

                # ⚡ ОЧИСТКА
                del photos
                del client

            except PermissionError:
                logger.warning(
                    f"[{thread_name}] User {user_id}: "
                    f"нет доступа к Content API, фото не обновлены"
                )
            except Exception as e:
                logger.error(
                    f"[{thread_name}] User {user_id}: "
                    f"ошибка получения фото: {e}"
                )

        total_records = inserted_count + photos_updated

        force_gc()

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
        force_gc()
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
    """Выполняет одну задачу синхронизации."""
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
# ГЛАВНАЯ ФУНКЦИЯ — ОПТИМИЗИРОВАННАЯ ПО ПАМЯТИ
# ═══════════════════════════════════════════════════════════════

def sync_all_users() -> dict:
    """
    Основная задача: синхронизирует ВСЕ данные для всех пользователей.

    ОПТИМИЗАЦИЯ ПАМЯТИ:
    - Ограниченный параллелизм (меньше потоков = меньше памяти)
    - Не храним все результаты — только статистику
    - Принудительная сборка мусора после каждого пользователя
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
    logger.info("=" * 70)

    # Статистика (только счётчики, не храним объекты)
    stats = {
        "users_total": 0,
        "users_success": 0,
        "users_partial": 0,
        "users_failed": 0,
        "tasks_total": 0,
        "tasks_success": 0,
        "tasks_failed": 0,
        "reports_inserted": 0,
        "reports_no_access": 0,
        "funnel_inserted": 0,
        "funnel_no_access": 0,
        "advert_inserted": 0,
        "advert_no_access": 0,
        "costprice_inserted": 0,
        "reports_deleted": 0,
        "funnel_deleted": 0,
        "advert_deleted": 0,
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

    # ⚡ ОПТИМИЗАЦИЯ: Уменьшаем параллелизм для экономии памяти
    # На 1 GB RAM лучше не более 3-5 потоков
    max_workers = min(config.MAX_TOTAL_WORKERS, 3)
    logger.info(f"Используем {max_workers} потоков (оптимизация памяти)")

    # Отслеживаем успешность по пользователям (только счётчики)
    user_task_counts: Dict[int, Dict[str, int]] = {}
    costprice_submitted: Set[int] = set()

    # Создаём начальные задачи
    initial_tasks: List[SyncTask] = []
    for user in users:
        user_id = user["user_id"]
        user_task_counts[user_id] = {"total": 0, "success": 0}

        for task_type in [TaskType.REPORTS, TaskType.FUNNEL, TaskType.ADVERT]:
            initial_tasks.append(SyncTask(
                user_id=user_id,
                username=user["username"],
                wb_token=user["wb_token"],
                task_type=task_type
            ))
            user_task_counts[user_id]["total"] += 1

    stats["tasks_total"] = len(initial_tasks)
    logger.info(f"Создано задач: {len(initial_tasks)}")

    # Маппинг user_id -> wb_token (для COSTPRICE)
    user_tokens = {u["user_id"]: u for u in users}

    # ⚡ Освобождаем users — больше не нужен
    del users
    force_gc()

    with ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix="sync"
    ) as executor:

        future_to_task: Dict[Future, SyncTask] = {
            executor.submit(execute_task, task): task
            for task in initial_tasks
        }

        # ⚡ Освобождаем initial_tasks
        del initial_tasks

        while future_to_task:
            for future in as_completed(future_to_task):
                task = future_to_task.pop(future)

                try:
                    result = future.result()

                    # Обновляем статистику (без хранения объектов)
                    if result.success:
                        stats["tasks_success"] += 1
                        user_task_counts[result.user_id]["success"] += 1
                    else:
                        stats["tasks_failed"] += 1

                    # Статистика по типам
                    if result.task_type == TaskType.REPORTS:
                        stats["reports_inserted"] += result.records_count
                        if result.no_access:
                            stats["reports_no_access"] += 1

                        # Запускаем COSTPRICE после успешного REPORTS
                        if result.success and result.user_id not in costprice_submitted:
                            costprice_submitted.add(result.user_id)
                            user_data = user_tokens[result.user_id]

                            costprice_task = SyncTask(
                                user_id=result.user_id,
                                username=user_data["username"],
                                wb_token=user_data["wb_token"],
                                task_type=TaskType.COSTPRICE
                            )

                            new_future = executor.submit(execute_task, costprice_task)
                            future_to_task[new_future] = costprice_task
                            stats["tasks_total"] += 1
                            user_task_counts[result.user_id]["total"] += 1

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

                    # ⚡ Явно удаляем результат
                    del result

                except Exception as e:
                    stats["tasks_failed"] += 1
                    logger.error(f"Task error: {e}")

                # ⚡ Сборка мусора после каждой задачи
                force_gc()

                break  # Обрабатываем по одной

    # Анализируем результаты по пользователям
    for user_id, counts in user_task_counts.items():
        if counts["success"] == counts["total"]:
            stats["users_success"] += 1
        elif counts["success"] > 0:
            stats["users_partial"] += 1
        else:
            stats["users_failed"] += 1

    # ⚡ Очищаем
    del user_task_counts
    del user_tokens
    del costprice_submitted
    force_gc()

    # Очистка устаревших данных (последовательно для экономии памяти)
    logger.info("Очистка устаревших данных...")

    stats["reports_deleted"] = cleanup_old_reports()
    force_gc()

    stats["funnel_deleted"] = cleanup_old_funnel_data()
    force_gc()

    stats["advert_deleted"] = cleanup_old_advert_stats()
    force_gc()

    stats["total_duration_seconds"] = round(time.time() - start_time, 2)

    # Итоговый отчёт
    logger.info("=" * 70)
    logger.info("СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА")
    logger.info("-" * 70)
    logger.info(f"Время выполнения: {stats['total_duration_seconds']} сек.")
    logger.info("-" * 70)
    logger.info("ПОЛЬЗОВАТЕЛИ:")
    logger.info(f"  Всего: {stats['users_total']}")
    logger.info(f"  Успешно: {stats['users_success']}")
    logger.info(f"  Частично: {stats['users_partial']}")
    logger.info(f"  С ошибками: {stats['users_failed']}")
    logger.info("-" * 70)
    logger.info("ЗАДАЧИ:")
    logger.info(f"  Всего: {stats['tasks_total']}")
    logger.info(f"  Успешно: {stats['tasks_success']}")
    logger.info(f"  С ошибками: {stats['tasks_failed']}")
    logger.info("-" * 70)
    logger.info("ЗАПИСЕЙ:")
    logger.info(f"  Отчёты: {stats['reports_inserted']}")
    logger.info(f"  Воронка: {stats['funnel_inserted']}")
    logger.info(f"  Реклама: {stats['advert_inserted']}")
    logger.info(f"  Себестоимость: {stats['costprice_inserted']}")
    logger.info("-" * 70)
    logger.info("УДАЛЕНО:")
    logger.info(
        f"  Отчёты: {stats['reports_deleted']}, "
        f"Воронка: {stats['funnel_deleted']}, "
        f"Реклама: {stats['advert_deleted']}"
    )
    logger.info("=" * 70)

    return stats


# ═══════════════════════════════════════════════════════════════
# УНИВЕРСАЛЬНАЯ ФУНКЦИЯ ДЛЯ ОТДЕЛЬНЫХ ТИПОВ ЗАДАЧ
# ═══════════════════════════════════════════════════════════════

def sync_single_task_type(task_type: TaskType) -> dict:
    """Синхронизирует один тип задач для всех пользователей."""
    logger.info("=" * 70)
    logger.info(f"ЗАПУСК: {task_type.value.upper()}")
    logger.info("=" * 70)

    stats = {"users": 0, "success": 0, "failed": 0, "no_access": 0, "records": 0}

    users = get_users_with_tokens()
    stats["users"] = len(users)

    if not users:
        logger.warning("Нет пользователей")
        return stats

    tasks = [
        SyncTask(
            user_id=u["user_id"],
            username=u["username"],
            wb_token=u["wb_token"],
            task_type=task_type
        )
        for u in users
    ]

    del users
    force_gc()

    # ⚡ Ограниченный параллелизм
    max_workers = min(config.MAX_WORKERS_PER_TASK_TYPE, 3)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(execute_task, t): t for t in tasks}

        del tasks

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
                del result
            except Exception as e:
                stats["failed"] += 1
                logger.error(f"Ошибка: {e}")

            force_gc()

    # Cleanup
    cleanup_map = {
        TaskType.REPORTS: cleanup_old_reports,
        TaskType.FUNNEL: cleanup_old_funnel_data,
        TaskType.ADVERT: cleanup_old_advert_stats,
    }

    if task_type in cleanup_map:
        cleanup_map[task_type]()
        force_gc()

    logger.info(f"{task_type.value.upper()}: {stats['records']} записей")
    return stats


def sync_reports_only() -> dict:
    return sync_single_task_type(TaskType.REPORTS)


def sync_funnel_only() -> dict:
    return sync_single_task_type(TaskType.FUNNEL)


def sync_advert_only() -> dict:
    return sync_single_task_type(TaskType.ADVERT)


def sync_costprice_only() -> dict:
    return sync_single_task_type(TaskType.COSTPRICE)