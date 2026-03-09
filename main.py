"""
Точка входа приложения.
═══════════════════════════════════════════════════════════════

Запуск:
    python main.py

Что делает:
    1. Запускает FastAPI сервер (для API эндпоинтов)
    2. Запускает фоновый планировщик (для синхронизации с WB)
"""

import sys
import os
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import config
from database import test_connection,init_database
from scheduler import sync_all_users
from api import router as api_router
from utils import logger
from crypto import set_encryption_key

# ═══════════════════════════════════════════════════════════════
# Инициализация ключа шифрования ← ДОБАВЛЕНО
# ═══════════════════════════════════════════════════════════════

if config.ENCRYPTION_KEY:
    set_encryption_key(config.ENCRYPTION_KEY)
    logger.info("✓ Ключ шифрования инициализирован")
else:
    logger.warning("⚠️ ENCRYPTION_KEY не установлен — шифрование токенов недоступно")

# ═══════════════════════════════════════════════════════════════
# Глобальный планировщик
# ═══════════════════════════════════════════════════════════════

scheduler = BackgroundScheduler()


# ═══════════════════════════════════════════════════════════════
# Lifespan — события старта и остановки FastAPI
# ═══════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Управление жизненным циклом приложения.
    """

    # ─────────────────────────────────────────────────────────
    # STARTUP
    # ─────────────────────────────────────────────────────────

    logger.info("=" * 60)
    logger.info("WB Financial Reports Sync Service")
    logger.info("=" * 60)
    logger.info(f"Окружение: {config.WB_ENV.value}")
    logger.info(f"API URL: {config.WB_API_REPORT_URL}")
    logger.info(f"Интервал синхронизации: {config.SYNC_INTERVAL_MINUTES} мин.")
    logger.info("=" * 60)

    # Проверка БД
    if not test_connection():
        logger.error("Не удалось подключиться к БД!")
        sys.exit(1)
    # ← ДОБАВЛЕНО: Инициализация БД
    logger.info("Инициализация структуры БД...")
    if not init_database():
        logger.error("Не удалось инициализировать БД!")
        sys.exit(1)
    # Настройка планировщика
    scheduler.add_job(
        sync_all_users,
        trigger=IntervalTrigger(minutes=config.SYNC_INTERVAL_MINUTES),
        id="sync_financial_reports",
        name="Синхронизация WB",
        replace_existing=True,
        max_instances=1,
        coalesce=True
    )

    # Запуск планировщика
    scheduler.start()
    logger.info(f"Планировщик запущен (интервал: {config.SYNC_INTERVAL_MINUTES} мин.)")

    # Первичная синхронизация в фоне
    def initial_sync():
        logger.info("Запуск первичной синхронизации в фоне...")
        try:
            sync_all_users()
            logger.info("✓ Первичная синхронизация завершена успешно")
        except Exception as e:
            logger.error(f"Ошибка первичной синхронизации: {e}", exc_info=True)

    sync_thread = threading.Thread(target=initial_sync, daemon=True)
    sync_thread.start()

    logger.info("FastAPI сервер готов к работе")
    logger.info("Документация: http://localhost:8000/docs")

    # ─────────────────────────────────────────────────────────
    # YIELD
    # ─────────────────────────────────────────────────────────

    yield

    # ─────────────────────────────────────────────────────────
    # SHUTDOWN
    # ─────────────────────────────────────────────────────────

    logger.info("Остановка сервиса...")
    scheduler.shutdown(wait=False)
    logger.info("Сервис остановлен")


# ═══════════════════════════════════════════════════════════════
# Создание FastAPI приложения
# ═══════════════════════════════════════════════════════════════

app = FastAPI(
    title="WB Sync Service",
    description="Сервис синхронизации данных с Wildberries API",
    version="1.0.0",
    lifespan=lifespan
)


# ═══════════════════════════════════════════════════════════════
# CORS
# ═══════════════════════════════════════════════════════════════

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ═══════════════════════════════════════════════════════════════
# Подключение роутов
# ═══════════════════════════════════════════════════════════════

app.include_router(api_router, prefix="/api", tags=["API"])


# ═══════════════════════════════════════════════════════════════
# Корневой эндпоинт
# ═══════════════════════════════════════════════════════════════

@app.get("/", tags=["Система"])
async def root():
    """Информация о сервисе"""
    return {
        "service": "WB Sync Service",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": {
            "user_load_info": "/api/user_load_info",
            "health": "/api/health"
        }
    }


# ═══════════════════════════════════════════════════════════════
# Точка входа
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=config.PORT,
        reload=False,
        log_level="info"
    )