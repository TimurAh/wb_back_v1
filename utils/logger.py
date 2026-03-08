"""
Настройка логирования
"""
import logging
import sys
import os
from logging.handlers import RotatingFileHandler
from config import config


def setup_logger(name: str = "wb_service") -> logging.Logger:
    """Создаёт и настраивает логгер"""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, config.LOG_LEVEL.upper(), logging.INFO))

    # Формат логов
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Убираем дублирование
    if not logger.handlers:
        # Файл
        if not os.path.exists('logs'):
            os.mkdir('logs')

        file_handler = RotatingFileHandler(
            'logs/app.log',
            maxBytes=5 * 1024 * 1024,
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Консоль
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # ─────────────────────────────────────────────
        # Handler для консоли (оставляем, если нужен)
        # ─────────────────────────────────────────────
        # console_handler = logging.StreamHandler(sys.stdout)
        # console_handler.setFormatter(formatter)
        # logger.addHandler(console_handler)

    return logger


# Глобальный логгер
logger = setup_logger()