"""
Подключение к PostgreSQL
"""
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from contextlib import contextmanager
from typing import Generator, Any
from config import config
from utils import logger


@contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Context manager для подключения к БД"""
    conn = None
    try:
        conn = psycopg2.connect(config.DATABASE_URL)
        yield conn
    except psycopg2.Error as e:
        logger.error(f"Ошибка подключения к БД: {e}")
        raise
    finally:
        if conn:
            conn.close()


@contextmanager
def get_cursor(commit: bool = False) -> Generator[psycopg2.extensions.cursor, None, None]:
    """Context manager для курсора с опциональным коммитом"""
    with get_connection() as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cursor
            if commit:
                conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка выполнения запроса: {e}")
            raise
        finally:
            cursor.close()


def test_connection() -> bool:
    """Проверка подключения к БД"""
    try:
        with get_cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            logger.info("✓ Подключение к БД успешно")
            return result is not None
    except Exception as e:
        logger.error(f"✗ Ошибка подключения к БД: {e}")
        return False
