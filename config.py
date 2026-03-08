"""
Конфигурация приложения из переменных окружения
"""
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from enum import Enum
from typing import Optional


# ═══════════════════════════════════════════════════════════════
# Enum для окружений
# ═══════════════════════════════════════════════════════════════

class WBEnvironment(str, Enum):
    """Окружение Wildberries API"""
    SANDBOX = "sandbox"
    PRODUCTION = "production"


# ═══════════════════════════════════════════════════════════════
# Основная конфигурация
# ═══════════════════════════════════════════════════════════════

class Config(BaseSettings):
    """
    Конфигурация приложения.

    Переменные загружаются в порядке приоритета:
    1. Системные переменные окружения (export VAR=value)
    2. ..env.local (локальные секреты)
    3. .env (шаблон с примерами)
    4. Значения по умолчанию (ниже)
    """

    # ───────────────────────────────────────────────────────────
    # DATABASE
    # ───────────────────────────────────────────────────────────

    DATABASE_URL: str = Field(
        default="postgresql://postgres:password@localhost:5432/wb_analytics",
        description="Строка подключения к PostgreSQL"
    )

    # ───────────────────────────────────────────────────────────
    # WILDBERRIES API
    # ───────────────────────────────────────────────────────────

    WB_ENV: WBEnvironment = Field(
        default=WBEnvironment.SANDBOX,
        description="Окружение WB API: sandbox или production"
    )

    # ───────────────────────────────────────────────────────────
    # SCHEDULER
    # ───────────────────────────────────────────────────────────

    SYNC_INTERVAL_MINUTES: int = Field(
        default=1440,
        ge=1,
        description="Интервал синхронизации в минутах"
    )

    DATA_RETENTION_MONTHS: int = Field(
        default=3,
        ge=1,
        le=12,
        description="Глубина выгрузки данных (месяцы)"
    )

    # ───────────────────────────────────────────────────────────
    # API LIMITS
    # ───────────────────────────────────────────────────────────

    WB_API_MAX_DAYS_PER_REQUEST: int = Field(
        default=31,
        ge=1,
        le=31,
        description="Максимум дней в одном запросе к WB API"
    )

    WB_API_RETRY_DELAY: int = Field(
        default=30,
        ge=1,
        description="Пауза между запросами при 429 (секунды)"
    )

    # ───────────────────────────────────────────────────────────
    # МНОГОПОТОЧНОСТЬ
    # ───────────────────────────────────────────────────────────

    MAX_TOTAL_WORKERS: int = Field(
        default=15,
        ge=1,
        le=50,
        description="Максимальное общее количество потоков"
    )

    MAX_WORKERS_PER_TASK_TYPE: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Максимум потоков на один тип задачи"
    )

    MAX_WORKERS_PER_USER: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Максимум одновременных задач для одного пользователя"
    )

    PARALLEL_USER_TASKS: bool = Field(
        default=True,
        description="Параллельная синхронизация задач пользователя"
    )

    # ───────────────────────────────────────────────────────────
    # LOGGING
    # ───────────────────────────────────────────────────────────

    LOG_LEVEL: str = Field(
        default="INFO",
        description="Уровень логирования: DEBUG, INFO, WARNING, ERROR"
    )

    # ───────────────────────────────────────────────────────────
    # COMPUTED PROPERTIES (вычисляемые на основе WB_ENV)
    # ───────────────────────────────────────────────────────────

    @property
    def WB_API_REPORT_URL(self) -> str:
        """URL для API отчётов"""
        if self.WB_ENV == WBEnvironment.PRODUCTION:
            return "https://statistics-api.wildberries.ru"
        return "https://statistics-api-sandbox.wildberries.ru"

    @property
    def WB_API_FUNNEL_PRODUCT_URL(self) -> str:
        """URL для API воронки продаж"""
        if self.WB_ENV == WBEnvironment.PRODUCTION:
            return "https://seller-analytics-api.wildberries.ru"
        return "https://statistics-api-sandbox.wildberries.ru"

    @property
    def WB_API_ADVERT_URL(self) -> str:
        """URL для API рекламы"""
        return "https://advert-api.wildberries.ru"

    # ───────────────────────────────────────────────────────────
    # VALIDATORS (валидация значений)
    # ───────────────────────────────────────────────────────────

    @field_validator('LOG_LEVEL')
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Проверка допустимых уровней логирования"""
        allowed = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        v_upper = v.upper()
        if v_upper not in allowed:
            raise ValueError(f"LOG_LEVEL должен быть одним из: {allowed}")
        return v_upper

    @field_validator('WB_ENV', mode='before')
    @classmethod
    def validate_wb_env(cls, v) -> WBEnvironment:
        """Преобразование строки в Enum"""
        if isinstance(v, str):
            return WBEnvironment(v.lower())
        return v

    # ───────────────────────────────────────────────────────────
    # SETTINGS CONFIG
    # ───────────────────────────────────────────────────────────

    model_config = SettingsConfigDict(
        env_file=('.env', '.env.production','.env.local'),
        env_file_encoding='utf-8',
        case_sensitive=True,
        extra='ignore'
    )


# ═══════════════════════════════════════════════════════════════
# Singleton instance
# ═══════════════════════════════════════════════════════════════

config = Config()


# ═══════════════════════════════════════════════════════════════
# Утилиты для отладки
# ═══════════════════════════════════════════════════════════════

def print_config() -> None:
    """Выводит текущую конфигурацию (без секретов)"""
    print("=" * 60)
    print("КОНФИГУРАЦИЯ ПРИЛОЖЕНИЯ")
    print("=" * 60)

    print("\n🌐 WILDBERRIES API:")
    print(f"  Окружение: {config.WB_ENV.value}")
    print(f"  Reports URL: {config.WB_API_REPORT_URL}")
    print(f"  Funnel URL: {config.WB_API_FUNNEL_PRODUCT_URL}")
    print(f"  Advert URL: {config.WB_API_ADVERT_URL}")

    print("\n🗄️  DATABASE:")
    # Скрываем пароль
    db_url_safe = config.DATABASE_URL.split('@')[-1] if '@' in config.DATABASE_URL else config.DATABASE_URL
    print(f"  URL: ...@{db_url_safe}")

    print("\n⏱️  SCHEDULER:")
    print(f"  Интервал синхронизации: {config.SYNC_INTERVAL_MINUTES} мин")
    print(f"  Глубина данных: {config.DATA_RETENTION_MONTHS} мес")

    print("\n🔧 API LIMITS:")
    print(f"  Максимум дней/запрос: {config.WB_API_MAX_DAYS_PER_REQUEST}")
    print(f"  Задержка при 429: {config.WB_API_RETRY_DELAY} сек")

    print("\n⚡ МНОГОПОТОЧНОСТЬ:")
    print(f"  Всего потоков: {config.MAX_TOTAL_WORKERS}")
    print(f"  На тип задачи: {config.MAX_WORKERS_PER_TASK_TYPE}")
    print(f"  На пользователя: {config.MAX_WORKERS_PER_USER}")
    print(f"  Параллельные задачи: {config.PARALLEL_USER_TASKS}")

    print("\n📋 LOGGING:")
    print(f"  Уровень: {config.LOG_LEVEL}")

    print("=" * 60)


def validate_config() -> bool:
    """
    Проверяет корректность конфигурации.
    Возвращает True если всё ОК.
    """
    try:
        # Проверка DATABASE_URL
        if not config.DATABASE_URL.startswith('postgresql://'):
            print("❌ DATABASE_URL должен начинаться с 'postgresql://'")
            return False

        # Проверка логических ограничений
        if config.MAX_WORKERS_PER_TASK_TYPE > config.MAX_TOTAL_WORKERS:
            print("❌ MAX_WORKERS_PER_TASK_TYPE не может быть больше MAX_TOTAL_WORKERS")
            return False

        if config.MAX_WORKERS_PER_USER > config.MAX_WORKERS_PER_TASK_TYPE:
            print("❌ MAX_WORKERS_PER_USER не может быть больше MAX_WORKERS_PER_TASK_TYPE")
            return False

        print("✅ Конфигурация валидна")
        return True

    except Exception as e:
        print(f"❌ Ошибка валидации конфигурации: {e}")
        return False


# ═══════════════════════════════════════════════════════════════
# CLI для тестирования конфигурации
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print_config()
    validate_config()