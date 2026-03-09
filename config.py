"""
Конфигурация приложения из переменных окружения
"""
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from enum import Enum
from typing import Optional
import os


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
    """

    DATABASE_URL: str = Field(
        default="postgresql://postgres:password@localhost:5432/wb_analytics",
        description="Строка подключения к PostgreSQL"
    )

    WB_ENV: WBEnvironment = Field(
        default=WBEnvironment.SANDBOX,
        description="Окружение WB API: sandbox или production"
    )

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

    ENCRYPTION_KEY: Optional[str] = Field(
        default=None,
        description="Ключ шифрования для токенов (Fernet)"
    )

    LOG_LEVEL: str = Field(
        default="INFO",
        description="Уровень логирования: DEBUG, INFO, WARNING, ERROR"
    )

    PORT: int = Field(
        default=8000,
        ge=1,
        le=65535,
        description="Порт HTTP сервера"
    )

    CORS_ORIGINS: list[str] = Field(
        default=["*"],
        description="Разрешённые origins для CORS"
    )

    # ─────────────────────────────────────────────────────────
    # Валидаторы
    # ─────────────────────────────────────────────────────────

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

    @model_validator(mode='after')
    def validate_worker_limits(self) -> 'Config':
        """Кросс-валидация лимитов воркеров"""
        if self.MAX_WORKERS_PER_TASK_TYPE > self.MAX_TOTAL_WORKERS:
            raise ValueError(
                f"MAX_WORKERS_PER_TASK_TYPE ({self.MAX_WORKERS_PER_TASK_TYPE}) "
                f"не может быть > MAX_TOTAL_WORKERS ({self.MAX_TOTAL_WORKERS})"
            )
        if self.MAX_WORKERS_PER_USER > self.MAX_WORKERS_PER_TASK_TYPE:
            raise ValueError(
                f"MAX_WORKERS_PER_USER ({self.MAX_WORKERS_PER_USER}) "
                f"не может быть > MAX_WORKERS_PER_TASK_TYPE ({self.MAX_WORKERS_PER_TASK_TYPE})"
            )
        return self

    # ─────────────────────────────────────────────────────────
    # Properties
    # ─────────────────────────────────────────────────────────

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

    model_config = SettingsConfigDict(
        env_file=('.env', '.env.production', '.env.local'),
        env_file_encoding='utf-8',
        case_sensitive=True,
        extra='ignore'
    )


# ═══════════════════════════════════════════════════════════════
# Singleton instance
# ═══════════════════════════════════════════════════════════════

config = Config()


# ═══════════════════════════════════════════════════════════════
# Отладка (только при LOG_LEVEL=DEBUG)
# ═══════════════════════════════════════════════════════════════

def _debug_env_loading():
    """Выводит отладочную информацию о загрузке конфигурации"""
    print("═" * 60)
    print("ОТЛАДКА ЗАГРУЗКИ КОНФИГУРАЦИИ")
    print("═" * 60)
    print(f"Текущая директория: {os.getcwd()}")
    print(f"Файлы .env в директории:")

    for f in ['.env', '.env.local', '.env.production']:
        exists = os.path.exists(f)
        print(f"  {f}: {'✓ найден' if exists else '✗ не найден'}")

        if exists:
            try:
                with open(f, 'r', encoding='utf-8') as file:
                    lines = file.readlines()[:3]
                    for line in lines:
                        if '=' in line and not line.strip().startswith('#'):
                            key = line.split('=')[0]
                            print(f"    → {key}=***")
            except Exception as e:
                print(f"    Ошибка чтения файла: {e}")

    if config.ENCRYPTION_KEY:
        print(f"ENCRYPTION_KEY: {config.ENCRYPTION_KEY[:5]}...")
    else:
        print("ENCRYPTION_KEY: НЕ УСТАНОВЛЕН")
    print("═" * 60)


# Вызываем только при DEBUG
if config.LOG_LEVEL == "DEBUG":
    _debug_env_loading()


# ═══════════════════════════════════════════════════════════════
# Утилиты
# ═══════════════════════════════════════════════════════════════

def print_config() -> None:
    """Выводит текущую конфигурацию (без секретов)"""
    print("=" * 60)
    print("КОНФИГУРАЦИЯ ПРИЛОЖЕНИЯ")
    print("=" * 60)

    print("\n🌐 WILDBERRIES API:")
    print(f"  Окружение: {config.WB_ENV.value}")
    print(f"  Reports URL: {config.WB_API_REPORT_URL}")

    print("\n🗄️  DATABASE:")
    db_url_safe = config.DATABASE_URL.split('@')[-1] if '@' in config.DATABASE_URL else config.DATABASE_URL
    print(f"  URL: ...@{db_url_safe}")

    print("\n⏱️  SCHEDULER:")
    print(f"  Интервал синхронизации: {config.SYNC_INTERVAL_MINUTES} мин")

    print("\n⚡ МНОГОПОТОЧНОСТЬ:")
    print(f"  Всего потоков: {config.MAX_TOTAL_WORKERS}")

    print("\n🔐 ШИФРОВАНИЕ:")
    if config.ENCRYPTION_KEY:
        print(f"  ENCRYPTION_KEY: {config.ENCRYPTION_KEY[:5]}...")
    else:
        print(f"  ENCRYPTION_KEY: ❌ НЕ УСТАНОВЛЕН")

    print("\n📋 LOGGING:")
    print(f"  Уровень: {config.LOG_LEVEL}")

    print("\n🌐 SERVER:")
    print(f"  Порт: {config.PORT}")
    print("=" * 60)


if __name__ == "__main__":
    print_config()