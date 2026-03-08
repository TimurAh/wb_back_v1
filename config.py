"""
Конфигурация приложения из переменных окружения
"""
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from enum import Enum
from typing import Optional
import os

# ═══════════════════════════════════════════════════════════════
# ОТЛАДКА — ДОБАВЬ ЭТО В НАЧАЛО
# ═══════════════════════════════════════════════════════════════

print("═" * 60)
print("ОТЛАДКА ЗАГРУЗКИ КОНФИГУРАЦИИ")
print("═" * 60)
print(f"Текущая директория: {os.getcwd()}")
print(f"Файлы .env в директории:")
for f in ['.env', '.env.local', '.env.production']:
    exists = os.path.exists(f)
    print(f"  {f}: {'✓ найден' if exists else '✗ не найден'}")

    # Если файл существует — покажем первые строки
    if exists:
        try:
            with open(f, 'r', encoding='utf-8') as file:
                lines = file.readlines()[:3]  # Первые 3 строки
                for line in lines:
                    # Скрываем значения (показываем только ключи)
                    if '=' in line and not line.strip().startswith('#'):
                        key = line.split('=')[0]
                        print(f"    → {key}=***")
        except Exception as e:
            print(f"    Ошибка чтения файла: {e}")

encryption_key_from_env = os.getenv('ENCRYPTION_KEY', 'НЕ УСТАНОВЛЕН')
if encryption_key_from_env != 'НЕ УСТАНОВЛЕН':
    print(f"ENCRYPTION_KEY из os.getenv: {encryption_key_from_env[:10]}...")
else:
    print(f"ENCRYPTION_KEY из os.getenv: НЕ УСТАНОВЛЕН")
print("═" * 60)

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

    # ← ВАЖНО: Сделай поле необязательным с дефолтом для отладки
    ENCRYPTION_KEY: Optional[str] = Field(
        default=None,
        description="Ключ шифрования для токенов (Fernet)"
    )

    LOG_LEVEL: str = Field(
        default="INFO",
        description="Уровень логирования: DEBUG, INFO, WARNING, ERROR"
    )

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

# ← ДОБАВЬ ОТЛАДКУ ПОСЛЕ СОЗДАНИЯ
print("ПОСЛЕ СОЗДАНИЯ CONFIG:")
if config.ENCRYPTION_KEY:
    print(f"✅ config.ENCRYPTION_KEY загружен: {config.ENCRYPTION_KEY[:20]}...")
else:
    print("❌ config.ENCRYPTION_KEY = None (НЕ ЗАГРУЖЕН)")
print("═" * 60)


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

    print("\n🗄️  DATABASE:")
    db_url_safe = config.DATABASE_URL.split('@')[-1] if '@' in config.DATABASE_URL else config.DATABASE_URL
    print(f"  URL: ...@{db_url_safe}")

    print("\n⏱️  SCHEDULER:")
    print(f"  Интервал синхронизации: {config.SYNC_INTERVAL_MINUTES} мин")

    print("\n⚡ МНОГОПОТОЧНОСТЬ:")
    print(f"  Всего потоков: {config.MAX_TOTAL_WORKERS}")

    print("\n🔐 ШИФРОВАНИЕ:")
    if config.ENCRYPTION_KEY:
        print(f"  ENCRYPTION_KEY: {config.ENCRYPTION_KEY[:20]}...")
    else:
        print(f"  ENCRYPTION_KEY: ❌ НЕ УСТАНОВЛЕН")

    print("\n📋 LOGGING:")
    print(f"  Уровень: {config.LOG_LEVEL}")
    print("=" * 60)


def validate_config() -> bool:
    """Проверяет корректность конфигурации"""
    try:
        if not config.DATABASE_URL.startswith('postgresql://'):
            print("❌ DATABASE_URL должен начинаться с 'postgresql://'")
            return False

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


if __name__ == "__main__":
    print_config()
    validate_config()