"""
Шифрование токенов для безопасного хранения
"""
from cryptography.fernet import Fernet
import os
import base64


# ═══════════════════════════════════════════════════════════════
# Генерация ключа шифрования
# ═══════════════════════════════════════════════════════════════

def generate_encryption_key() -> bytes:
    """
    Генерирует ключ шифрования.

    ВАЖНО: Вызови один раз, сохрани ключ в переменную окружения!

    Returns:
        bytes: Ключ шифрования в формате base64
    """
    return Fernet.generate_key()


# ═══════════════════════════════════════════════════════════════
# Получение ключа из переменной окружения
# ═══════════════════════════════════════════════════════════════

def get_encryption_key() -> bytes:
    """
    Получает ключ шифрования из переменной окружения.

    Returns:
        bytes: Ключ шифрования

    Raises:
        ValueError: Если ключ не установлен
    """
    key = os.getenv('ENCRYPTION_KEY')

    if not key:
        raise ValueError(
            "ENCRYPTION_KEY не установлен в переменных окружения!\n"
            "Сгенерируй ключ:\n"
            "  python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'\n"
            "И добавь в .env.local:\n"
            "  ENCRYPTION_KEY=сгенерированный_ключ"
        )

    return key.encode()


# ═══════════════════════════════════════════════════════════════
# Шифрование и расшифровка
# ═══════════════════════════════════════════════════════════════

def encrypt_token(token: str) -> str:
    """
    Шифрует токен для хранения в БД.

    Args:
        token: Оригинальный токен WB API

    Returns:
        str: Зашифрованный токен (base64)
    """
    key = get_encryption_key()
    fernet = Fernet(key)

    encrypted = fernet.encrypt(token.encode())
    return base64.urlsafe_b64encode(encrypted).decode()


def decrypt_token(encrypted_token: str) -> str:
    """
    Расшифровывает токен из БД.

    Args:
        encrypted_token: Зашифрованный токен из БД

    Returns:
        str: Оригинальный токен WB API
    """
    key = get_encryption_key()
    fernet = Fernet(key)

    decoded = base64.urlsafe_b64decode(encrypted_token.encode())
    decrypted = fernet.decrypt(decoded)
    return decrypted.decode()