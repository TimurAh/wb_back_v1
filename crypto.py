"""
Шифрование токенов для безопасного хранения
"""
from cryptography.fernet import Fernet
from typing import Optional


# Глобальная переменная для кеширования ключа
_encryption_key: Optional[bytes] = None


def set_encryption_key(key: str) -> None:
    """
    Устанавливает ключ шифрования из конфигурации.

    Вызывается один раз при старте приложения из config.

    Args:
        key: Ключ шифрования в формате base64
    """
    global _encryption_key
    _encryption_key = key.encode()


def get_encryption_key() -> bytes:
    """
    Получает ключ шифрования.

    Returns:
        bytes: Ключ шифрования

    Raises:
        ValueError: Если ключ не установлен
    """
    if _encryption_key is None:
        raise ValueError(
            "ENCRYPTION_KEY не установлен!\n"
            "Вызовите set_encryption_key(config.ENCRYPTION_KEY) при старте приложения."
        )

    return _encryption_key


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
    return encrypted.decode()


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

    decrypted = fernet.decrypt(encrypted_token.encode())
    return decrypted.decode()


__all__ = ['encrypt_token', 'decrypt_token', 'set_encryption_key']