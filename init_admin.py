"""
Модуль для инициализации демо-администратора в системе.

Запуск:
    python init_admin.py

Назначение:
    Создаёт первого администратора в таблице users, если таблица пуста.
    Демо-администратор: логин 'admin', пароль 'admin123'
"""
import sqlite3
import hashlib
import os
from pathlib import Path

# Конфигурация
DB_PATH = 'data/ecosystem_data.db'
SALT = "bankguard_salt_2024"


def hash_password(password: str) -> str:
    """
    Хеширует пароль с использованием SHA-256 и соли.

    Args:
        password: Пароль в открытом виде

    Returns:
        Хешированный пароль
    """
    return hashlib.sha256((password + SALT).encode()).hexdigest()


def create_demo_admin(db_path: str = DB_PATH) -> bool:
    """
    Создаёт демо-администратора, если таблица users пуста.

    Args:
        db_path: Путь к файлу базы данных SQLite

    Returns:
        True если администратор был создан, False если уже существовал или ошибка

    Example:
        >>> create_demo_admin()
        Создан демо-администратор: admin / admin123
          ВНИМАНИЕ: Измените пароль после первого входа!
        True
    """
    # Проверяем, существует ли база данных
    if not os.path.exists(db_path):
        print(f" База данных не найдена: {db_path}")
        print("   Запустите сначала python db_creator.py")
        return False

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Проверяем, существует ли таблица users
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='users'
        """)
        if not cursor.fetchone():
            print(" Таблица users не существует")
            print(" Запустите сначала python db_creator.py")
            conn.close()
            return False

        # Проверяем, есть ли уже пользователи
        cursor.execute('SELECT COUNT(*) FROM users')
        count = cursor.fetchone()[0]

        if count == 0:
            password_hash = hash_password("admin123")
            cursor.execute('''
                INSERT INTO users (username, password_hash, is_admin, has_telegram)
                VALUES (?, ?, ?, ?)
            ''', ('admin', password_hash, 1, 0))
            conn.commit()
            print(" Создан демо-администратор: admin / admin123")
            print("Измените пароль после первого входа!")
            conn.close()
            return True
        else:
            print(f"Таблица users не пуста ({count} записей), демо-администратор не создан")
            conn.close()
            return False

    except Exception as e:
        print(f"Ошибка при создании демо-администратора: {e}")
        return False


if __name__ == "__main__":
    """
    Запуск скрипта для создания демо-администратора.

    Использование:
        python init_admin.py
    """
    create_demo_admin()