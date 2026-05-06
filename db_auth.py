"""
Модуль для создания отдельной базы данных сотрудников.
"""

import sqlite3
from pathlib import Path

DB_AUTH_PATH = 'data/users.db'


def create_users_table():
    """
    Создаёт таблицу users в отдельной БД data/users.db.
    Поля: id, username, password_hash, is_admin, has_telegram, created_at.
    """
    Path('data').mkdir(exist_ok=True)

    conn = sqlite3.connect(DB_AUTH_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            is_admin INTEGER DEFAULT 0,
            has_telegram INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    conn.commit()
    conn.close()
    print(f"Таблица users создана в {DB_AUTH_PATH}")


if __name__ == "__main__":
    create_users_table()