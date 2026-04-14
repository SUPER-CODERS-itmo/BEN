"""
Тесты для скрипта init_admin.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import unittest
import tempfile
import os
import sqlite3
from init_admin import create_demo_admin, hash_password


class TestInitAdmin(unittest.TestCase):
    """Тесты для инициализации демо-администратора"""

    def setUp(self):
        """Создаём временную БД перед каждым тестом"""
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db.close()
        self._create_users_table()

    def tearDown(self):
        """Удаляем временную БД после теста"""
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)

    def _create_users_table(self):
        """Создаёт таблицу users во временной БД"""
        conn = sqlite3.connect(self.temp_db.name)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE users (
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

    def test_create_demo_admin_when_empty(self):
        """Создание демо-администратора в пустой таблице"""
        result = create_demo_admin(self.temp_db.name)
        self.assertTrue(result)

        # Проверяем, что админ появился
        conn = sqlite3.connect(self.temp_db.name)
        cursor = conn.cursor()
        cursor.execute('SELECT username, is_admin FROM users')
        user = cursor.fetchone()
        conn.close()

        self.assertIsNotNone(user)
        self.assertEqual(user[0], "admin")
        self.assertEqual(user[1], 1)

    def test_create_demo_admin_when_not_empty(self):
        """Не создаёт демо-администратора, если таблица не пуста"""
        # Сначала создаём пользователя
        conn = sqlite3.connect(self.temp_db.name)
        cursor = conn.cursor()
        hashed = hash_password("somepass")
        cursor.execute('INSERT INTO users (username, password_hash, is_admin) VALUES (?, ?, ?)',
                       ("existing_user", hashed, 0))
        conn.commit()
        conn.close()

        # Пытаемся создать демо-администратора
        result = create_demo_admin(self.temp_db.name)
        self.assertFalse(result)

        # Проверяем, что админ не создался (всего 1 пользователь)
        conn = sqlite3.connect(self.temp_db.name)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM users')
        count = cursor.fetchone()[0]
        conn.close()

        self.assertEqual(count, 1)

    def test_create_demo_admin_db_not_exists(self):
        """Обработка отсутствия базы данных"""
        result = create_demo_admin("/non/existent/path.db")
        self.assertFalse(result)

    def test_hash_password_function(self):
        """Проверка функции хеширования в init_admin"""
        hash1 = hash_password("test")
        hash2 = hash_password("test")
        self.assertEqual(hash1, hash2)
        self.assertEqual(len(hash1), 64)


if __name__ == '__main__':
    unittest.main()