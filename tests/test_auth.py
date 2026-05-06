"""
Тесты для модуля аутентификации auth.py

Запуск:
    pytest tests/test_auth.py -v
    или
    python -m pytest tests/test_auth.py -v
"""

import sys
from pathlib import Path

# Добавляем путь к корню проекта
sys.path.insert(0, str(Path(__file__).parent.parent))

import unittest
import tempfile
import os
import sqlite3
from auth import (
    hash_password, verify_password, create_user, authenticate_user,
    create_session, validate_token, logout, get_all_users, delete_user
)


class TestPasswordHashing(unittest.TestCase):
    """Тесты для хеширования паролей"""

    def test_hash_password_returns_string(self):
        """Хеш пароля должен быть строкой"""
        result = hash_password("test123")
        self.assertIsInstance(result, str)

    def test_hash_password_length(self):
        """Хеш SHA-256 должен быть 64 символа"""
        result = hash_password("test123")
        self.assertEqual(len(result), 64)

    def test_hash_password_same_password_same_hash(self):
        """Одинаковые пароли дают одинаковый хеш"""
        hash1 = hash_password("mypass")
        hash2 = hash_password("mypass")
        self.assertEqual(hash1, hash2)

    def test_hash_password_different_password_different_hash(self):
        """Разные пароли дают разные хеши"""
        hash1 = hash_password("pass1")
        hash2 = hash_password("pass2")
        self.assertNotEqual(hash1, hash2)

    def test_verify_password_correct(self):
        """Проверка правильного пароля"""
        password = "secret123"
        hashed = hash_password(password)
        self.assertTrue(verify_password(password, hashed))

    def test_verify_password_incorrect(self):
        """Проверка неправильного пароля"""
        password = "secret123"
        hashed = hash_password(password)
        self.assertFalse(verify_password("wrong", hashed))


class TestUserManagement(unittest.TestCase):
    """Тесты для управления пользователями"""

    def setUp(self):
        """Создаём временную БД перед каждым тестом"""
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db.close()
        self._create_users_table()
        self.conn = None  # Для хранения соединения

    def tearDown(self):
        """Удаляем временную БД после теста"""
        # Закрываем все соединения
        if self.conn:
            self.conn.close()

        # Принудительно закрываем все соединения с этой БД
        import gc
        gc.collect()

        if os.path.exists(self.temp_db.name):
            try:
                os.unlink(self.temp_db.name)
            except PermissionError:
                # Если файл всё ещё занят, ждём и пробуем снова
                import time
                time.sleep(0.1)
                try:
                    os.unlink(self.temp_db.name)
                except:
                    pass

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

    def test_create_user_success(self):
        """Успешное создание пользователя"""
        success, message = create_user(self.temp_db.name, "testuser", "pass123")
        self.assertTrue(success)
        self.assertIn("успешно", message)

    def test_create_user_duplicate(self):
        """Создание дубликата пользователя должно вернуть ошибку"""
        create_user(self.temp_db.name, "user1", "pass")
        success, message = create_user(self.temp_db.name, "user1", "pass")
        self.assertFalse(success)
        self.assertIn("уже существует", message)

    def test_create_user_with_admin_flag(self):
        """Создание пользователя с правами администратора"""
        success, _ = create_user(self.temp_db.name, "admin_user", "pass", is_admin=True)
        self.assertTrue(success)

        # Проверяем, что флаг is_admin установлен
        conn = sqlite3.connect(self.temp_db.name)
        cursor = conn.cursor()
        cursor.execute('SELECT is_admin FROM users WHERE username = ?', ("admin_user",))
        result = cursor.fetchone()
        conn.close()
        self.assertEqual(result[0], 1)

    def test_create_user_with_telegram_flag(self):
        """Создание пользователя с флагом Telegram"""
        success, _ = create_user(self.temp_db.name, "tg_user", "pass", has_telegram=True)
        self.assertTrue(success)

        conn = sqlite3.connect(self.temp_db.name)
        cursor = conn.cursor()
        cursor.execute('SELECT has_telegram FROM users WHERE username = ?', ("tg_user",))
        result = cursor.fetchone()
        conn.close()
        self.assertEqual(result[0], 1)

    def test_authenticate_user_success(self):
        """Успешная аутентификация"""
        create_user(self.temp_db.name, "alice", "secret")
        success, token, user = authenticate_user(self.temp_db.name, "alice", "secret")
        self.assertTrue(success)
        self.assertIsNotNone(token)
        self.assertEqual(user['username'], "alice")

    def test_authenticate_user_wrong_password(self):
        """Аутентификация с неправильным паролем"""
        create_user(self.temp_db.name, "bob", "correct")
        success, token, user = authenticate_user(self.temp_db.name, "bob", "wrong")
        self.assertFalse(success)
        self.assertIsNone(token)
        self.assertIsNone(user)

    def test_authenticate_user_not_exists(self):
        """Аутентификация несуществующего пользователя"""
        success, token, user = authenticate_user(self.temp_db.name, "nobody", "pass")
        self.assertFalse(success)

    def test_get_all_users(self):
        """Получение списка всех пользователей"""
        create_user(self.temp_db.name, "user_a", "pass")
        create_user(self.temp_db.name, "user_b", "pass")
        users = get_all_users(self.temp_db.name)
        self.assertEqual(len(users), 2)
        usernames = [u['username'] for u in users]
        self.assertIn("user_a", usernames)
        self.assertIn("user_b", usernames)

    def test_delete_user_success(self):
        """Успешное удаление пользователя"""
        create_user(self.temp_db.name, "todelete", "pass")
        users_before = get_all_users(self.temp_db.name)
        self.assertEqual(len(users_before), 1)

        success, message = delete_user(self.temp_db.name, users_before[0]['id'])
        self.assertTrue(success)

        users_after = get_all_users(self.temp_db.name)
        self.assertEqual(len(users_after), 0)

    def test_delete_user_not_exists(self):
        """Удаление несуществующего пользователя"""
        success, message = delete_user(self.temp_db.name, 999)
        self.assertFalse(success)
        self.assertIn("не найден", message)


class TestSessionTokens(unittest.TestCase):
    """Тесты для токенов сессий"""

    def setUp(self):
        self.token = "test_token_123"
        self.user_data = {
            'id': 1,
            'username': 'testuser',
            'is_admin': False,
            'has_telegram': False
        }

    def tearDown(self):
        """Очищаем токены после тестов"""
        from auth import _active_tokens
        _active_tokens.clear()

    def test_create_and_validate_session(self):
        """Создание и проверка сессии"""
        create_session(self.token, self.user_data)
        user = validate_token(self.token)
        self.assertIsNotNone(user)
        self.assertEqual(user['username'], 'testuser')

    def test_validate_invalid_token(self):
        """Проверка несуществующего токена"""
        user = validate_token("fake_token")
        self.assertIsNone(user)

    def test_logout_removes_token(self):
        """Выход удаляет токен"""
        create_session(self.token, self.user_data)
        self.assertIsNotNone(validate_token(self.token))

        logout(self.token)
        self.assertIsNone(validate_token(self.token))

    def test_logout_invalid_token(self):
        """Выход с несуществующим токеном"""
        result = logout("fake_token")
        self.assertFalse(result)

    def test_token_expiry(self):
        """Проверка истечения токена (с моком времени)"""
        from auth import _active_tokens
        from datetime import datetime, timedelta

        # Создаём токен с истекшим временем
        expired_at = datetime.now() - timedelta(hours=1)
        _active_tokens[self.token] = (self.user_data, expired_at)

        user = validate_token(self.token)
        self.assertIsNone(user)
        self.assertNotIn(self.token, _active_tokens)


if __name__ == '__main__':
    unittest.main()