"""
Тесты для скрипта create_user.py (только функции валидации)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import unittest
from unittest.mock import patch, MagicMock


class TestUsernameValidation(unittest.TestCase):
    """Тесты для проверки логина"""

    def test_valid_username(self):
        """Проверка корректных логинов"""
        from create_user import validate_username

        valid_usernames = ["alice", "user_123", "admin", "ivan_ivanov", "a" * 30]
        for username in valid_usernames:
            is_valid, error = validate_username(username)
            self.assertTrue(is_valid, f"Логин '{username}' должен быть валидным")

    def test_empty_username(self):
        """Пустой логин"""
        from create_user import validate_username
        is_valid, error = validate_username("")
        self.assertFalse(is_valid)
        self.assertIn("пустым", error)

    def test_short_username(self):
        """Слишком короткий логин"""
        from create_user import validate_username
        is_valid, error = validate_username("ab")
        self.assertFalse(is_valid)
        self.assertIn("минимум 3", error)

    def test_long_username(self):
        """Слишком длинный логин"""
        from create_user import validate_username
        is_valid, error = validate_username("a" * 31)
        self.assertFalse(is_valid)
        self.assertIn("максимум 30", error)

    def test_username_starts_with_digit(self):
        """Логин не может начинаться с цифры"""
        from create_user import validate_username
        is_valid, error = validate_username("1alice")
        self.assertFalse(is_valid)
        self.assertIn("начинаться с цифры", error)

    def test_username_invalid_chars(self):
        """Логин с запрещёнными символами"""
        from create_user import validate_username
        invalid_usernames = ["alice!", "user@name", "ivan#", "alice space"]
        for username in invalid_usernames:
            is_valid, error = validate_username(username)
            self.assertFalse(is_valid, f"Логин '{username}' должен быть невалидным")


class TestPasswordValidation(unittest.TestCase):
    """Тесты для проверки пароля"""

    def test_valid_password(self):
        """Проверка корректных паролей"""
        from create_user import validate_password

        valid_passwords = ["pass123", "12345678", "longpassword", "a" * 8]
        for password in valid_passwords:
            is_valid, warning = validate_password(password)
            self.assertTrue(is_valid, f"Пароль '{password}' должен быть валидным")

    def test_empty_password(self):
        """Пустой пароль"""
        from create_user import validate_password
        is_valid, error = validate_password("")
        self.assertFalse(is_valid)
        self.assertIn("пустым", error)

    def test_short_password(self):
        """Слишком короткий пароль (менее 4 символов)"""
        from create_user import validate_password
        is_valid, error = validate_password("123")
        self.assertFalse(is_valid)
        self.assertIn("минимум 4", error)

    def test_weak_password_warning(self):
        """Предупреждение для слабого пароля (4-7 символов)"""
        from create_user import validate_password
        is_valid, warning = validate_password("1234")
        self.assertTrue(is_valid)
        self.assertIn("Рекомендуется", warning)


if __name__ == '__main__':
    unittest.main()