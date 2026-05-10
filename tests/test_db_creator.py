import unittest
from unittest.mock import patch
import sqlite3
import pandas as pd
import numpy as np

from db_creator import normalize_phone, DataPopulator, COMPLAINTS_TSV


class TestNormalizePhone(unittest.TestCase):
    """Тесты для функции normalize_phone."""

    def test_valid_strings(self):
        """Проверка работы со строками, содержащими номера телефонов."""
        self.assertEqual(normalize_phone("+7 (999) 123-45-67"), "79991234567")
        self.assertEqual(normalize_phone("8-800-555-35-35"), "88005553535")
        self.assertEqual(normalize_phone("79991234567"), "79991234567")

    def test_numeric_inputs(self):
        """Проверка работы с числами (int, float)."""
        self.assertEqual(normalize_phone(79991234567), "79991234567")
        self.assertEqual(normalize_phone(123.45), "12345")

    def test_empty_and_nan(self):
        """Проверка работы с пустыми значениями и NaN."""
        self.assertIsNone(normalize_phone(None))
        self.assertIsNone(normalize_phone(pd.NA))
        self.assertIsNone(normalize_phone(np.nan))
        self.assertIsNone(normalize_phone(float('nan')))

    def test_no_digits(self):
        """Проверка строк, в которых вообще нет цифр."""
        self.assertEqual(normalize_phone("hello world"), "")
        self.assertEqual(normalize_phone("!@#$%^&*()"), "")


class TestDataPopulator(unittest.TestCase):
    """Тесты для класса DataPopulator."""

    def setUp(self):
        """Инициализация перед каждым тестом. Используем БД в оперативной памяти."""
        self.db_path = ':memory:'
        self.populator = DataPopulator(self.db_path)

    def tearDown(self):
        """Очистка после каждого теста."""
        self.populator.close()

    def test_db_connection(self):
        """Проверка успешного подключения к БД."""
        self.assertIsInstance(self.populator.conn, sqlite3.Connection)
        self.assertIsInstance(self.populator.cursor, sqlite3.Cursor)

    def test_setup_schema(self):
        """Проверка правильности создания таблиц в БД."""
        self.populator.setup_schema()

        self.populator.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in self.populator.cursor.fetchall()]

        expected_tables = [
            'unified_users', 'bank_clients', 'bank_transactions',
            'market_place_delivery', 'mobile_build', 'mobile_clients',
            'ecosystem_mapping'
        ]

        for table in expected_tables:
            self.assertIn(table, tables, f"Таблица {table} не была создана")

    @patch('pandas.DataFrame.to_csv')
    def test_generate_data(self, mock_to_csv):
        """Проверка процесса генерации данных и наполнения таблиц."""
        self.populator.setup_schema()

        n_users = 50
        n_frauds = 5

        self.populator.generate_data(n_users=n_users, n_frauds=n_frauds)

        cursor = self.populator.cursor

        cursor.execute("SELECT COUNT(*) FROM bank_clients;")
        self.assertEqual(cursor.fetchone()[0], n_users)

        cursor.execute("SELECT COUNT(*) FROM unified_users;")
        self.assertEqual(cursor.fetchone()[0], n_users)

        cursor.execute("SELECT COUNT(*) FROM ecosystem_mapping;")
        self.assertEqual(cursor.fetchone()[0], n_users)

        expected_transactions = n_frauds + (n_users * 2)
        cursor.execute("SELECT COUNT(*) FROM bank_transactions;")
        self.assertEqual(cursor.fetchone()[0], expected_transactions)

        expected_calls = n_frauds + (n_users * 2)
        cursor.execute("SELECT COUNT(*) FROM mobile_build;")
        self.assertEqual(cursor.fetchone()[0], expected_calls)

        cursor.execute("SELECT COUNT(*) FROM market_place_delivery;")
        market_count = cursor.fetchone()[0]
        self.assertGreater(market_count, 0)
        self.assertLessEqual(market_count, n_users)

        mock_to_csv.assert_called_once()

        args, kwargs = mock_to_csv.call_args
        self.assertEqual(args[0], COMPLAINTS_TSV)
        self.assertEqual(kwargs['sep'], '\t')
        self.assertFalse(kwargs['index'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
