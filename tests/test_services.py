import unittest
import sqlite3
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from services.db import UsersDB
from services.api_client import BenAPIClient
from services.formatter import fmt_user_list, _escape_md
from services.poller import FraudPoller

# --- 1. Тесты для базы данных (db.py) ---

class TestUsersDB(unittest.TestCase):
    def setUp(self):
        # Создаем объект БД
        self.db = UsersDB(":memory:")
        
        # Создаем одно постоянное соединение для теста
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row
        
        # Создаем структуру таблицы
        self.connection.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                tg_username TEXT,
                telegram_id TEXT UNIQUE,
                password TEXT NOT NULL,
                is_admin INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        self.connection.commit()

        # Класс-заглушка для имитации контекстного менеджера "with self._conn()"
        class MockContextManager:
            def __init__(self, conn):
                self.conn = conn
            def __enter__(self):
                return self.conn
            def __exit__(self, exc_type, exc_val, exc_tb):
                pass

        # Подменяем метод у конкретного экземпляра
        self.db._conn = lambda: MockContextManager(self.connection)

    def tearDown(self):
        self.connection.close()

    def test_add_and_authenticate_user(self):
        """Проверка создания пользователя и успешной авторизации."""
        self.db.add_user("admin", "pass123", is_admin=True)
        
        user = self.db.authenticate("admin", "pass123")
        self.assertIsNotNone(user)
        self.assertEqual(user["username"], "admin")
        self.assertEqual(int(user["is_admin"]), 1)

    def test_authenticate_fail(self):
        """Проверка, что неверный пароль или логин не проходят авторизацию."""
        self.db.add_user("operator", "secret")
        
        # Неверный пароль
        self.assertIsNone(self.db.authenticate("operator", "wrong_pass"))
        # Несуществующий пользователь
        self.assertIsNone(self.db.authenticate("unknown", "secret"))

    def test_link_telegram(self):
        """Проверка привязки и поиска по Telegram ID."""
        self.db.add_user("user1", "pass")
        self.db.link_telegram("user1", "999888")
        
        user = self.db.get_by_telegram("999888")
        self.assertIsNotNone(user)
        self.assertEqual(user["username"], "user1")

    def test_delete_user(self):
        """Проверка удаления пользователя."""
        self.db.add_user("to_delete", "123")
        
        # Удаляем
        result = self.db.delete_user("to_delete")
        self.assertTrue(result)
        
        # Проверяем, что в базе его нет
        user = self.db.authenticate("to_delete", "123")
        self.assertIsNone(user)

# --- 2. Тесты для API клиента (api_client.py) ---

class TestBenAPIClient(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = BenAPIClient("http://fake-api.com", "token123")

    @patch("httpx.AsyncClient.get")
    async def test_get_complaints(self, mock_get):
        """Проверка получения списка жалоб через API."""
        # Мокаем ответ от httpx
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"userId": "V1", "text": "Fraud"}]
        mock_get.return_value = mock_response

        res = await self.client.get_complaints(limit=5)
        
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["userId"], "V1")
        mock_get.assert_called_once()
        # Проверяем, что параметры ушли верно
        args, kwargs = mock_get.call_args
        self.assertEqual(kwargs["params"]["limit"], 5)

# --- 3. Тесты для форматировщика (formatter.py) ---

class TestFormatter(unittest.TestCase):
    def test_escape_md(self):
        """Проверка экранирования спецсимволов Markdown."""
        text = "Hello *World* _Test_"
        escaped = _escape_md(text)
        self.assertIn("\\*", escaped)
        self.assertIn("\\_", escaped)

    def test_fmt_user_list_empty(self):
        """Проверка форматирования пустого списка пользователей."""
        result = fmt_user_list([])
        self.assertEqual(result, "Пользователей нет.")

    def test_fmt_user_list_with_data(self):
        """Проверка форматирования списка с данными."""
        users = [
            {"username": "admin", "is_admin": 1, "telegram_id": "123", "tg_username": "adm"},
            {"username": "op", "is_admin": 0, "telegram_id": None, "tg_username": None}
        ]
        result = fmt_user_list(users)
        self.assertIn("admin", result)
        self.assertIn("🟢", result) # Привязан
        self.assertIn("🔴", result) # Не привязан

# --- 4. Тесты для поллера (poller.py) ---

class TestFraudPoller(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.mock_bot = AsyncMock()
        self.mock_api = AsyncMock()
        self.mock_db = MagicMock()
        self.poller = FraudPoller(self.mock_bot, self.mock_api, self.mock_db)

    @patch("services.poller.fmt_investigation", return_value="Alert!")
    async def test_process_case_notification(self, mock_fmt):
        """Тест логики обработки одного кейса и рассылки."""
        # Данные для теста
        complaint_id = "V123"
        fraud_id = "F456"
        inv_data = {"fraud_bank_id": fraud_id, "transaction_info": {}}
        
        # Настраиваем моки
        self.mock_api.get_calls.return_value = []
        self.mock_api.get_delivery.return_value = {}
        self.mock_db.get_notifiable.return_value = [{"username": "admin", "telegram_id": "111"}]
        
        # Вызываем внутренний метод обработки кейса (если он доступен)
        # В poller.py это часть логики внутри цикла, протестируем _broadcast напрямую
        await self.poller._broadcast("Test message")
        
        # Проверяем, что бот пытался отправить сообщение
        self.mock_bot.send_message.assert_called_once_with(
            "111", "Test message", parse_mode="Markdown"
        )

if __name__ == "__main__":
    unittest.main()