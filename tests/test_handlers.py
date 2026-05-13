import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# Импорты хэндлеров и состояний
from handlers.auth import cmd_start, on_password, cmd_logout, LoginFSM
from handlers.admin import show_users, AddUserFSM
from handlers.cases import show_complaints, InvestigateFSM

class TestBotHandlers(unittest.IsolatedAsyncioTestCase):
    """Юнит-тесты для хэндлеров бота на базе unittest."""

    def setUp(self):
        # Моки для зависимостей
        self.users_db = MagicMock()
        self.api_client = AsyncMock()
        self.state = AsyncMock()
        
        # Мок сообщения
        self.message = AsyncMock()
        self.message.from_user.id = 12345
        self.message.from_user.username = "test_user"
        self.message.chat.id = 12345
        self.message.answer = AsyncMock()
        self.message.delete = AsyncMock()

    # --- Тесты AUTH (auth.py) ---

    async def test_cmd_start_new_user(self):
        """Тест /start: новый пользователь должен быть переведен в состояние ввода логина."""
        self.users_db.get_by_telegram.return_value = None
        
        await cmd_start(self.message, self.state, self.users_db)
        
        self.state.set_state.assert_called_with(LoginFSM.login)
        self.assertIn("Введите логин", self.message.answer.call_args[0][0])

    async def test_cmd_logout_logic(self):
        """Тест выхода: проверка отвязки TG ID и очистки состояний."""
        self.users_db.get_by_telegram.return_value = {"username": "admin_user"}
        
        await cmd_logout(self.message, self.state, self.users_db)
        
        self.users_db.unlink_telegram.assert_called_with("12345")
        self.state.clear.assert_called_once()
        self.assertIn("До свидания", self.message.answer.call_args[0][0])

    # --- Тесты ADMIN (admin.py) ---

    async def test_show_users_access_denied(self):
        """Тест защиты админки: обычный оператор не должен видеть список пользователей."""
        # Имитируем, что пользователь не админ (is_admin=0)
        self.users_db.get_by_telegram.return_value = {"username": "op", "is_admin": 0}
        
        await show_users(self.message, self.users_db)
        
        self.message.answer.assert_called_with("⛔️ Доступ запрещён.")

    async def test_show_users_access_granted(self):
        """Тест админки: админ должен видеть список."""
        self.users_db.get_by_telegram.return_value = {"username": "admin", "is_admin": 1}
        self.users_db.get_all.return_value = []
        
        # Патчим форматировщик, так как он требует реальных данных
        with patch('bot.handlers.admin.fmt_user_list', return_value="Список юзеров"):
            await show_users(self.message, self.users_db)
            self.users_db.get_all.assert_called_once()

    # --- Тесты CASES (cases.py) ---

    async def test_show_complaints_not_authenticated(self):
        """Тест: неавторизованный пользователь получает предупреждение."""
        self.users_db.get_by_telegram.return_value = None
        
        await show_complaints(self.message, self.api_client, self.users_db)
        
        self.assertIn("Сначала войдите", self.message.answer.call_args[0][0])

    async def test_on_id_input_menu_cancel(self):
        """Тест: нажатие кнопок меню во время FSM должно отменять расследование."""
        self.message.text = "📋 Жалобы"
        
        from bot.handlers.cases import on_id_input
        await on_id_input(self.message, self.state, self.api_client, self.users_db)
        
        self.state.clear.assert_called_once()
        self.assertIn("Расследование отменено", self.message.answer.call_args[0][0])

if __name__ == '__main__':
    unittest.main()