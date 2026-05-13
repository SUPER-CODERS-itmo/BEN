"""
Тесты для API модуля (backend/api.py)
Запуск: python -m pytest backend/tests/test_api.py -v
"""

import sys
import os
from pathlib import Path

# Добавляем путь к backend
sys.path.insert(0, str(Path(__file__).parent.parent))

import unittest
from unittest.mock import patch, AsyncMock, MagicMock
import json
from fastapi.testclient import TestClient

# Импортируем app из api
from api import app

client = TestClient(app)

# Тестовый токен для авторизации
TEST_TOKEN = "test_token_123"


class TestLoginAPI(unittest.TestCase):
    """Тесты для эндпоинта /login (не требует токена)"""

    @patch('api.authenticate_user')
    @patch('api.create_session')
    def test_login_success(self, mock_create_session, mock_authenticate_user):
        """Успешный вход"""
        mock_authenticate_user.return_value = (True, TEST_TOKEN, {
            'id': 1,
            'username': 'admin',
            'is_admin': True,
            'has_telegram': False
        })

        response = client.post("/login", json={
            "username": "admin",
            "password": "admin123"
        })

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("token", data)
        self.assertEqual(data["token"], TEST_TOKEN)
        self.assertEqual(data["user"]["username"], "admin")

    @patch('api.authenticate_user')
    def test_login_wrong_password(self, mock_authenticate_user):
        """Неверный пароль"""
        mock_authenticate_user.return_value = (False, None, None)

        response = client.post("/login", json={
            "username": "admin",
            "password": "wrong"
        })

        self.assertEqual(response.status_code, 401)

    def test_login_missing_fields(self):
        """Отсутствуют поля"""
        response = client.post("/login", json={"username": "admin"})
        self.assertEqual(response.status_code, 422)


class TestComplaintsAPI(unittest.TestCase):
    """Тесты для эндпоинта /complaints (требует токен)"""

    def setUp(self):
        """Устанавливаем заголовок с токеном"""
        self.headers = {"Authorization": f"Bearer {TEST_TOKEN}"}

    @patch('api.validate_token')
    @patch('api.read_complaints_safe')
    def test_get_complaints_success(self, mock_read_complaints, mock_validate_token):
        """Успешное получение списка жалоб"""
        mock_validate_token.return_value = {"id": 1, "username": "admin"}

        import pandas as pd
        mock_df = pd.DataFrame({
            'userId': ['1', '2', '3'],
            'text': ['Жалоба 1', 'Жалоба 2', 'Жалоба 3'],
            'event_date': ['2025-05-01', '2025-05-02', '2025-05-03']
        })
        mock_read_complaints.return_value = mock_df

        response = client.get("/complaints", headers=self.headers)

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(data), 3)

    @patch('api.validate_token')
    @patch('api.read_complaints_safe')
    def test_get_complaints_with_filters(self, mock_read_complaints, mock_validate_token):
        """Фильтрация по дате"""
        mock_validate_token.return_value = {"id": 1, "username": "admin"}

        import pandas as pd
        mock_df = pd.DataFrame({
            'userId': ['1', '2', '3'],
            'text': ['A', 'B', 'C'],
            'event_date': ['2025-05-01', '2025-05-10', '2025-05-20']
        })
        mock_read_complaints.return_value = mock_df

        response = client.get("/complaints?start_date=2025-05-05&end_date=2025-05-15", headers=self.headers)

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['userId'], '2')


class TestComplaintDetailAPI(unittest.TestCase):
    """Тесты для /complaints/{complaint_id} (требует токен)"""

    def setUp(self):
        self.headers = {"Authorization": f"Bearer {TEST_TOKEN}"}

    @patch('api.validate_token')
    @patch('api.read_complaints_safe')
    def test_get_complaint_by_id_found(self, mock_read_complaints, mock_validate_token):
        """Жалоба найдена"""
        mock_validate_token.return_value = {"id": 1, "username": "admin"}

        import pandas as pd
        mock_df = pd.DataFrame({
            'userId': ['123', '456'],
            'text': ['Текст жалобы 123', 'Текст жалобы 456'],
            'event_date': ['2025-05-01', '2025-05-02']
        })
        mock_read_complaints.return_value = mock_df

        response = client.get("/complaints/123", headers=self.headers)

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data['id'], '123')
        self.assertEqual(data['text'], 'Текст жалобы 123')

    @patch('api.validate_token')
    @patch('api.read_complaints_safe')
    def test_get_complaint_by_id_not_found(self, mock_read_complaints, mock_validate_token):
        """Жалоба не найдена"""
        mock_validate_token.return_value = {"id": 1, "username": "admin"}

        import pandas as pd
        mock_df = pd.DataFrame({
            'userId': ['123'],
            'text': ['Текст'],
            'event_date': ['2025-05-01']
        })
        mock_read_complaints.return_value = mock_df

        response = client.get("/complaints/999", headers=self.headers)

        self.assertEqual(response.status_code, 404)


class TestInvestigateAPI(unittest.TestCase):
    """Тесты для /investigate/{complaint_id} (требует токен)"""

    def setUp(self):
        self.headers = {"Authorization": f"Bearer {TEST_TOKEN}"}

    @patch('api.validate_token')
    @patch('api.FraudInvestigator')
    def test_investigate_success(self, mock_investigator_class, mock_validate_token):
        """Успешное расследование"""
        mock_validate_token.return_value = "operator_01"

        mock_investigator = AsyncMock()
        mock_investigator.investigate_single_case.return_value = json.dumps({
            'fraud_bank_id': 'fraud_123',
            'transaction_info': {'amount': 50000}
        })
        mock_investigator_class.return_value = mock_investigator

        response = client.post("/investigate/123", headers=self.headers)

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data['fraud_bank_id'], 'fraud_123')


class TestFullProfileAPI(unittest.TestCase):
    """Тесты для /full-profile/{bank_id} (требует токен)"""

    def setUp(self):
        self.headers = {"Authorization": f"Bearer {TEST_TOKEN}"}

    @patch('api.validate_token')
    @patch('api.FraudInvestigator')
    def test_full_profile_found(self, mock_investigator_class, mock_validate_token):
        """Профиль найден"""
        mock_validate_token.return_value = {"username": "admin"}

        mock_investigator = AsyncMock()
        mock_investigator.fetch_full_user_profile.return_value = {
            'bank_id': 'bank_123',
            'name': 'Иванов Иван',
            'phone': '+79001234567'
        }
        mock_investigator_class.return_value = mock_investigator

        response = client.get("/full-profile/bank_123", headers=self.headers)

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data['bank_id'], 'bank_123')

    @patch('api.validate_token')
    @patch('api.FraudInvestigator')
    def test_full_profile_not_found(self, mock_investigator_class, mock_validate_token):
        """Профиль не найден"""
        mock_validate_token.return_value = {"username": "admin"}

        mock_investigator = AsyncMock()
        mock_investigator.fetch_full_user_profile.return_value = None
        mock_investigator_class.return_value = mock_investigator

        response = client.get("/full-profile/nonexistent", headers=self.headers)

        self.assertEqual(response.status_code, 404)


if __name__ == '__main__':
    unittest.main(verbosity=2)