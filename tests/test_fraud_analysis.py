import unittest
from unittest.mock import patch
import pandas as pd
import numpy as np
import json
import os

from backend.fraud_analysis import AmountExtractor, EcosystemDB, FraudInvestigator


class TestAmountExtractor(unittest.TestCase):
    """Тесты для извлечения суммы из текста."""

    def setUp(self):
        self.extractor = AmountExtractor()

    def test_extract_valid_amounts(self):
        self.assertEqual(self.extractor.extract("Перевел 5000 руб мошенникам"), 5000)
        self.assertEqual(self.extractor.extract("Сумма 10.000 ₽"), 10000)
        self.assertEqual(self.extractor.extract("Потерял 1 500р"), 1500)
        self.assertEqual(self.extractor.extract("500 р."), 500)

    def test_extract_invalid_formats(self):
        self.assertIsNone(self.extractor.extract("Просто текст без денег"))
        self.assertIsNone(self.extractor.extract("Цена 500 долларов"))
        self.assertIsNone(self.extractor.extract(None))
        self.assertIsNone(self.extractor.extract(np.nan))

    def test_extract_edge_cases(self):
        # Проверка на пустые строки и некорректные типы
        self.assertIsNone(self.extractor.extract(""))
        self.assertEqual(self.extractor.extract("0 руб"), 0)


class TestEcosystemDB(unittest.IsolatedAsyncioTestCase):
    """Асинхронные тесты для взаимодействия с БД."""

    async def asyncSetUp(self):
        # Создаем временную БД в памяти для каждого теста
        self.db_path = ":memory:"
        self.db = EcosystemDB(self.db_path)
        await self.db.connect()

        # Создаем минимально необходимую схему
        await self.db.conn.executescript("""
            CREATE TABLE bank_clients (userId TEXT, fio TEXT, account TEXT);
            CREATE TABLE bank_transactions (account_out TEXT, account_in TEXT, value INTEGER, event_date TEXT);
            CREATE TABLE unified_users (bank_id TEXT, account TEXT, unique_id TEXT, fio_bank TEXT, 
                                       phone_bank TEXT, phone_mobile TEXT, address TEXT, 
                                       marketplace_id TEXT, mobile_id TEXT);

            INSERT INTO bank_clients VALUES ('V1', 'Victim Name', 'ACC_V');
            INSERT INTO bank_clients VALUES ('F1', 'Fraud Name', 'ACC_F');
            INSERT INTO bank_transactions VALUES ('ACC_V', 'ACC_F', 5000, '2023-10-27 10:00:00');
        """)

    async def asyncTearDown(self):
        await self.db.close()

    async def test_find_transaction_success(self):
        row = await self.db.find_transaction('V1', 5000)
        self.assertIsNotNone(row)
        self.assertEqual(row['victim_name'], 'Victim Name')
        self.assertEqual(row['fraud_bank_id'], 'F1')

    async def test_find_transaction_not_found(self):
        # Неверная сумма
        row = await self.db.find_transaction('V1', 9999)
        self.assertIsNone(row)

    async def test_get_user_profile_data_not_exists(self):
        data = await self.db.get_user_profile_data('NON_EXISTENT')
        self.assertIsNone(data)


class TestFraudInvestigator(unittest.IsolatedAsyncioTestCase):
    """Тесты бизнес-логики расследователя."""

    def setUp(self):
        # Создаем фиктивный TSV файл для тестов
        self.complaints_path = 'test_complaints.tsv'
        df = pd.DataFrame({
            'userId': ['V1'],
            'text': ['Украли 5000 руб'],
            'event_date': ['2023-10-27']
        })
        df.to_csv(self.complaints_path, sep='\t', index=False)

        self.db_path = ":memory:"
        self.investigator = FraudInvestigator(self.db_path, self.complaints_path)

    def tearDown(self):
        if os.path.exists(self.complaints_path):
            os.remove(self.complaints_path)

    def test_generate_tags_logic(self):
        # Подготовка данных для проверки тегов
        user_row = {
            'address': 'г. Москва, ул. Ленина',
            'account': 'ACC123',
            'marketplace_id': 'MKT_10',  # Четное -> Wildberries
            'mobile_id': 'MOB_00'  # 00 % 4 = 0 -> МТС
        }
        transfers = [
            {'account_in': 'ACC123', 'value': 20000},
            {'account_in': 'ACC123', 'value': 40000},  # Итого 60000 -> Крупная кража
        ]

        tags = self.investigator._generate_tags(user_row, transfers)

        self.assertIn('Москва', tags)
        self.assertIn('Крупная кража', tags)
        self.assertIn('Wildberries', tags)
        self.assertIn('МТС', tags)

    @patch('backend.fraud_analysis.EcosystemDB.connect')
    @patch('backend.fraud_analysis.EcosystemDB.get_user_profile_data')
    async def test_fetch_full_user_profile(self, mock_get_data, mock_connect):
        # Мокаем возврат данных из БД
        mock_get_data.return_value = {
            'user': {
                'unique_id': 'U1', 'fio_bank': 'Иван', 'phone_bank': '7999',
                'address': 'г. Сочи', 'account': 'ACC1', 'marketplace_id': '1',
                'bank_id': 'B1', 'phone_mobile': '7999', 'mobile_id': 'M1'
            },
            'victims': [{'userId': 'V1', 'author_name': 'Алексей'}],
            'transfers': [{'event_date': '2023', 'value': 100, 'account_out': '1', 'account_in': '2'}],
            'calls': [],
            'orders': []
        }

        profile = await self.investigator.fetch_full_user_profile('B1')

        self.assertEqual(profile['status'], "Мошенник")
        self.assertEqual(profile['name'], "Иван")
        self.assertEqual(profile['threat'], "_high")
        self.assertTrue(any("Сочи" in tag for tag in profile['tags']))

    async def test_investigate_single_case_no_amount(self):
        # Создаем жалобу без суммы
        with patch.object(self.investigator, 'complaints_df',
                          pd.DataFrame({'userId': ['V2'], 'text': ['Просто жалоба'], 'event_date': ['2023']})):
            result_json = await self.investigator.investigate_single_case('V2')
            result = json.loads(result_json)
            self.assertIn('error', result)
            self.assertEqual(result['error'], "Amount extraction failed")


if __name__ == '__main__':
    unittest.main()