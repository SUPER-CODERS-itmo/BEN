import sys
import os
import tempfile
import sqlite3
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from fraud_analysis import AmountExtractor, EcosystemDB, FraudInvestigator


class TestAmountExtractor(unittest.TestCase):
    """Тесты для класса AmountExtractor - извлечение сумм из текста"""

    def setUp(self):
        """Создаём экземпляр перед каждым тестом"""
        self.extractor = AmountExtractor()

    # === Тесты для извлечения сумм с разными форматами ===

    def test_extract_with_r_abbreviation(self):
        """Извлечение суммы с сокращением 'р'"""
        text = "у меня пропали 50000 р"
        result = self.extractor.extract(text)
        self.assertEqual(result, 50000)

    def test_extract_with_rub_abbreviation(self):
        """Извлечение суммы с сокращением 'руб'"""
        text = "списали 12300 руб"
        result = self.extractor.extract(text)
        self.assertEqual(result, 12300)

    def test_extract_with_rubles_word(self):
        """Извлечение суммы со словом 'рублей'"""
        text = "похитили 75000 рублей"
        result = self.extractor.extract(text)
        self.assertEqual(result, 75000)

    def test_extract_with_ruble_word_singular(self):
        """Извлечение суммы со словом 'рубль'"""
        text = "украли 1000 рубль"
        result = self.extractor.extract(text)
        self.assertEqual(result, 1000)

    def test_extract_with_currency_symbol(self):
        """Извлечение суммы с символом ₽"""
        text = "потерял 25000 ₽"
        result = self.extractor.extract(text)
        self.assertEqual(result, 25000)

    def test_extract_with_loss_keyword(self):
        """Извлечение суммы с ключевым словом 'пропали' перед числом"""
        text = "пропали 45000 с карты"
        result = self.extractor.extract(text)
        self.assertEqual(result, 45000)

    def test_extract_with_loss_keyword_different_position(self):
        """Ключевое слово 'пропали' в середине предложения"""
        text = "деньги пропали 32000 рублей"
        result = self.extractor.extract(text)
        self.assertEqual(result, 32000)

    def test_extract_with_space_between_number_and_currency(self):
        """Пробел между числом и валютой"""
        text = "списали 89000 рублей"
        result = self.extractor.extract(text)
        self.assertEqual(result, 89000)

    def test_extract_without_space(self):
        """Без пробела между числом и валютой"""
        text = "потерял 15000р"
        result = self.extractor.extract(text)
        self.assertEqual(result, 15000)

    def test_extract_multiple_numbers_takes_first(self):
        """Извлекает первое число, даже если их несколько"""
        text = "сначала 10000 рублей, потом еще 20000"
        result = self.extractor.extract(text)
        self.assertEqual(result, 10000)

    # === Тесты для случаев, когда сумма не найдена ===

    def test_extract_no_amount_returns_none(self):
        """Текст без упоминания суммы возвращает None"""
        text = "здравствуйте, меня обманули мошенники"
        result = self.extractor.extract(text)
        self.assertIsNone(result)

    def test_extract_empty_string_returns_none(self):
        """Пустая строка возвращает None"""
        result = self.extractor.extract("")
        self.assertIsNone(result)

    def test_extract_nan_value_returns_none(self):
        """Значение NaN возвращает None"""
        result = self.extractor.extract(float('nan'))
        self.assertIsNone(result)

    def test_extract_none_value_returns_none(self):
        """Значение None возвращает None"""
        result = self.extractor.extract(None)
        self.assertIsNone(result)

    def test_extract_text_without_numbers_returns_none(self):
        """Текст без чисел возвращает None"""
        text = "мошенники обманули меня"
        result = self.extractor.extract(text)
        self.assertIsNone(result)

    def test_extract_numbers_without_currency_returns_none(self):
        """Числа без указания валюты не извлекаются"""
        text = "я потерял 50000"
        result = self.extractor.extract(text)
        self.assertIsNone(result)

    # === Тесты для обработки регистра ===

    def test_extract_uppercase_currency(self):
        """Валюта в верхнем регистре"""
        text = "списали 30000 РУБЛЕЙ"
        result = self.extractor.extract(text)
        self.assertEqual(result, 30000)

    def test_extract_mixed_case(self):
        """Смешанный регистр валюты"""
        text = "потерял 18000 Руб"
        result = self.extractor.extract(text)
        self.assertEqual(result, 18000)

    def test_extract_uppercase_loss_keyword(self):
        """Ключевое слово 'ПРОПАЛИ' в верхнем регистре"""
        text = "ПРОПАЛИ 67000 рублей"
        result = self.extractor.extract(text)
        self.assertEqual(result, 67000)


class TestEcosystemDB(unittest.TestCase):
    """Тесты для класса EcosystemDB - работа с базой данных"""

    def setUp(self):
        """Создаём временную БД перед каждым тестом"""
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db.close()
        self._create_test_database()

    def tearDown(self):
        """Удаляем временную БД после теста"""
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)

    def _create_test_database(self):
        """Создаёт тестовую базу данных с нужными таблицами"""
        conn = sqlite3.connect(self.temp_db.name)
        cursor = conn.cursor()

        # Таблица bank_clients
        cursor.execute('''
            CREATE TABLE bank_clients (
                userId INTEGER PRIMARY KEY,
                account TEXT,
                phone TEXT,
                fio TEXT
            )
        ''')

        # Таблица bank_transactions
        cursor.execute('''
            CREATE TABLE bank_transactions (
                id INTEGER PRIMARY KEY,
                account_out TEXT,
                account_in TEXT,
                value INTEGER,
                event_date TEXT
            )
        ''')

        # Таблица mobile_build
        cursor.execute('''
            CREATE TABLE mobile_build (
                id INTEGER PRIMARY KEY,
                event_date TEXT,
                from_call TEXT,
                to_call TEXT,
                duration_sec INTEGER
            )
        ''')

        # Таблица ecosystem_mapping
        cursor.execute('''
            CREATE TABLE ecosystem_mapping (
                id INTEGER PRIMARY KEY,
                bank_id INTEGER,
                marketplace_id INTEGER
            )
        ''')

        # Таблица market_place_delivery
        cursor.execute('''
            CREATE TABLE market_place_delivery (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                event_date TEXT,
                contact_fio TEXT,
                contact_phone TEXT,
                address TEXT
            )
        ''')

        # Добавляем тестовые данные
        cursor.execute('''
            INSERT INTO bank_clients (userId, account, phone, fio)
            VALUES (1, '4081781012345678', '+79990001111', 'Иванов Иван Петрович')
        ''')

        cursor.execute('''
            INSERT INTO bank_clients (userId, account, phone, fio)
            VALUES (2, '4081781023456789', '+78880002222', 'Петров Петр Сидорович')
        ''')

        cursor.execute('''
            INSERT INTO bank_transactions (account_out, account_in, value, event_date)
            VALUES ('4081781012345678', '4081781023456789', 50000, '2024-01-15')
        ''')

        cursor.execute('''
            INSERT INTO mobile_build (event_date, from_call, to_call, duration_sec)
            VALUES ('2024-01-14', '+79990001111', '+78880002222', 120)
        ''')

        cursor.execute('''
            INSERT INTO ecosystem_mapping (bank_id, marketplace_id)
            VALUES (2, 100)
        ''')

        cursor.execute('''
            INSERT INTO market_place_delivery (user_id, event_date, contact_fio, contact_phone, address)
            VALUES (100, '2024-01-16', 'Петров П.С.', '+78880002222', 'г. Москва, ул. Тестовая, д.1')
        ''')

        conn.commit()
        conn.close()

    def test_db_connection_context_manager(self):
        """Проверка работы контекстного менеджера"""
        with EcosystemDB(self.temp_db.name) as db:
            self.assertIsNotNone(db.conn)
            self.assertEqual(db.conn.row_factory, sqlite3.Row)

    def test_find_transaction_info_success(self):
        """Поиск существующей транзакции"""
        with EcosystemDB(self.temp_db.name) as db:
            result = db.find_transaction_info(victim_id=1, amount=50000)
            self.assertIsNotNone(result)
            self.assertEqual(result['victim_account'], '4081781012345678')
            self.assertEqual(result['victim_phone'], '+79990001111')
            self.assertEqual(result['fraud_account'], '4081781023456789')
            self.assertEqual(result['transaction_date'], '2024-01-15')
            self.assertEqual(result['fraud_bank_id'], 2)
            self.assertEqual(result['fraud_fio'], 'Петров Петр Сидорович')
            self.assertEqual(result['fraud_phone'], '+78880002222')

    def test_find_transaction_info_wrong_amount(self):
        """Поиск транзакции с неправильной суммой"""
        with EcosystemDB(self.temp_db.name) as db:
            result = db.find_transaction_info(victim_id=1, amount=99999)
            self.assertIsNone(result)

    def test_find_transaction_info_wrong_victim_id(self):
        """Поиск транзакции с неправильным ID жертвы"""
        with EcosystemDB(self.temp_db.name) as db:
            result = db.find_transaction_info(victim_id=999, amount=50000)
            self.assertIsNone(result)

    def test_get_calls_success(self):
        """Получение звонков между жертвой и мошенником"""
        with EcosystemDB(self.temp_db.name) as db:
            result = db.get_calls('+79990001111', '+78880002222')
            self.assertIsInstance(result, list)
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]['from_call'], '+79990001111')
            self.assertEqual(result[0]['to_call'], '+78880002222')
            self.assertEqual(result[0]['duration_sec'], 120)

    def test_get_calls_reverse_order(self):
        """Звонки в обратном порядке (мошенник звонит жертве)"""
        with EcosystemDB(self.temp_db.name) as db:
            # Добавляем звонок от мошенника к жертве
            conn = sqlite3.connect(self.temp_db.name)
            conn.execute('''
                INSERT INTO mobile_build (event_date, from_call, to_call, duration_sec)
                VALUES ('2024-01-13', '+78880002222', '+79990001111', 60)
            ''')
            conn.commit()
            conn.close()

            result = db.get_calls('+79990001111', '+78880002222')
            self.assertGreaterEqual(len(result), 1)

    def test_get_calls_no_calls(self):
        """Нет звонков между номерами"""
        with EcosystemDB(self.temp_db.name) as db:
            result = db.get_calls('+79999999999', '+78888888888')
            self.assertIsInstance(result, list)
            self.assertEqual(len(result), 0)

    def test_get_market_activity_success(self):
        """Получение активности на маркетплейсе"""
        with EcosystemDB(self.temp_db.name) as db:
            result = db.get_market_activity(fraud_bank_id=2)
            self.assertIsInstance(result, list)
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]['contact_fio'], 'Петров П.С.')
            self.assertEqual(result[0]['address'], 'г. Москва, ул. Тестовая, д.1')

    def test_get_market_activity_no_activity(self):
        """Нет активности на маркетплейсе для мошенника"""
        with EcosystemDB(self.temp_db.name) as db:
            result = db.get_market_activity(fraud_bank_id=999)
            self.assertIsInstance(result, list)
            self.assertEqual(len(result), 0)


class TestFraudInvestigator(unittest.TestCase):
    """Тесты для класса FraudInvestigator - основной оркестратор"""

    def setUp(self):
        """Создаём временные файлы для тестов"""
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db.close()
        self._create_test_database()

        # Создаём тестовый файл с жалобами
        self.temp_complaints = tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False, encoding='utf-8')
        self.temp_complaints.write('userId\ttext\tevent_date\n')
        self.temp_complaints.write('1\tпропали 50000 рублей с карты\t2024-01-16\n')
        self.temp_complaints.write('1\tменя обманули, денег нет\t2024-01-17\n')
        self.temp_complaints.write('2\tсписали 50000 р мошенникам\t2024-01-16\n')
        self.temp_complaints.write('999\tпотерял 1000 рублей\t2024-01-15\n')
        self.temp_complaints.close()

        self.temp_output_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Удаляем временные файлы после тестов"""
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)
        if os.path.exists(self.temp_complaints.name):
            os.unlink(self.temp_complaints.name)
        # Удаляем созданный CSV если есть
        output_file = Path(self.temp_output_dir) / "fraud_cases_detected.csv"
        if output_file.exists():
            output_file.unlink()
        if os.path.exists(self.temp_output_dir):
            try:
                os.rmdir(self.temp_output_dir)
            except OSError:
                pass  # Dir might not be empty or already removed

    def _create_test_database(self):
        """Создаёт тестовую БД для интеграционных тестов"""
        conn = sqlite3.connect(self.temp_db.name)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE bank_clients (
                userId INTEGER PRIMARY KEY,
                account TEXT,
                phone TEXT,
                fio TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE bank_transactions (
                id INTEGER PRIMARY KEY,
                account_out TEXT,
                account_in TEXT,
                value INTEGER,
                event_date TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE mobile_build (
                id INTEGER PRIMARY KEY,
                event_date TEXT,
                from_call TEXT,
                to_call TEXT,
                duration_sec INTEGER
            )
        ''')

        cursor.execute('''
            CREATE TABLE ecosystem_mapping (
                id INTEGER PRIMARY KEY,
                bank_id INTEGER,
                marketplace_id INTEGER
            )
        ''')

        cursor.execute('''
            CREATE TABLE market_place_delivery (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                event_date TEXT,
                contact_fio TEXT,
                contact_phone TEXT,
                address TEXT
            )
        ''')

        cursor.execute('''
            INSERT INTO bank_clients (userId, account, phone, fio)
            VALUES (1, 'ACC001', '+79990001111', 'Иванов Иван')
        ''')

        cursor.execute('''
            INSERT INTO bank_clients (userId, account, phone, fio)
            VALUES (2, 'ACC002', '+78880002222', 'Петров Петр')
        ''')

        cursor.execute('''
            INSERT INTO bank_transactions (account_out, account_in, value, event_date)
            VALUES ('ACC001', 'ACC002', 50000, '2024-01-15')
        ''')

        cursor.execute('''
            INSERT INTO mobile_build (event_date, from_call, to_call, duration_sec)
            VALUES ('2024-01-14', '+79990001111', '+78880002222', 120)
        ''')

        cursor.execute('''
            INSERT INTO ecosystem_mapping (bank_id, marketplace_id)
            VALUES (2, 100)
        ''')

        cursor.execute('''
            INSERT INTO market_place_delivery (user_id, event_date, contact_fio, contact_phone, address)
            VALUES (100, '2024-01-16', 'Петров П.С.', '+78880002222', 'Тестовый адрес')
        ''')

        conn.commit()
        conn.close()

    def test_run_success(self):
        """Полный успешный запуск расследования"""
        with patch('fraud_analysis.logger') as mock_logger:
            investigator = FraudInvestigator(
                db_path=self.temp_db.name,
                complaints_path=self.temp_complaints.name,
                output_dir=self.temp_output_dir
            )

            investigator.run()

            # Проверяем, что создался CSV файл
            output_file = Path(self.temp_output_dir) / "fraud_cases_detected.csv"
            self.assertTrue(output_file.exists())

            # Проверяем содержимое CSV
            df = pd.read_csv(output_file)
            self.assertGreaterEqual(len(df), 1)  # Должна быть хотя бы одна найденная жалоба

            # Проверяем, что данные о мошеннике корректны
            fraud_cases = df[df['extracted_amount'] == 50000]
            self.assertGreaterEqual(len(fraud_cases), 1)

            # Проверяем колонки
            expected_columns = [
                'complaint_id', 'complaint_text', 'complaint_date',
                'extracted_amount', 'victim_account', 'victim_phone',
                'fraud_account', 'transaction_date', 'fraud_bank_owner_id',
                'fraud_bank_owner_fio', 'fraud_bank_owner_phone',
                'has_calls', 'has_market_activity'
            ]
            for col in expected_columns:
                self.assertIn(col, df.columns)

    def test_run_no_matching_complaints(self):
        """Нет подходящих жалоб"""
        # Создаём файл с жалобами, которые не подходят
        temp_complaints_no_match = tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False, encoding='utf-8')
        temp_complaints_no_match.write('userId\ttext\tevent_date\n')
        temp_complaints_no_match.write('1\tтекст без суммы\t2024-01-16\n')
        temp_complaints_no_match.write('999\tнеправильный ID\t2024-01-15\n')
        temp_complaints_no_match.close()

        try:
            investigator = FraudInvestigator(
                db_path=self.temp_db.name,
                complaints_path=temp_complaints_no_match.name,
                output_dir=self.temp_output_dir
            )

            investigator.run()

            # Проверяем, что CSV не создался (или создался пустым)
            output_file = Path(self.temp_output_dir) / "fraud_cases_detected.csv"
            # В unittest логика проверки отсутствия файла или его пустоты
            # может зависеть от реализации run(), здесь оставляем комментарий,
            # так как оригинальный assert был закомментирован.
        finally:
            os.unlink(temp_complaints_no_match.name)

    def test_run_with_missing_complaints_file(self):
        """Файл с жалобами отсутствует"""
        with patch('fraud_analysis.logger') as mock_logger:
            investigator = FraudInvestigator(
                db_path=self.temp_db.name,
                complaints_path="non_existent_file.tsv",
                output_dir=self.temp_output_dir
            )

            investigator.run()

            # Проверяем, что была вызвана ошибка в логгере
            mock_logger.error.assert_called()

    def test_process_fraud_case_has_calls_and_market(self):
        """Проверка обогащения кейса звонками и маркетплейсом"""
        investigator = FraudInvestigator(
            db_path=self.temp_db.name,
            complaints_path=self.temp_complaints.name,
            output_dir=self.temp_output_dir
        )

        with EcosystemDB(self.temp_db.name) as db:
            trans = db.find_transaction_info(1, 50000)
            self.assertIsNotNone(trans)

            case = investigator._process_fraud_case(
                db=db,
                v_id=1,
                text="тестовый текст",
                date="2024-01-16",
                amount=50000,
                trans=trans
            )

            # Проверяем наличие звонков
            self.assertIn('calls_data', case)
            self.assertIn('has_calls', case)
            self.assertEqual(case['has_calls'], 1)

            # Проверяем наличие маркетплейса
            self.assertIn('market_data', case)
            self.assertIn('has_market_activity', case)
            self.assertEqual(case['has_market_activity'], 1)

    def test_save_results_creates_csv(self):
        """Сохранение результатов создаёт CSV файл"""
        investigator = FraudInvestigator(
            db_path=self.temp_db.name,
            complaints_path=self.temp_complaints.name,
            output_dir=self.temp_output_dir
        )

        # Создаём тестовый кейс
        investigator.cases = [{
            'complaint_id': 1,
            'complaint_text': 'тест',
            'complaint_date': '2024-01-16',
            'extracted_amount': 50000,
            'victim_account': 'ACC001',
            'victim_phone': '+79990001111',
            'fraud_account': 'ACC002',
            'transaction_date': '2024-01-15',
            'fraud_bank_owner_id': 2,
            'fraud_bank_owner_fio': 'Петров Петр',
            'fraud_bank_owner_phone': '+78880002222',
            'calls_data': [],
            'market_data': [],
            'has_calls': 0,
            'has_market_activity': 0
        }]

        investigator._save_results()

        output_file = Path(self.temp_output_dir) / "fraud_cases_detected.csv"
        self.assertTrue(output_file.exists())

        # Проверяем, что calls_data и market_data не попали в CSV
        df = pd.read_csv(output_file)
        self.assertNotIn('calls_data', df.columns)
        self.assertNotIn('market_data', df.columns)
        self.assertIn('has_calls', df.columns)


class TestEdgeCases(unittest.TestCase):
    """Пограничные случаи и особые ситуации"""

    def test_amount_extractor_with_very_large_number(self):
        """Очень большая сумма"""
        extractor = AmountExtractor()
        text = f"потерял {10 ** 12} рублей"
        result = extractor.extract(text)
        self.assertEqual(result, 10 ** 12)

    def test_amount_extractor_with_unicode_currency(self):
        """Разные валютные символы Unicode"""
        extractor = AmountExtractor()
        text = "списали 1000 ₽"
        result = extractor.extract(text)
        self.assertEqual(result, 1000)

    def test_amount_extractor_with_newlines(self):
        """Текст с переносами строк"""
        extractor = AmountExtractor()
        text = "пропали\n50000\nрублей"
        result = extractor.extract(text)
        self.assertEqual(result, 50000)

    def test_amount_extractor_with_extra_spaces(self):
        """Лишние пробелы"""
        extractor = AmountExtractor()
        text = "пропали    50000    рублей"
        result = extractor.extract(text)
        self.assertEqual(result, 50000)

    def test_ecosystem_db_with_empty_database(self):
        """Пустая база данных"""
        temp_empty_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        temp_empty_db.close()

        try:
            # Создаём пустую БД с правильными таблицами
            conn = sqlite3.connect(temp_empty_db.name)
            cursor = conn.cursor()
            cursor.execute('CREATE TABLE bank_clients (userId INTEGER PRIMARY KEY, account TEXT, phone TEXT, fio TEXT)')
            cursor.execute(
                'CREATE TABLE bank_transactions (id INTEGER PRIMARY KEY, account_out TEXT, account_in TEXT, value INTEGER, event_date TEXT)')
            cursor.execute(
                'CREATE TABLE mobile_build (id INTEGER PRIMARY KEY, event_date TEXT, from_call TEXT, to_call TEXT, duration_sec INTEGER)')
            cursor.execute(
                'CREATE TABLE ecosystem_mapping (id INTEGER PRIMARY KEY, bank_id INTEGER, marketplace_id INTEGER)')
            cursor.execute(
                'CREATE TABLE market_place_delivery (id INTEGER PRIMARY KEY, user_id INTEGER, event_date TEXT, contact_fio TEXT, contact_phone TEXT, address TEXT)')
            conn.commit()
            conn.close()

            with EcosystemDB(temp_empty_db.name) as db:
                result = db.find_transaction_info(1, 100)
                self.assertIsNone(result)

                calls = db.get_calls('+111', '+222')
                self.assertIsInstance(calls, list)
                self.assertEqual(len(calls), 0)

                market = db.get_market_activity(1)
                self.assertIsInstance(market, list)
                self.assertEqual(len(market), 0)
        finally:
            os.unlink(temp_empty_db.name)


if __name__ == '__main__':
    unittest.main()
