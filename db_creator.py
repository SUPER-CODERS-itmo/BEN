import sqlite3
import pandas as pd
import random
import re
from faker import Faker
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Any

fake = Faker('ru_RU')

# Конфигурация путей
DB_PATH = 'data/ecosystem_data.db'
COMPLAINTS_TSV = 'data/bank_complaints.tsv'
Path('data').mkdir(exist_ok=True)


def normalize_phone(phone: Any) -> Optional[str]:
    """Нормализует номер телефона, удаляя все нецифровые символы.

    Args:
        phone: Номер телефона в любом формате (строка, число или NaN).

    Returns:
        Строка, состоящая только из цифр, или None, если входное значение пустое.
    """
    if pd.isna(phone):
        return None
    return re.sub(r'\D', '', str(phone))


class DataPopulator:
    """Класс для наполнения базы данных экосистемы синтетическими данными.

    Этот класс отвечает за создание схемы таблиц, генерацию профилей пользователей,
    имитацию транзакций, звонков и сценариев мошенничества.

    Attributes:
        conn: Подключение к базе данных SQLite.
        cursor: Объект курсора для выполнения SQL-запросов.
    """

    def __init__(self, db_path: str):
        """Инициализирует подключение к базе данных.

        Args:
            db_path: Путь к файлу базы данных SQLite.
        """
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

    def setup_schema(self):
        """Создает структуру таблиц в базе данных.

        Удаляет существующие таблицы, если они есть, и создает новые согласно
        схеме экосистемы (unified_users, bank_clients, transactions и др.).
        """
        self.cursor.executescript("""
            DROP TABLE IF EXISTS unified_users;
            DROP TABLE IF EXISTS bank_clients;
            DROP TABLE IF EXISTS bank_transactions;
            DROP TABLE IF EXISTS market_place_delivery;
            DROP TABLE IF EXISTS mobile_build;
            DROP TABLE IF EXISTS mobile_clients;
            DROP TABLE IF EXISTS ecosystem_mapping;

            CREATE TABLE unified_users (
                unique_id TEXT, mobile_id TEXT, bank_id TEXT, marketplace_id TEXT,
                phone_mobile REAL, fio_mobile TEXT, address TEXT, account TEXT,
                phone_bank REAL, fio_bank TEXT, event_date TEXT, contact_fio TEXT,
                contact_phone REAL, address_market TEXT
            );
            CREATE TABLE bank_clients (userId TEXT, account TEXT, phone INTEGER, fio TEXT);
            CREATE TABLE bank_transactions (event_date TEXT, account_out TEXT, account_in TEXT, value REAL);
            CREATE TABLE market_place_delivery (event_date TEXT, user_id TEXT, contact_fio TEXT, contact_phone INTEGER, address TEXT);
            CREATE TABLE mobile_build (event_date TEXT, from_call INTEGER, to_call INTEGER, duration_sec INTEGER);
            CREATE TABLE mobile_clients (client_id TEXT, phone INTEGER, fio TEXT, address TEXT);
            CREATE TABLE ecosystem_mapping (unique_id TEXT, mobile_id TEXT, bank_id TEXT, marketplace_id TEXT);
            
            CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            is_admin INTEGER DEFAULT 0,
            has_telegram INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

    def generate_data(self, n_users: int = 100, n_frauds: int = 10):
        """Генерирует синтетические данные и наполняет ими таблицы.

        Процесс включает создание "мастер-данных" пользователей, наполнение
        источников (банк, мобильный оператор, маркетплейс), имитацию звонков
        злоумышленников и последующих подозрительных транзакций.

        Args:
            n_users: Общее количество уникальных пользователей для генерации.
            n_frauds: Количество генерируемых сценариев мошенничества.
        """
        # 1. Генерируем "Мастер-данные" пользователей
        users_pool = []
        for i in range(n_users):
            phone = int(f"79{random.randint(100000000, 999999999)}")
            user = {
                'unique_id': f"UID_{i + 1:03d}",
                'fio': fake.name(),
                'phone': phone,
                'address': fake.address(),
                'bank_id': f"B_{fake.unique.random_int(1000, 9999)}",
                'mobile_id': f"MOB_{fake.unique.random_int(1000, 9999)}",
                'market_id': f"MKT_{fake.unique.random_int(1000, 9999)}",
                'account': f"40817810{random.randint(100000000000, 999999999999)}",
                'is_fraudster': False
            }
            users_pool.append(user)

        # Помечаем некоторых как мошенников
        fraudsters = random.sample(users_pool, n_frauds)
        for f in fraudsters:
            f['is_fraudster'] = True

        # 2. Наполняем таблицы источников
        complaints = []

        for u in users_pool:
            # Bank
            self.cursor.execute("INSERT INTO bank_clients VALUES (?,?,?,?)",
                                (u['bank_id'], u['account'], u['phone'], u['fio']))
            # Mobile
            self.cursor.execute("INSERT INTO mobile_clients VALUES (?,?,?,?)",
                                (u['mobile_id'], u['phone'], u['fio'], u['address']))
            # Mapping
            self.cursor.execute("INSERT INTO ecosystem_mapping VALUES (?,?,?,?)",
                                (u['unique_id'], u['mobile_id'], u['bank_id'], u['market_id']))

            # Marketplace activity
            if random.random() > 0.3:
                self.cursor.execute("INSERT INTO market_place_delivery VALUES (?,?,?,?,?)",
                                    (fake.date_this_month().strftime('%Y-%m-%d'), u['market_id'],
                                     u['fio'], u['phone'], u['address']))

            # Unified Users
            self.cursor.execute("""
                INSERT INTO unified_users VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (u['unique_id'], u['mobile_id'], u['bank_id'], u['market_id'],
                  u['phone'], u['fio'], u['address'], u['account'],
                  u['phone'], u['fio'], fake.date_this_month().strftime('%Y-%m-%d'),
                  u['fio'], u['phone'], u['address']))

        # 3. Генерируем сценарии мошенничества
        victims = [u for u in users_pool if not u['is_fraudster']]

        for fraudster in fraudsters:
            victim = random.choice(victims)
            amount = random.choice([1500, 5000, 12000, 45000, 90000])
            dt_call = fake.date_time_this_month()
            dt_trans = dt_call + timedelta(minutes=random.randint(5, 30))

            # Звонок
            self.cursor.execute("INSERT INTO mobile_build VALUES (?,?,?,?)",
                                (dt_call.strftime('%Y-%m-%d %H:%M:%S'),
                                 fraudster['phone'], victim['phone'], random.randint(30, 300)))

            # Транзакция
            self.cursor.execute("INSERT INTO bank_transactions VALUES (?,?,?,?)",
                                (dt_trans.strftime('%Y-%m-%d %H:%M:%S'),
                                 victim['account'], fraudster['account'], amount))

            # Жалоба (в TSV)
            complaints.append({
                'userId': victim['bank_id'],
                'text': f"Помогите! {amount} руб. украли со счета после звонка.",
                'event_date': (dt_trans + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
            })

        # 4. Добавляем обычный шум
        for _ in range(n_users * 2):
            u1, u2 = random.sample(users_pool, 2)
            self.cursor.execute("INSERT INTO bank_transactions VALUES (?,?,?,?)",
                                (fake.date_time_this_month().strftime('%Y-%m-%d %H:%M:%S'),
                                 u1['account'], u2['account'], random.randint(100, 2000)))
            self.cursor.execute("INSERT INTO mobile_build VALUES (?,?,?,?)",
                                (fake.date_time_this_month().strftime('%Y-%m-%d %H:%M:%S'),
                                 u1['phone'], u2['phone'], random.randint(10, 100)))

        self.conn.commit()

        # Сохранение TSV
        pd.DataFrame(complaints).to_csv(COMPLAINTS_TSV, sep='\t', index=False)
        print(f"База данных успешно наполнена. Сгенерировано {n_users} пользователей и {n_frauds} кейсов мошенничества.")

    def close(self):
        """Закрывает соединение с базой данных."""
        self.conn.close()


if __name__ == "__main__":
    populator = DataPopulator(DB_PATH)
    populator.setup_schema()
    populator.generate_data(n_users=1500, n_frauds=150)
    populator.close()