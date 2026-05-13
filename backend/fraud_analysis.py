"""Модуль для расследования мошеннических транзакций в банковской экосистеме.

Этот модуль извлекает суммы из текста жалоб, ищет соответствующие транзакции
в базе данных SQLite и формирует детальные профили пользователей с тегами риска.
"""

import asyncio
import json
import logging
import re
from typing import Any, Dict, List, Optional

import aiosqlite
import pandas as pd

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class AmountExtractor:
    """Извлекает сумму транзакции из текстового описания жалобы."""

    def __init__(self) -> None:
        """Инициализирует регулярное выражение для поиска денежных сумм."""
        # Поддерживает пробелы и точки: "5 000 руб", "10.000 ₽", "500р"
        self._pattern = re.compile(r'([\d\s\.]+)[\s]?(?:руб|р|₽)', re.IGNORECASE)

    def extract(self, text: str) -> Optional[int]:
        """Парсит текст для поиска суммы.

        Args:
            text: Строка текста жалобы.

        Returns:
            Целое число (сумма) или None, если сумма не найдена или некорректна.
        """
        if not text or pd.isna(text):
            return None

        match = self._pattern.search(text)
        if match:
            # Очищаем от пробелов и точек перед конвертацией
            clean_number = re.sub(r'[\s\.]', '', match.group(1))
            try:
                return int(clean_number)
            except ValueError:
                return None
        return None


class EcosystemDB:
    """Класс для взаимодействия с базой данных транзакций экосистемы."""

    def __init__(self, db_path: str) -> None:
        """Инициализирует подключение к БД.

        Args:
            db_path: Путь к файлу базы данных SQLite.
        """
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        """Устанавливает асинхронное соединение с базой данных."""
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row

    async def close(self) -> None:
        """Закрывает соединение с базой данных."""
        if self.conn:
            await self.conn.close()

    async def find_transaction(
            self, victim_id: str, amount: int
    ) -> Optional[aiosqlite.Row]:
        """Ищет последнюю транзакцию по ID жертвы и сумме.

        Args:
            victim_id: Идентификатор пострадавшего (userId).
            amount: Сумма транзакции.

        Returns:
            Строка результата запроса или None.
        """
        query = """
            SELECT
                v.fio AS victim_name,
                f.fio AS fraud_name,
                t.event_date AS date,
                f.userId AS fraud_bank_id
            FROM bank_clients v
            JOIN bank_transactions t ON t.account_out = v.account
            JOIN bank_clients f ON f.account = t.account_in
            WHERE v.userId = ? AND t.value = ?
            ORDER BY t.event_date DESC
            LIMIT 1
        """
        if not self.conn:
            return None

        async with self.conn.execute(query, (victim_id, amount)) as cursor:
            return await cursor.fetchone()

    async def get_user_profile_data(
            self, bank_id: str
    ) -> Optional[Dict[str, Any]]:
        """Собирает комплексные данные о пользователе из разных таблиц.

        Args:
            bank_id: ID пользователя в банковской системе.

        Returns:
            Словарь со списками транзакций, звонков и заказов или None.
        """
        if not self.conn:
            return None

        # 1. Основные данные пользователя
        user_query = "SELECT * FROM unified_users WHERE bank_id = ?"
        async with self.conn.execute(user_query, (bank_id,)) as cursor:
            user = await cursor.fetchone()
            if not user:
                return None

        account = user['account']

        # 2. Потенциальные жертвы (кто переводил деньги этому аккаунту)
        victims_query = """
                    SELECT DISTINCT v.userId, v.fio AS author_name
                    FROM bank_clients v
                    JOIN bank_transactions t ON t.account_out = v.account
                    WHERE t.account_in = ?
                """
        async with self.conn.execute(victims_query, (account,)) as cursor:
            victims = await cursor.fetchall()

        # 3. Транзакции
        transfers_query = """
            SELECT * FROM bank_transactions
            WHERE account_out = ? OR account_in = ?
            LIMIT 10
        """
        async with self.conn.execute(transfers_query, (account, account)) as cursor:
            transfers = await cursor.fetchall()

        # 4. Звонки
        calls = []
        if user['phone_mobile']:
            phone = str(int(user['phone_mobile']))
            calls_query = """
                SELECT * FROM mobile_build
                WHERE from_call = ? OR to_call = ?
                LIMIT 10
            """
            async with self.conn.execute(calls_query, (phone, phone)) as cursor:
                calls = await cursor.fetchall()

        # 5. Заказы маркетплейса
        orders = []
        if user['marketplace_id']:
            orders_query = """
                SELECT * FROM market_place_delivery
                WHERE user_id = ?
                LIMIT 10
            """
            async with self.conn.execute(orders_query, (user['marketplace_id'],)) as cursor:
                orders = await cursor.fetchall()

        return {
            "user": user,
            "victims": victims,
            "transfers": transfers,
            "calls": calls,
            "orders": orders
        }


class FraudInvestigator:
    """Оркестратор процесса расследования жалоб на мошенничество."""

    def __init__(self, db_path: str, complaints_path: str) -> None:
        """Инициализирует следователя и загружает данные.

        Args:
            db_path: Путь к БД.
            complaints_path: Путь к TSV-файлу с жалобами.
        """
        self.db_path = db_path
        self.complaints_path = complaints_path
        self.extractor = AmountExtractor()
        self.complaints_df = self._load_complaints()

    def _load_complaints(self) -> pd.DataFrame:
        """Загружает файл жалоб."""
        try:
            return pd.read_csv(self.complaints_path, sep='\t')
        except Exception as e:
            logger.error("Failed to load complaints file: %s", e)
            return pd.DataFrame()

    def _generate_tags(
            self, user_row: aiosqlite.Row, transfers: List[aiosqlite.Row]
    ) -> List[str]:
        """Генерирует список тегов на основе бизнес-логики.

        Args:
            user_row: Данные пользователя из БД.
            transfers: Список транзакций пользователя.

        Returns:
            Список строковых тегов (город, уровень кражи, оператор и т.д.).
        """
        tags = []
        user = dict(user_row)

        # Тег города
        address = user.get('address', '')
        if address:
            city_part = address.split(',')[0].strip()
            clean_city = re.sub(
                r'^(д\.|г\.|с\.|ст\.|к\.|клх|п\.)\s*', '', city_part
            )
            tags.append(clean_city)

        # Тег объема украденного
        account = user.get('account')
        stolen_sum = sum(
            t['value'] for t in transfers if t['account_in'] == account
        )

        if stolen_sum >= 50000:
            tags.append("Крупная кража")
        elif stolen_sum >= 15000:
            tags.append("Средняя кража")
        elif stolen_sum > 1:
            tags.append("Малая кража")

        # Тег маркетплейса
        mkt_id = user.get('marketplace_id', '')
        mkt_match = re.search(r'\d+', mkt_id)
        if mkt_match:
            tags.append("Wildberries" if int(mkt_match.group()) % 2 == 0 else "Ozon")

        # Тег оператора
        mob_id = user.get('mobile_id', '')
        mob_match = re.search(r'\d+$', mob_id)
        if mob_match:
            op_code = int(mob_match.group()[-2:]) % 4
            operators = {0: "МТС", 1: "МегаФон", 2: "Билайн", 3: "Tele2"}
            tags.append(operators.get(op_code, "Неизвестный оператор"))

        return tags

    async def fetch_full_user_profile(
            self, bank_id: str
    ) -> Optional[Dict[str, Any]]:
        """Собирает и форматирует профиль пользователя.

        Args:
            bank_id: Банковский идентификатор пользователя.

        Returns:
            Словарь с форматированным профилем для фронтенда или None.
        """
        db = EcosystemDB(self.db_path)
        await db.connect()
        try:
            data = await db.get_user_profile_data(bank_id)
            if not data:
                return None

            user = data['user']

            actual_complaints = []
            if not self.complaints_df.empty:
                for v in data['victims']:
                    # Безопасно ищем жалобы по userId
                    v_complaints = self.complaints_df[self.complaints_df['userId'].astype(str) == str(v['userId'])]
                    for _, c_row in v_complaints.iterrows():
                        actual_complaints.append({
                            "author": v['author_name'],
                            "text": c_row['text']
                        })

            is_fraud = len(actual_complaints) > 0

            phone_val = user['phone_bank']
            formatted_phone = f"+{int(phone_val)}" if phone_val else "Неизвестно"

            profile = {
                "id": user['unique_id'],
                "name": user['fio_bank'],
                "status": "Мошенник" if is_fraud else "Пользователь",
                "phone": formatted_phone,
                "address": user['address'],
                "bankAccount": user['account'],
                "marketplaceId": user['marketplace_id'],
                "mobileId": user['mobile_id'],
                "bankId": user['bank_id'],
                "threat": "_high" if is_fraud else "_low",
                "tags": self._generate_tags(user, data['transfers']),
                "complaints": actual_complaints,
                "transfers": [
                    {
                        "date": t['event_date'],
                        "sum": f"{t['value']} ₽",
                        "from": t['account_out'],
                        "to": t['account_in']
                    } for t in data['transfers']
                ],
                "calls": [
                    {
                        "date": c['event_date'],
                        "duration": f"{c['duration_sec']} сек.",
                        "from": c['from_call'],
                        "to": c['to_call']
                    } for c in data['calls']
                ],
                "orders": [
                    {
                        "date": o['event_date'],
                        "id": o['user_id'],
                        "fio": o['contact_fio'],
                        "phone": (f"+{int(o['contact_phone'])}"
                                  if o['contact_phone'] else "Неизвестно"),
                        "address": o['address']
                    } for o in data['orders']
                ],
                "connections": []
            }
            return profile
        finally:
            await db.close()

    async def investigate_single_case(self, user_id: str) -> str:
        """Проводит анализ жалобы по ID пользователя.

        Args:
            user_id: ID пострадавшего пользователя.

        Returns:
            JSON-строка с результатами поиска транзакции или ошибкой.
        """
        if self.complaints_df.empty:
            return json.dumps({"error": "Complaints data unavailable"})

        # Фильтруем жалобы пользователя
        user_complaints = self.complaints_df[
            self.complaints_df['userId'] == user_id
            ].sort_values('event_date', ascending=False)

        if user_complaints.empty:
            return json.dumps({"error": f"No complaints for user {user_id}"})

        complaint_text = user_complaints.iloc[0]['text']
        amount = self.extractor.extract(complaint_text)

        if not amount:
            return json.dumps({"error": "Amount extraction failed"})

        db = EcosystemDB(self.db_path)
        await db.connect()
        try:
            trans = await db.find_transaction(user_id, amount)
            if trans:
                result = {
                    "transaction_info": {
                        "who": trans['victim_name'],
                        "to_whom": trans['fraud_name'],
                        "when": trans['date'],
                        "amount": amount
                    },
                    "fraud_bank_id": trans['fraud_bank_id']
                }
            else:
                result = {"error": "Transaction not found"}

            return json.dumps(result, indent=4, ensure_ascii=False)
        finally:
            await db.close()


async def main() -> None:
    """Точка входа для демонстрации работы."""
    investigator = FraudInvestigator(
        db_path='data/ecosystem_data.db',
        complaints_path='data/bank_complaints.tsv'
    )

    # Пример работы
    target_id = "B_9208"
    profile = await investigator.fetch_full_user_profile(target_id)

    if profile:
        print(json.dumps(profile, indent=4, ensure_ascii=False))
    else:
        print(f"User {target_id} not found.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
