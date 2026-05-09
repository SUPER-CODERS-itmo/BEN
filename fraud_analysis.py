import asyncio
import json
import logging
import re
from typing import Any, Dict, Optional, List

import aiosqlite
import pandas as pd

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class AmountExtractor:
    """Извлекает сумму транзакции из текстового описания жалобы."""

    def __init__(self) -> None:
        # Улучшенная регулярка: поддерживает пробелы и точки в числах (например, "5 000 руб" или "10.000 ₽")
        self.pattern = re.compile(r'([\d\s\.]+)[\s]?(?:руб|р|₽)', re.IGNORECASE)

    def extract(self, text: str) -> Optional[int]:
        if not text or pd.isna(text):
            return None

        match = self.pattern.search(text)
        if match:
            # Очищаем от пробелов и точек перед конвертацией в int
            clean_number = re.sub(r'[\s\.]', '', match.group(1))
            try:
                return int(clean_number)
            except ValueError:
                return None
        return None


class EcosystemDB:
    """Класс для взаимодействия с базой данных транзакций экосистемы."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()

    async def find_transaction(self, victim_id: str, amount: int) -> Optional[aiosqlite.Row]:
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

    async def get_user_profile_data(self, bank_id: str) -> Optional[Dict[str, Any]]:
        if not self.conn:
            return None

        async with self.conn.execute("SELECT * FROM unified_users WHERE bank_id = ?", (bank_id,)) as cursor:
            user = await cursor.fetchone()
            if not user:
                return None

        account = user['account']

        complaints_query = """
                    SELECT DISTINCT c.text, c.event_date, v.fio AS author_name
                    FROM complaints c
                    JOIN bank_clients v ON c.victim_bank_id = v.userId
                    JOIN bank_transactions t ON t.account_out = v.account
                    WHERE t.account_in = ?
                """
        async with self.conn.execute(complaints_query, (account,)) as cursor:
            complaints_raw = await cursor.fetchall()
        async with self.conn.execute(
                "SELECT * FROM bank_transactions WHERE account_out = ? OR account_in = ? LIMIT 10",
                (account, account)
        ) as cursor:
            transfers_raw = await cursor.fetchall()

            # Безопасное извлечение телефона
            phone_mobile = user['phone_mobile']
            calls_raw: List[aiosqlite.Row] = []
            if phone_mobile:
                phone = str(int(phone_mobile))
                async with self.conn.execute(
                        "SELECT * FROM mobile_build WHERE from_call = ? OR to_call = ? LIMIT 10",
                        (phone, phone)
                ) as cursor:
                    calls_raw = await cursor.fetchall()

            mkt_id = user['marketplace_id']
            orders_raw: List[aiosqlite.Row] = []
            if mkt_id:
                async with self.conn.execute(
                        "SELECT * FROM market_place_delivery WHERE user_id = ? LIMIT 10",
                        (mkt_id,)
                ) as cursor:
                    orders_raw = await cursor.fetchall()

            return {
                "user": user,
                "complaints": complaints_raw,
                "transfers": transfers_raw,
                "calls": calls_raw,
                "orders": orders_raw
            }


class FraudInvestigator:
    """Оркестратор процесса расследования жалоб на мошенничество."""

    def __init__(self, db_path: str, complaints_path: str) -> None:
        self.db_path = db_path
        self.complaints_path = complaints_path
        self.extractor = AmountExtractor()

        # Загружаем датафрейм один раз при инициализации
        try:
            self.complaints_df = pd.read_csv(self.complaints_path, sep='\t')
        except Exception as e:
            logger.error(f"Failed to load complaints file: {e}")
            self.complaints_df = pd.DataFrame()

    def _generate_tags(self, user_row: aiosqlite.Row, transfers: List[aiosqlite.Row]) -> List[str]:
        """Генерирует список тегов на основе правил бизнес-логики."""
        tags = []

        user = dict(user_row)

        address = user.get('address', '')
        if address:
            city_part = address.split(',')[0].strip()
            clean_city = re.sub(r'^(д\.|г\.|с\.|ст\.|к\.|клх|п\.)\s*', '', city_part)
            tags.append(clean_city)

        account = user.get('account')
        stolen_sum = sum(t['value'] for t in transfers if t['account_in'] == account)

        if stolen_sum >= 50000:
            tags.append("Крупная кража")
        elif stolen_sum >= 15000:
            tags.append("Средняя кража")
        elif stolen_sum > 1:
            tags.append("Малая кража")

        mkt_id = user.get('marketplace_id', '')
        mkt_match = re.search(r'\d+', mkt_id)
        if mkt_match:
            mkt_num = int(mkt_match.group())
            tags.append("Wildberries" if mkt_num % 2 == 0 else "Ozon")

        mob_id = user.get('mobile_id', '')
        mob_match = re.search(r'\d+$', mob_id)
        if mob_match:
            last_two_digits = int(mob_match.group()[-2:])
            op_code = last_two_digits % 4
            operators = {0: "МТС", 1: "МегаФон", 2: "Билайн", 3: "Tele2"}
            tags.append(operators.get(op_code, "Неизвестный оператор"))

        return tags

    async def fetch_full_user_profile(self, bank_id: str) -> Optional[Dict[str, Any]]:
        """Собирает и форматирует профиль пользователя из базы данных."""

        db = EcosystemDB(self.db_path)
        await db.connect()
        try:
            # Получаем сырые данные через класс БД
            data = await db.get_user_profile_data(bank_id)
            if not data:
                return None

            user = data['user']
            complaints_raw = data['complaints']
            transfers_raw = data['transfers']
            calls_raw = data['calls']
            orders_raw = data['orders']

            # Безопасное форматирование телефона
            phone_bank = user['phone_bank']
            formatted_phone = f"+{int(phone_bank)}" if phone_bank else "Неизвестно"

            # Преобразуем в "красивый" формат
            profile = {
                "id": user['unique_id'],
                "name": user['fio_bank'],
                "status": "Мошенник" if len(complaints_raw) > 0 else "Пользователь",
                "phone": formatted_phone,
                "address": user['address'],
                "bankAccount": user['account'],
                "marketplaceId": user['marketplace_id'],
                "bankId": user['bank_id'],
                "threat": "_high" if len(complaints_raw) > 0 else "_low",
                "tags": self._generate_tags(user, transfers_raw),
                "complaints": [{"author": c['author_name'], "text": c['text']} for c in complaints_raw],
                "transfers": [
                    {"date": t['event_date'], "sum": f"{t['value']} ₽", "from": t['account_out'], "to": t['account_in']}
                    for t in transfers_raw
                ],
                "calls": [
                    {"date": c['event_date'], "duration": f"{c['duration_sec']} сек.", "from": c['from_call'],
                     "to": c['to_call']}
                    for c in calls_raw
                ],
                "orders": [
                    {
                        "date": o['event_date'],
                        "id": o['user_id'],
                        "fio": o['contact_fio'],
                        "phone": f"+{int(o['contact_phone'])}" if o['contact_phone'] else "Неизвестно",
                        "address": o['address']
                    }
                    for o in orders_raw
                ],
                "connections": []
            }
            return profile

        finally:
            await db.close()

    async def investigate_single_case(self, user_id: str) -> str:
        """Проводит полный цикл анализа жалобы по ID пользователя."""

        if self.complaints_df.empty:
            return json.dumps({"error": "Complaints data is unavailable"}, ensure_ascii=False)

        # 1. Поиск жалобы в уже загруженном датафрейме
        user_complaints = self.complaints_df[self.complaints_df['userId'] == user_id].sort_values(
            'event_date', ascending=False
        )

        if user_complaints.empty:
            return json.dumps({"error": f"Complaint for user {user_id} not found"}, ensure_ascii=False)

        complaint_text = user_complaints.iloc[0]['text']
        amount = self.extractor.extract(complaint_text)

        if not amount:
            return json.dumps({"error": "Could not extract amount from text"}, ensure_ascii=False)

        # 2. Поиск соответствующей транзакции в базе данных
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
                result = {"error": "Transaction not found for given amount and user"}

            return json.dumps(result, indent=4, ensure_ascii=False)

        finally:
            await db.close()


async def main() -> None:
    """Точка входа для демонстрации работы расследования."""
    investigator = FraudInvestigator(
        db_path='data/ecosystem_data.db',
        complaints_path='data/bank_complaints.tsv'
    )

    # Тестовый ID
    target_id = "B_6069"

    profile_dict = await investigator.fetch_full_user_profile(target_id)

    if profile_dict:
        print(json.dumps(profile_dict, indent=4, ensure_ascii=False))
    else:
        print(f"Пользователь с ID {target_id} не найден.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
