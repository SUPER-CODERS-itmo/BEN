"""Модуль для расследования случаев мошенничества в экосистеме банка."""

import asyncio
import json
import logging
import re
from typing import Any, Dict, Optional

import aiosqlite
import pandas as pd

# Настройка логирования в стиле Google
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class AmountExtractor:
    """Извлекает сумму транзакции из текстового описания жалобы."""

    def __init__(self) -> None:
        """Инициализирует регулярное выражение для поиска сумм."""
        self.pattern = re.compile(r'(\d+)[\s]?(?:руб|р|₽)', re.IGNORECASE)

    def extract(self, text: str) -> Optional[int]:
        """Ищет числовое значение суммы в тексте.

        Args:
            text: Строка текста жалобы.

        Returns:
            Целое число (сумма) или None, если совпадений не найдено.
        """
        if not text or pd.isna(text):
            return None

        match = self.pattern.search(text)
        return int(match.group(1)) if match else None


class EcosystemDB:
    """Класс для взаимодействия с базой данных транзакций экосистемы."""

    def __init__(self, db_path: str) -> None:
        """Инициализирует путь к БД.

        Args:
            db_path: Путь к файлу базы данных SQLite.
        """
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        """Устанавливает асинхронное соединение с БД."""
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row

    async def close(self) -> None:
        """Закрывает соединение с БД."""
        if self.conn:
            await self.conn.close()

    async def find_transaction(self, victim_id: str, amount: int) -> Optional[Any]:
        """Ищет последнюю транзакцию по ID жертвы и сумме.

        Args:
            victim_id: Идентификатор пострадавшего (userId).
            amount: Сумма транзакции.

        Returns:
            Объект aiosqlite.Row с данными транзакции или None.
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


class FraudInvestigator:
    """Оркестратор процесса расследования жалоб на мошенничество."""

    def __init__(self, db_path: str, complaints_path: str) -> None:
        """Инициализирует компоненты расследования.

        Args:
            db_path: Путь к базе данных.
            complaints_path: Путь к TSV файлу с жалобами.
        """
        self.db_path = db_path
        self.complaints_path = complaints_path
        self.extractor = AmountExtractor()

    async def investigate_single_case(self, user_id: str) -> str:
        """Проводит полный цикл анализа жалобы по ID пользователя.

        Args:
            user_id: Идентификатор пользователя, подавшего жалобу.

        Returns:
            JSON-строка с результатами расследования или описанием ошибки.
        """
        # 1. Чтение жалобы и извлечение суммы
        try:
            df = pd.read_csv(self.complaints_path, sep='\t')
            # Фильтрация и сортировка по дате (от новых к старым)
            user_complaints = df[df['userId'] == user_id].sort_values(
                'event_date', ascending=False
            )

            if user_complaints.empty:
                return json.dumps(
                    {"error": f"Complaint for user {user_id} not found"},
                    ensure_ascii=False
                )

            complaint_text = user_complaints.iloc[0]['text']
            amount = self.extractor.extract(complaint_text)

            if not amount:
                return json.dumps(
                    {"error": "Could not extract amount from text"},
                    ensure_ascii=False
                )

        except Exception as e:
            logger.exception("Error processing complaints file")
            return json.dumps({"error": f"File error: {str(e)}"},
                              ensure_ascii=False)

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
                result = {
                    "error": "Transaction not found for given amount and user"
                }

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
    target_id = "B_5400"

    json_report = await investigator.investigate_single_case(target_id)
    print(json_report)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass