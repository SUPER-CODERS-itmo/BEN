"""Читает fraud_cases_detected.csv для фичи 'последние 10 мошенников'.

Файл содержит 150 строк с уже выявленными кейсами мошенничества.
В BEN API нет эндпоинта для этих данных — читаем CSV напрямую.

Колонки CSV (13 штук):
    complaint_id           — ID жалобы (userId жертвы).
    complaint_text         — текст жалобы.
    complaint_date         — дата подачи жалобы.
    extracted_amount       — сумма транзакции, извлечённая из текста.
    victim_account         — счёт жертвы.
    victim_phone           — телефон жертвы.
    fraud_account          — счёт мошенника.
    transaction_date       — дата и время транзакции.
    fraud_bank_owner_id    — userId мошенника в банке (используется для /calls и /delivery).
    fraud_bank_owner_fio   — ФИО мошенника.
    fraud_bank_owner_phone — телефон мошенника.
    has_calls              — 1 если найдены звонки, 0 если нет.
    has_market_activity    — 1 если есть активность на маркетплейсе, 0 если нет.
"""

import pandas as pd


class FraudCasesReader:
    """Читает и фильтрует данные из fraud_cases_detected.csv.

    Attributes:
        csv_path: Путь к файлу fraud_cases_detected.csv.
    """

    def __init__(self, csv_path: str) -> None:
        """Инициализирует ридер.

        Args:
            csv_path: Путь к файлу fraud_cases_detected.csv.
        """
        self.csv_path = csv_path

    def get_latest(self, n: int = 10) -> list[dict]:
        """Возвращает n последних кейсов, отсортированных по дате транзакции.

        Используется для кнопки '🏴‍☠️ Топ мошенников' в меню бота.

        Args:
            n: Количество возвращаемых кейсов (по умолчанию 10).

        Returns:
            Список словарей с данными кейсов, отсортированных от нового к старому.
        """
        df = pd.read_csv(self.csv_path)
        df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
        df = df.sort_values("transaction_date", ascending=False).head(n)
        return df.to_dict(orient="records")

    def get_by_id(self, complaint_id: str) -> dict | None:
        """Ищет конкретный кейс по complaint_id (userId жертвы).

        Используется для построения детальной карточки мошенника
        при нажатии инлайн-кнопки в списке топ-10.

        Args:
            complaint_id: ID жалобы для поиска.

        Returns:
            Словарь с данными кейса или None, если кейс не найден.
        """
        df = pd.read_csv(self.csv_path)
        row = df[df["complaint_id"].astype(str) == str(complaint_id)]
        if row.empty:
            return None
        return row.iloc[0].to_dict()
