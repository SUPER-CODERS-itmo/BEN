"""Асинхронный клиент для BEN API.

Обёртка над эндпоинтами api.py. Все методы — async, используют httpx.
Авторизация: Bearer-токен в заголовке Authorization.

Эндпоинты:
    GET  /complaints                  — список жалоб с фильтрацией и пагинацией.
    GET  /complaints/{id}             — текст конкретной жалобы.
    POST /investigate/{complaint_id}  — запуск расследования через FraudInvestigator.
    GET  /cases/{fraud_id}/calls      — история звонков между мошенником и жертвой.
    GET  /cases/{fraud_id}/delivery   — доставки маркетплейса для мошенника.
    GET  /frauds                      — список профилей выявленных мошенников.
    GET  /full-profile/{bank_id}      — полный профиль пользователя (единое окно).
"""

from typing import Any, Optional
import httpx
_TRANSPORT = httpx.AsyncHTTPTransport(retries=1)

class BenAPIClient:
    """Клиент BEN API с авторизацией через Bearer-токен.

    Attributes:
        base_url: Базовый URL API без trailing slash.
    """

    def __init__(self, base_url: str, token: str) -> None:
        """Инициализирует клиент.

        Args:
            base_url: Базовый URL BEN API (например, http://localhost:8000).
            token:    Bearer-токен для авторизации.
        """
        self.base_url = base_url.rstrip("/")
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    # ── Жалобы ──────────────────────────────────────────────────────────

    async def get_complaints(
        self,
        start_date: Optional[str] = None,
        end_date:   Optional[str] = None,
        skip:  int = 0,
        limit: int = 20,
    ) -> list[dict]:
        """Возвращает список жалоб с фильтрацией по дате и пагинацией.

        Args:
            start_date: Начальная дата в формате YYYY-MM-DD (включительно).
            end_date:   Конечная дата в формате YYYY-MM-DD (включительно).
            skip:       Количество записей для пропуска (пагинация).
            limit:      Максимальное количество возвращаемых записей.

        Returns:
            Список словарей с полями userId, text, event_date.

        Raises:
            httpx.HTTPStatusError: При ошибке HTTP (4xx, 5xx).
        """
        params: dict[str, Any] = {"skip": skip, "limit": limit}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return await self._get("/complaints", params=params)

    async def get_complaint(self, complaint_id: str) -> dict:
        """Возвращает текст конкретной жалобы по ID.

        Args:
            complaint_id: userId из bank_complaints.tsv.

        Returns:
            Словарь с полями id и text.

        Raises:
            httpx.HTTPStatusError: 404 если жалоба не найдена.
        """
        return await self._get(f"/complaints/{complaint_id}")

    # ── Расследование ────────────────────────────────────────────────────

    async def investigate(self, complaint_id: str) -> dict:
        """Запускает расследование по жалобе через FraudInvestigator.

        Вызывает POST /investigate/{complaint_id}, который внутри:
        1. Читает жалобу из bank_complaints.tsv.
        2. Извлекает сумму транзакции регуляркой.
        3. Ищет соответствующую транзакцию в ecosystem_data.db.
        4. Возвращает данные жертвы, мошенника и транзакции.

        Args:
            complaint_id: userId пострадавшего клиента.

        Returns:
            Словарь с ключами transaction_info (who, to_whom, when, amount)
            и fraud_bank_id.

        Raises:
            httpx.HTTPStatusError: 404 если жалоба или транзакция не найдены.
        """
        async with httpx.AsyncClient(timeout=60, transport=_TRANSPORT) as client:
            r = await client.post(
                f"{self.base_url}/investigate/{complaint_id}",
                headers=self._headers,
            )
            r.raise_for_status()
            return r.json()

    # ── Детали кейса ─────────────────────────────────────────────────────

    async def get_calls(self, fraud_id: str, victim_id: str) -> list[dict]:
        """Возвращает историю звонков между мошенником и жертвой.

        Запрос к таблице mobile_build через номера телефонов из bank_clients.

        Args:
            fraud_id:  userId мошенника в банковской системе.
            victim_id: userId жертвы в банковской системе.

        Returns:
            Список словарей с полями from, to, duration, date.

        Raises:
            httpx.HTTPStatusError: 404 если телефоны не найдены.
        """
        return await self._get(
            f"/cases/{fraud_id}/calls", params={"victim_id": victim_id}
        )

    async def get_delivery(self, fraud_id: str) -> dict:
        """Возвращает данные о доставках маркетплейса для мошенника.

        Сначала маппит bank_id → marketplace_id через ecosystem_mapping,
        затем ищет доставки в market_place_delivery.

        Args:
            fraud_id: userId мошенника в банковской системе.

        Returns:
            Словарь с ключом data (список доставок: address, contact_fio,
            contact_phone, date) или message если аккаунт маркетплейса не найден.
        """
        return await self._get(f"/cases/{fraud_id}/delivery")

    # ── Мошенники ────────────────────────────────────────────────────────

    async def get_frauds(
            self,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            skip: int = 0,
            limit: int = 10,
    ) -> list[dict]:
        params: dict[str, Any] = {"skip": skip, "limit": limit}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return await self._get("/frauds", params=params)

    async def get_full_profile(self, bank_id: str) -> dict:
        return await self._get(f"/full-profile/{bank_id}")

    # ── Низкоуровневый хелпер ────────────────────────────────────────────

    async def _get(self, path: str, params: Optional[dict] = None) -> Any:
        """Выполняет GET-запрос к BEN API.

        Args:
            path:   Путь эндпоинта (например, /complaints).
            params: Query-параметры запроса.

        Returns:
            Десериализованный JSON-ответ.

        Raises:
            httpx.HTTPStatusError: При ошибке HTTP (4xx, 5xx).
            httpx.TimeoutException: При превышении таймаута (30 сек).
        """
        async with httpx.AsyncClient(timeout=30, transport=_TRANSPORT) as client:
            r = await client.get(
                f"{self.base_url}{path}",
                headers=self._headers,
                params=params or {},
            )
            r.raise_for_status()
            return r.json()