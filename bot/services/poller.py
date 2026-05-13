"""Фоновый поллер: следит за новыми жалобами и рассылает уведомления.

Запускается параллельно с ботом через asyncio.TaskGroup.
При старте загружает список существующих жалоб — чтобы не слать уведомления
о старых кейсах при первом запуске.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from aiogram import Bot
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest

from services.api_client import BenAPIClient
from services.db import UsersDB
from services.formatter import fmt_investigation

logger = logging.getLogger(__name__)


class FraudPoller:
    """Периодически опрашивает BEN API и рассылает сводки по новым кейсам.

    Attributes:
        bot:      Экземпляр Telegram Bot для отправки сообщений.
        api:      Клиент BEN API.
        users_db: Сервис базы данных пользователей.
        interval: Интервал между проверками в секундах.
    """

    def __init__(
        self,
        bot:      Bot,
        api:      BenAPIClient,
        users_db: UsersDB,
        interval: int = 60,
    ) -> None:
        """Инициализирует поллер.

        Args:
            bot:      Экземпляр aiogram Bot.
            api:      Клиент BEN API.
            users_db: Сервис bot_users.db.
            interval: Пауза между циклами поллинга в секундах.
        """
        self.bot      = bot
        self.api      = api
        self.users_db = users_db
        self.interval = interval
        self._seen:    set[str] = set()   # уже обработанные complaint_id
        self._running = False

    async def start(self) -> None:
        """Запускает бесконечный цикл поллинга.

        Сначала загружает существующие жалобы (_preload),
        затем каждые self.interval секунд вызывает _poll.
        Ошибки отдельных тиков логируются и не останавливают цикл.
        """
        self._running = True
        logger.info("FraudPoller started (interval=%ds)", self.interval)
        await self._preload()
        while self._running:
            await asyncio.sleep(self.interval)
            try:
                await self._poll()
            except Exception as exc:
                logger.exception("Poller tick error: %s", exc)

    async def stop(self) -> None:
        """Останавливает цикл поллинга после текущего тика."""
        self._running = False

    # ── Внутренние методы ────────────────────────────────────────────────

    async def _preload(self) -> None:
        """Загружает ID существующих жалоб при старте бота.

        Предотвращает повторную рассылку по старым кейсам при перезапуске.
        Запрашивает до 200 последних жалоб без фильтра по дате.
        """
        try:
            items = await self.api.get_complaints(limit=200)
            for c in items:
                self._seen.add(str(c["userId"]))
            logger.info("Pre-loaded %d existing complaints", len(self._seen))
        except Exception as exc:
            logger.warning("Pre-load failed (non-critical): %s", exc)

    async def _poll(self) -> None:
        """Один цикл проверки: ищет жалобы за последние 24 часа.

        Фильтрует уже обработанные (self._seen), для каждой новой
        запускает _handle в отдельной задаче.
        """
        since = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d")
        items = await self.api.get_complaints(start_date=since, limit=100)

        new = [c for c in items if str(c["userId"]) not in self._seen]
        if not new:
            return

        logger.info("Found %d new complaint(s)", len(new))
        for c in new:
            uid = str(c["userId"])
            self._seen.add(uid)
            await self._handle(uid)

    async def _handle(self, complaint_id: str) -> None:
        """Расследует одну жалобу и отправляет сводку операторам.

        Последовательно:
        1. Запускает /investigate — получает данные транзакции и fraud_bank_id.
        2. Если fraud_bank_id найден — запрашивает звонки и доставки.
        3. Форматирует сводку и вызывает _broadcast.

        Args:
            complaint_id: userId жертвы из bank_complaints.tsv.
        """
        try:
            inv = await self.api.investigate(complaint_id)
        except Exception as exc:
            logger.warning("investigate(%s) failed: %s", complaint_id, exc)
            return

        fraud_id = inv.get("fraud_bank_id")
        calls:    Optional[list] = None
        delivery: Optional[dict] = None

        if fraud_id:
            try:
                calls = await self.api.get_calls(fraud_id, complaint_id)
            except Exception as exc:
                logger.debug("get_calls failed: %s", exc)
            try:
                delivery = await self.api.get_delivery(fraud_id)
            except Exception as exc:
                logger.debug("get_delivery failed: %s", exc)

        text = fmt_investigation(complaint_id, inv, calls, delivery)
        await self._broadcast(text)

    async def _broadcast(self, text: str) -> None:
        """Рассылает сообщение всем пользователям с привязанным Telegram ID.

        При TelegramForbiddenError (пользователь заблокировал бота) —
        автоматически отвязывает его telegram_id, чтобы не повторять попытки.

        Args:
            text: Отформатированное Markdown-сообщение для отправки.
        """
        recipients = self.users_db.get_notifiable()
        logger.info("Broadcasting to %d recipient(s)", len(recipients))

        for u in recipients:
            tg_id = u["telegram_id"]
            try:
                await self.bot.send_message(tg_id, text, parse_mode="Markdown")
            except TelegramForbiddenError:
                logger.warning("User %s blocked bot — unlinking TG", u["username"])
                self.users_db.unlink_telegram(tg_id)
            except (TelegramBadRequest, Exception) as exc:
                logger.warning("Send to %s failed: %s", tg_id, exc)
