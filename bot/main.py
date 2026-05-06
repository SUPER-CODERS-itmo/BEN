"""BEN Fraud Monitor Bot — точка входа.

Запускает два параллельных корутина через asyncio.TaskGroup:
    1. Telegram long-polling (aiogram Dispatcher).
    2. FraudPoller — фоновая проверка новых жалоб в BEN API.

Зависимости пробрасываются в хэндлеры через dp[key] (workflow_data aiogram 3):
    users_db     — UsersDB    (bot_users.db)
    api          — BenAPIClient (BEN API)
    fraud_reader — FraudCasesReader (fraud_cases_detected.csv)

Порядок регистрации роутеров важен:
    auth → cases → admin
    (auth первым, чтобы LoginFSM перехватывал ввод до остальных хэндлеров)
"""

import asyncio
import logging
import sys

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from config import config
from services.api_client import BenAPIClient
from services.db import UsersDB
from services.fraud_reader import FraudCasesReader
from services.poller import FraudPoller
from handlers import auth, cases, admin

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    """Инициализирует все сервисы и запускает бота с поллером.

    Порядок запуска:
    1. Создаёт экземпляры UsersDB, BenAPIClient, FraudCasesReader.
    2. Инициализирует Bot и Dispatcher с MemoryStorage для FSM.
    3. Пробрасывает зависимости в dp.workflow_data.
    4. Регистрирует роутеры хэндлеров.
    5. Запускает Dispatcher и FraudPoller параллельно через TaskGroup.
    """
    logger.info("Starting BEN Fraud Monitor Bot...")

    users_db     = UsersDB(config.users_db_path)
    api          = BenAPIClient(config.api_base_url, config.api_token)
    fraud_reader = FraudCasesReader(config.fraud_cases_csv)

    session = AiohttpSession(proxy="socks5://127.0.0.1:10808")
    bot = Bot(
        token=config.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN),
        session=session,
    )

    dp = Dispatcher(storage=MemoryStorage())

    # Зависимости для хэндлеров
    dp["users_db"]     = users_db
    dp["api"]          = api
    dp["fraud_reader"] = fraud_reader

    # Роутеры — порядок важен
    dp.include_router(auth.router)
    dp.include_router(cases.router)
    dp.include_router(admin.router)

    poller = FraudPoller(
        bot=bot,
        api=api,
        users_db=users_db,
        interval=config.poll_interval,
    )

    async with asyncio.TaskGroup() as tg:
        tg.create_task(
            dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
        )
        tg.create_task(poller.start())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")