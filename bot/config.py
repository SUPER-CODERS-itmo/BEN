"""Конфигурация бота.

Все параметры читаются из переменных окружения.
Если переменная не задана — используется значение по умолчанию (для локальной разработки).

Переменные окружения:
    BOT_TOKEN:        Токен Telegram-бота от @BotFather.
    BEN_API_URL:      Базовый URL BEN API (по умолчанию http://localhost:8000).
    BEN_API_TOKEN:    Bearer-токен для авторизации в BEN API.
    USERS_DB:         Путь к SQLite-базе пользователей бота (bot_users.db).
    FRAUD_CASES_CSV:  Путь к CSV с выявленными кейсами (fraud_cases_detected.csv).
    POLL_INTERVAL:    Интервал поллинга новых жалоб в секундах (по умолчанию 60).
"""

import os
from dataclasses import dataclass


@dataclass
class Config:
    """Хранит все параметры конфигурации бота.

    Attributes:
        bot_token:       Telegram Bot API токен.
        api_base_url:    Базовый URL BEN API без trailing slash.
        api_token:       Bearer-токен для BEN API.
        users_db_path:   Путь к bot_users.db (БД бота, отдельная от сайта).
        fraud_cases_csv: Путь к fraud_cases_detected.csv для фичи топ-10.
        poll_interval:   Пауза между циклами поллинга в секундах.
        poll_limit:      Максимум жалоб за один запрос при поллинге.
    """

    # ── Telegram ────────────────────────────────────────────────────────
    bot_token: str = os.getenv("BOT_TOKEN", "8642122674:AAGyINEflUNL3uJdt_U5u_c6opSFr58NoBQ")

    # ── BEN API ─────────────────────────────────────────────────────────
    api_base_url: str = os.getenv("BEN_API_URL",   "http://localhost:8000")
    api_token:    str = os.getenv("BEN_API_TOKEN",  "secret-token-123")

    # ── Пути к данным ───────────────────────────────────────────────────
    users_db_path:   str = os.getenv("USERS_DB",        "../data/bot_users.db")
    fraud_cases_csv: str = os.getenv("FRAUD_CASES_CSV", "../data/fraud_cases_detected.csv")

    # ── Поллинг ─────────────────────────────────────────────────────────
    poll_interval: int = int(os.getenv("POLL_INTERVAL", "60"))
    poll_limit:    int = 50


config = Config()
