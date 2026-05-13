"""Пакет сервисов бота.

Содержит:
    db           — UsersDB, работа с bot_users.db.
    api_client   — BenAPIClient, async-клиент BEN API.
    fraud_reader — FraudCasesReader, чтение fraud_cases_detected.csv.
    poller       — FraudPoller, фоновый поллер новых жалоб.
    formatter    — функции форматирования Telegram-сообщений.
"""
