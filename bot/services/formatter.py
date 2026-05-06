"""Форматирование всех Telegram-сообщений бота.

Все функции возвращают строки в Markdown-разметке (parse_mode='Markdown').
Не содержит бизнес-логики — только преобразование данных в текст.
"""

from typing import Optional


def fmt_investigation(
    complaint_id: str,
    inv:      dict,
    calls:    Optional[list] = None,
    delivery: Optional[dict] = None,
) -> str:
    """Форматирует полную сводку по расследованию жалобы.

    Объединяет данные транзакции (из /investigate), звонков (из /calls)
    и доставок маркетплейса (из /delivery) в одно Telegram-сообщение.

    Args:
        complaint_id: ID жалобы (userId жертвы).
        inv:          Результат /investigate — dict с ключами
                      transaction_info (who, to_whom, when, amount) и fraud_bank_id.
        calls:        Список звонков из /calls или None если запрос не удался.
        delivery:     Ответ /delivery (data + message) или None если запрос не удался.

    Returns:
        Отформатированная Markdown-строка для отправки в Telegram.
    """
    tx       = inv.get("transaction_info", {})
    fraud_id = inv.get("fraud_bank_id", "—")

    lines = [
        "🚨 *Новый кейс мошенничества*",
        "",
        "📋 *Транзакция*",
        f"  Жертва:     `{tx.get('who', '—')}`",
        f"  Мошенник:   `{tx.get('to_whom', '—')}`",
        f"  Сумма:      *{tx.get('amount', '—')} руб.*",
        f"  Дата:       {tx.get('when', '—')}",
        f"  ID жалобы:  `{complaint_id}`",
        f"  ID мошен.:  `{fraud_id}`",
    ]

    # Звонки
    lines += ["", "📞 *Звонки*"]
    if calls is None:
        lines.append("  ⚠️ Данные недоступны")
    elif not calls:
        lines.append("  Звонков не найдено")
    else:
        for c in calls[:5]:
            lines.append(
                f"  • {c.get('date', '?')} | "
                f"{c.get('from', '?')} → {c.get('to', '?')} | "
                f"{c.get('duration', 0)} сек."
            )
        if len(calls) > 5:
            lines.append(f"  _...ещё {len(calls) - 5}_")

    # Доставки
    lines += ["", "📦 *Маркетплейс*"]
    if delivery is None:
        lines.append("  ⚠️ Данные недоступны")
    else:
        items = delivery.get("data", [])
        msg   = delivery.get("message")
        if not items:
            lines.append(f"  {msg or 'Доставок нет'}")
        else:
            for d in items[:3]:
                lines.append(f"  📍 {d.get('address', '—')}")
                lines.append(
                    f"     👤 {d.get('contact_fio', '—')} | "
                    f"📱 {d.get('contact_phone', '—')} | "
                    f"🗓 {d.get('date', '—')}"
                )
            if len(items) > 3:
                lines.append(f"  _...ещё {len(items) - 3}_")

    return "\n".join(lines)


def fmt_top_fraudsters(cases: list[dict]) -> str:
    """Форматирует список последних 10 выявленных мошенников.

    Данные берутся из fraud_cases_detected.csv через FraudCasesReader.

    Args:
        cases: Список словарей с данными кейсов (поля из CSV).

    Returns:
        Markdown-строка со списком мошенников.
        Каждая запись: ID, ФИО, сумма, дата, флаги звонков и маркетплейса.
    """
    if not cases:
        return "📭 Данных нет."

    lines = ["🏴‍☠️ *Последние 10 выявленных мошенников*\n"]

    for i, c in enumerate(cases, 1):
        has_calls  = "✅" if c.get("has_calls")           else "❌"
        has_market = "✅" if c.get("has_market_activity") else "❌"
        amount     = c.get("extracted_amount", "—")
        date       = str(c.get("transaction_date", "—"))[:10]

        lines.append(
            f"*{i}.* `{c.get('fraud_bank_owner_id', '—')}` — "
            f"{c.get('fraud_bank_owner_fio', '—')}\n"
            f"     💰 {amount} руб. | 🗓 {date}\n"
            f"     📞 {has_calls} звонки  📦 {has_market} маркетплейс\n"
            f"     📱 {c.get('fraud_bank_owner_phone', '—')}"
        )

    return "\n".join(lines)


def fmt_fraud_card(c: dict) -> str:
    """Форматирует детальную карточку одного мошенника из CSV.

    Вызывается при нажатии инлайн-кнопки в списке топ-10.

    Args:
        c: Словарь с данными кейса из fraud_cases_detected.csv.
           Ожидаемые ключи: fraud_bank_owner_fio, fraud_bank_owner_id,
           fraud_bank_owner_phone, fraud_account, extracted_amount,
           transaction_date, has_calls, has_market_activity, complaint_id.

    Returns:
        Markdown-строка с полной карточкой мошенника.
    """
    has_calls  = "✅ есть" if c.get("has_calls")           else "❌ нет"
    has_market = "✅ есть" if c.get("has_market_activity") else "❌ нет"
    date       = str(c.get("transaction_date", "—"))[:19]

    return (
        f"🔎 *Карточка мошенника*\n\n"
        f"👤 ФИО:         `{c.get('fraud_bank_owner_fio', '—')}`\n"
        f"🏦 ID в банке:  `{c.get('fraud_bank_owner_id', '—')}`\n"
        f"📱 Телефон:     `{c.get('fraud_bank_owner_phone', '—')}`\n"
        f"💳 Счёт:        `{c.get('fraud_account', '—')}`\n\n"
        f"💰 Сумма:       *{c.get('extracted_amount', '—')} руб.*\n"
        f"🗓 Дата:        {date}\n\n"
        f"📞 Звонки:      {has_calls}\n"
        f"📦 Маркетплейс: {has_market}\n\n"
        f"📋 Жалоба ID:  `{c.get('complaint_id', '—')}`"
    )


def _escape_md(text: str) -> str:
    """Экранирует спецсимволы Markdown: _ * ` [."""
    for ch in ("_", "*", "`", "["):
        text = text.replace(ch, f"\\{ch}")
    return text


def fmt_user_list(users: list[dict]) -> str:
    """Форматирует список пользователей бота для админа.

    Args:
        users: Список словарей из UsersDB.get_all().
               Ожидаемые ключи: is_admin, username, tg_username, telegram_id.

    Returns:
        Строка со списком пользователей (без Markdown для надёжности).
        🟢 — telegram_id привязан (получает уведомления).
        🔴 — telegram_id не привязан (не получает уведомлений).
    """
    if not users:
        return "Пользователей нет."

    lines = ["👥 Пользователи бота\n"]
    for u in users:
        role   = "🔑 Админ" if u["is_admin"] else "👤 Оператор"
        tg_un  = f"@{u['tg_username']}" if u.get("tg_username") else "—"
        linked = "🟢" if u.get("telegram_id") else "🔴"
        lines.append(f"{linked} {role} | {_escape_md(u['username'])} | {tg_un}")

    return "\n".join(lines)