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

    Данные берутся из GET /frauds — список полных профилей.

    Args:
        cases: Список профилей мошенников (поля из fetch_full_user_profile).

    Returns:
        Markdown-строка со списком мошенников.
    """
    if not cases:
        return "📭 Данных нет."

    lines = ["🏴‍☠️ *Последние 10 выявленных мошенников*\n"]

    for i, c in enumerate(cases, 1):
        has_calls  = "✅" if c.get("calls")  else "❌"
        has_market = "✅" if c.get("orders") else "❌"
        tags       = ", ".join(c.get("tags", [])) or "—"

        lines.append(
            f"*{i}.* `{c.get('bankId', '—')}` — "
            f"{c.get('name', '—')}\n"
            f"     🏷 {tags}\n"
            f"     📞 {has_calls} звонки  📦 {has_market} маркетплейс\n"
            f"     📱 {c.get('phone', '—')}"
        )

    return "\n".join(lines)


def fmt_fraud_card(c: dict) -> str:
    """Форматирует детальную карточку одного мошенника.

    Вызывается при нажатии инлайн-кнопки в списке топ-10.
    Данные берутся из GET /full-profile/{bank_id}.

    Args:
        c: Профиль мошенника из fetch_full_user_profile.

    Returns:
        Markdown-строка с полной карточкой мошенника.
    """
    has_calls  = "✅ есть" if c.get("calls")  else "❌ нет"
    has_market = "✅ есть" if c.get("orders") else "❌ нет"
    tags       = ", ".join(c.get("tags", [])) or "—"
    complaints = c.get("complaints", [])
    last_complaint = complaints[0].get("text", "—")[:80] if complaints else "—"

    return (
        f"🔎 *Карточка мошенника*\n\n"
        f"👤 ФИО:         `{c.get('name', '—')}`\n"
        f"🏦 ID в банке:  `{c.get('bankId', '—')}`\n"
        f"📱 Телефон:     `{c.get('phone', '—')}`\n"
        f"💳 Счёт:        `{c.get('bankAccount', '—')}`\n"
        f"📍 Адрес:       {c.get('address', '—')}\n\n"
        f"🏷 Теги:        {tags}\n\n"
        f"💰 Переводов:   {len(c.get('transfers', []))}\n"
        f"📞 Звонков:     {len(c.get('calls', []))}\n"
        f"📦 Заказов:     {len(c.get('orders', []))}\n\n"
        f"📋 Жалоба:      _{last_complaint}..._"
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