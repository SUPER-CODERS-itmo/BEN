"""Хэндлеры для работы с жалобами, расследованиями и топ-10 мошенников.

Три точки входа для расследования:
    1. Кнопка '🔍 Расследовать' → FSM → ввод ID вручную.
    2. Инлайн-кнопка из списка жалоб → callback 'inv:{id}'.
    3. Команда /case <ID>.

Топ мошенников читается из fraud_cases_detected.csv через FraudCasesReader,
детальные данные по кейсу (звонки, доставки) — через BenAPIClient.
"""

import logging
from typing import Optional

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

from services.api_client import BenAPIClient
from services.db import UsersDB
from services.formatter import fmt_investigation, fmt_top_fraudsters, fmt_fraud_card

logger = logging.getLogger(__name__)
router = Router()


class InvestigateFSM(StatesGroup):
    """Состояния FSM для ручного ввода ID жалобы."""
    waiting_id = State()


def _auth(users_db: UsersDB, tg_id: str) -> Optional[dict]:
    """Проверяет, авторизован ли пользователь.

    Args:
        users_db: Сервис базы данных пользователей.
        tg_id:    Числовой Telegram ID (строка).

    Returns:
        Словарь с данными пользователя или None если не авторизован.
    """
    return users_db.get_by_telegram(tg_id)


# ── Список жалоб ─────────────────────────────────────────────────────────────

@router.message(F.text == "📋 Жалобы")
async def show_complaints(
    message: Message, api: BenAPIClient, users_db: UsersDB
) -> None:
    """Показывает последние 10 жалоб с кнопками быстрого расследования.

    Запрашивает GET /complaints?limit=10 из BEN API.
    Для первых 5 жалоб строит инлайн-кнопки с callback 'inv:{userId}'.

    Args:
        message:  Входящее сообщение.
        api:      Клиент BEN API.
        users_db: Сервис базы данных пользователей.
    """
    if not _auth(users_db, str(message.from_user.id)):
        await message.answer("⚠️ Сначала войдите: /start")
        return

    await message.answer("⏳ Загружаю...")
    try:
        items = await api.get_complaints(limit=10)
    except Exception as exc:
        await message.answer(f"❌ Ошибка API:\n`{exc}`", parse_mode="Markdown")
        return

    if not items:
        await message.answer("Жалоб не найдено.")
        return

    lines = ["📋 *Последние 10 жалоб*\n"]
    for c in items:
        lines.append(f"• `{c['userId']}` — {str(c.get('event_date', ''))[:10]}")

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text=f"🔍 {c['userId']}",
                callback_data=f"inv:{c['userId']}"
            )]
            for c in items[:5]
        ]
    )
    await message.answer("\n".join(lines), parse_mode="Markdown", reply_markup=keyboard)


# ── Расследование: кнопка меню → FSM ────────────────────────────────────────

@router.message(F.text == "🔍 Расследовать")
async def ask_id(message: Message, state: FSMContext, users_db: UsersDB) -> None:
    """Запускает FSM для ручного ввода ID жалобы.

    Args:
        message:  Входящее сообщение.
        state:    FSM-контекст.
        users_db: Сервис базы данных пользователей.
    """
    if not _auth(users_db, str(message.from_user.id)):
        await message.answer("⚠️ Сначала войдите: /start")
        return
    await state.set_state(InvestigateFSM.waiting_id)
    await message.answer(
        "Введите ID жалобы _(например: B\\_5400)_:", parse_mode="Markdown"
    )


# Кнопки меню — никогда не считаются ID жалобы
_MENU_BUTTONS = {
    "📋 Жалобы", "🔍 Расследовать", "🏴\u200d☠️ Топ мошенников",
    "👥 Пользователи", "🚪 Выйти",
}


@router.message(InvestigateFSM.waiting_id)
async def on_id_input(
    message: Message,
    state: FSMContext,
    api: BenAPIClient,
    users_db: UsersDB,
) -> None:
    """Принимает ID жалобы от пользователя и запускает расследование.

    Если пользователь нажал кнопку меню вместо ввода ID — сбрасывает FSM
    и обрабатывает кнопку как обычно (через остальные хэндлеры).

    Args:
        message:  Сообщение с ID жалобы.
        state:    FSM-контекст.
        api:      Клиент BEN API.
        users_db: Сервис базы данных пользователей.
    """
    if message.text in _MENU_BUTTONS:
        await state.clear()
        await message.answer(
            "Расследование отменено. Выберите действие в меню."
        )
        return
    await state.clear()
    await _run(message, api, message.text.strip())


# ── Расследование: инлайн-кнопка ─────────────────────────────────────────────

@router.callback_query(F.data.startswith("inv:"))
async def cb_investigate(
    cb: CallbackQuery, api: BenAPIClient, users_db: UsersDB
) -> None:
    """Обрабатывает нажатие инлайн-кнопки расследования из списка жалоб.

    Args:
        cb:       CallbackQuery с data='inv:{complaint_id}'.
        api:      Клиент BEN API.
        users_db: Сервис базы данных пользователей.
    """
    await cb.answer()
    complaint_id = cb.data.split(":", 1)[1]
    await _run(cb.message, api, complaint_id)


# ── Расследование: команда /case <ID> ────────────────────────────────────────

@router.message(Command("case"))
async def cmd_case(
    message: Message, api: BenAPIClient, users_db: UsersDB
) -> None:
    """Запускает расследование через команду /case <complaint_id>.

    Пример: /case B_5400

    Args:
        message:  Входящее сообщение с командой.
        api:      Клиент BEN API.
        users_db: Сервис базы данных пользователей.
    """
    if not _auth(users_db, str(message.from_user.id)):
        await message.answer("⚠️ Сначала войдите: /start")
        return
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Укажите ID: `/case B_5400`", parse_mode="Markdown")
        return
    await _run(message, api, parts[1].strip())


async def _run(message: Message, api: BenAPIClient, complaint_id: str) -> None:
    """Выполняет расследование и отправляет результат в чат.

    Общая логика для всех точек входа (FSM, inline, команда):
    1. POST /investigate/{complaint_id} — транзакция и fraud_bank_id.
    2. GET /cases/{fraud_id}/calls — звонки (если fraud_id найден).
    3. GET /cases/{fraud_id}/delivery — доставки (если fraud_id найден).
    4. Форматирует и отправляет сводку с инлайн-кнопками деталей.

    Args:
        message:      Объект Message для ответа.
        api:          Клиент BEN API.
        complaint_id: ID жалобы для расследования.
    """
    await message.answer(f"⏳ Расследую `{complaint_id}`...", parse_mode="Markdown")
    try:
        inv = await api.investigate(complaint_id)
    except Exception as exc:
        await message.answer(f"❌ Ошибка:\n`{exc}`", parse_mode="Markdown")
        return

    fraud_id = inv.get("fraud_bank_id")
    calls:    Optional[list] = None
    delivery: Optional[dict] = None

    if fraud_id:
        try:
            calls = await api.get_calls(fraud_id, complaint_id)
        except Exception:
            pass
        try:
            delivery = await api.get_delivery(fraud_id)
        except Exception:
            pass

    text = fmt_investigation(complaint_id, inv, calls, delivery)

    kb = None
    if fraud_id:
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="📞 Звонки",
                callback_data=f"calls:{fraud_id}:{complaint_id}"
            ),
            InlineKeyboardButton(
                text="📦 Доставки",
                callback_data=f"delivery:{fraud_id}"
            ),
        ]])
    await message.answer(text, parse_mode="Markdown", reply_markup=kb)


# ── Детали: звонки ────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("calls:"))
async def cb_calls(cb: CallbackQuery, api: BenAPIClient) -> None:
    """Загружает и показывает полный список звонков между участниками кейса.

    Args:
        cb:  CallbackQuery с data='calls:{fraud_id}:{victim_id}'.
        api: Клиент BEN API.
    """
    await cb.answer()
    _, fraud_id, victim_id = cb.data.split(":", 2)
    await cb.message.answer("⏳ Загружаю звонки...")
    try:
        calls = await api.get_calls(fraud_id, victim_id)
    except Exception as exc:
        await cb.message.answer(f"❌ `{exc}`", parse_mode="Markdown")
        return

    if not calls:
        await cb.message.answer("📞 Звонков не найдено.")
        return

    lines = [f"📞 *Звонки* (мошенник `{fraud_id}`)\n"]
    for c in calls:
        lines.append(
            f"• {c.get('date','?')} | "
            f"{c.get('from','?')} → {c.get('to','?')} | "
            f"{c.get('duration',0)} сек."
        )
    await cb.message.answer("\n".join(lines), parse_mode="Markdown")


# ── Детали: доставки ──────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("delivery:"))
async def cb_delivery(cb: CallbackQuery, api: BenAPIClient) -> None:
    """Загружает и показывает данные о доставках маркетплейса.

    Args:
        cb:  CallbackQuery с data='delivery:{fraud_id}'.
        api: Клиент BEN API.
    """
    await cb.answer()
    fraud_id = cb.data.split(":", 1)[1]
    await cb.message.answer("⏳ Загружаю доставки...")
    try:
        data = await api.get_delivery(fraud_id)
    except Exception as exc:
        await cb.message.answer(f"❌ `{exc}`", parse_mode="Markdown")
        return

    items = data.get("data", [])
    if not items:
        await cb.message.answer(f"📦 {data.get('message', 'Доставок нет.')}")
        return

    lines = [f"📦 *Доставки* (мошенник `{fraud_id}`)\n"]
    for d in items:
        lines.append(
            f"📍 *{d.get('address','—')}*\n"
            f"   👤 {d.get('contact_fio','—')} | "
            f"📱 {d.get('contact_phone','—')} | "
            f"🗓 {d.get('date','—')}"
        )
    await cb.message.answer("\n".join(lines), parse_mode="Markdown")


# ── Топ-10 мошенников ────────────────────────────────────────────────────────

@router.message(F.text == "🏴‍☠️ Топ мошенников")
async def show_top_fraudsters(
    message:  Message,
    users_db: UsersDB,
    api:      BenAPIClient,
) -> None:
    """Показывает последние 10 выявленных мошенников через GET /frauds.

    Args:
        message:  Входящее сообщение.
        users_db: Сервис базы данных пользователей.
        api:      Клиент BEN API.
    """
    if not _auth(users_db, str(message.from_user.id)):
        await message.answer("⚠️ Сначала войдите: /start")
        return

    await message.answer("⏳ Загружаю...")
    try:
        cases = await api.get_frauds(limit=10)
    except Exception as exc:
        await message.answer(f"❌ Ошибка API:\n`{exc}`", parse_mode="Markdown")
        return

    text = fmt_top_fraudsters(cases)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text=f"🔎 {c.get('bankId', '?')} — "
                     f"{str(c.get('name', ''))[:20]}",
                callback_data=f"fraudcard:{c.get('bankId', '')}"
            )]
            for c in cases
        ]
    )
    await message.answer(text, parse_mode="Markdown", reply_markup=kb)

@router.callback_query(F.data.startswith("fraudcard:"))
async def cb_fraud_card(
    cb:  CallbackQuery,
    api: BenAPIClient,
) -> None:
    """Показывает детальную карточку мошенника через GET /full-profile/{bank_id}.

    Args:
        cb:  CallbackQuery с data='fraudcard:{bank_id}'.
        api: Клиент BEN API.
    """
    await cb.answer()
    bank_id = cb.data.split(":", 1)[1]

    await cb.message.answer("⏳ Загружаю профиль...")
    try:
        profile = await api.get_full_profile(bank_id)
    except Exception as exc:
        await cb.message.answer(f"❌ `{exc}`", parse_mode="Markdown")
        return

    text = fmt_fraud_card(profile)

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="📞 Звонки",
            callback_data=f"calls:{bank_id}:{bank_id}"
        ),
        InlineKeyboardButton(
            text="📦 Доставки",
            callback_data=f"delivery:{bank_id}"
        ),
    ]])
    await cb.message.answer(text, parse_mode="Markdown", reply_markup=kb)