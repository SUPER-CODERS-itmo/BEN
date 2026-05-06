"""Хэндлеры авторизации: /start, вход по логину/паролю, выход.

FSM-состояния:
    LoginFSM.login    — ожидание логина.
    LoginFSM.password — ожидание пароля.

После успешного входа telegram_id пользователя записывается в bot_users.db
через UsersDB.link_telegram — с этого момента он начинает получать уведомления.
"""

import logging

from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
)

from services.db import UsersDB

logger = logging.getLogger(__name__)
router = Router()


class LoginFSM(StatesGroup):
    """Состояния FSM для процесса входа в бота."""
    login    = State()   # ожидание ввода логина
    password = State()   # ожидание ввода пароля


def main_menu(is_admin: bool) -> ReplyKeyboardMarkup:
    """Строит главное меню в зависимости от роли пользователя.

    Операторы видят: Жалобы, Расследовать, Топ мошенников, Выйти.
    Администраторы дополнительно видят: Пользователи.

    Args:
        is_admin: True если пользователь — администратор.

    Returns:
        ReplyKeyboardMarkup с кнопками меню.
    """
    rows = [
        [KeyboardButton(text="📋 Жалобы"),        KeyboardButton(text="🔍 Расследовать")],
        [KeyboardButton(text="🏴‍☠️ Топ мошенников")],
    ]
    if is_admin:
        rows.append([KeyboardButton(text="👥 Пользователи")])
    rows.append([KeyboardButton(text="🚪 Выйти")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, users_db: UsersDB) -> None:
    """Обрабатывает команду /start.

    Если пользователь уже авторизован (telegram_id найден в БД) —
    показывает приветствие и меню. Иначе запускает FSM входа.

    Args:
        message:   Входящее сообщение от пользователя.
        state:     FSM-контекст для управления состоянием диалога.
        users_db:  Сервис базы данных пользователей.
    """
    tg_id    = str(message.from_user.id)
    existing = users_db.get_by_telegram(tg_id)

    if existing:
        await message.answer(
            f"👋 Добро пожаловать, *{existing['username']}*!",
            parse_mode="Markdown",
            reply_markup=main_menu(bool(existing["is_admin"])),
        )
        return

    await state.set_state(LoginFSM.login)
    await message.answer(
        "🏦 *BEN — Система анализа мошенничества*\n\nВведите логин:",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove(),
    )


@router.message(LoginFSM.login)
async def on_login(message: Message, state: FSMContext) -> None:
    """Принимает логин и переходит к запросу пароля.

    Args:
        message: Сообщение с логином от пользователя.
        state:   FSM-контекст.
    """
    await state.update_data(username=message.text.strip())
    await state.set_state(LoginFSM.password)
    await message.answer("🔒 Введите пароль:")


@router.message(LoginFSM.password)
async def on_password(message: Message, state: FSMContext, users_db: UsersDB) -> None:
    """Принимает пароль, проверяет учётные данные и выполняет вход.

    Сразу удаляет сообщение с паролем из чата для безопасности.
    При неверном логине/пароле сбрасывает FSM к шагу ввода логина.
    При успехе: привязывает telegram_id, очищает FSM, показывает меню.

    Args:
        message:  Сообщение с паролем от пользователя.
        state:    FSM-контекст.
        users_db: Сервис базы данных пользователей.
    """
    data     = await state.get_data()
    username = data.get("username", "")
    password = message.text.strip()

    try:
        await message.delete()
    except Exception:
        pass

    user = users_db.authenticate(username, password)
    if not user:
        await state.set_state(LoginFSM.login)
        await message.answer("❌ Неверный логин или пароль.\n\nВведите логин:")
        return

    tg_id = str(message.from_user.id)
    users_db.link_telegram(username, tg_id)
    await state.clear()

    role = "Администратор" if user["is_admin"] else "Оператор"
    await message.answer(
        f"✅ Вход выполнен!\n\n"
        f"👤 *{username}* | {role}\n\n"
        f"Вы будете получать уведомления о новых кейсах.",
        parse_mode="Markdown",
        reply_markup=main_menu(bool(user["is_admin"])),
    )
    logger.info("Login: %s (TG %s)", username, tg_id)


@router.message(F.text == "🚪 Выйти")
async def cmd_logout(message: Message, state: FSMContext, users_db: UsersDB) -> None:
    """Выполняет выход: отвязывает telegram_id и сбрасывает FSM.

    После выхода пользователь перестаёт получать уведомления от поллера.

    Args:
        message:  Входящее сообщение.
        state:    FSM-контекст.
        users_db: Сервис базы данных пользователей.
    """
    tg_id = str(message.from_user.id)
    user  = users_db.get_by_telegram(tg_id)
    users_db.unlink_telegram(tg_id)
    await state.clear()

    name = user["username"] if user else "пользователь"
    await message.answer(
        f"👋 До свидания, *{name}*!\n"
        f"Уведомления отключены.\n\n/start — войти снова.",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove(),
    )
