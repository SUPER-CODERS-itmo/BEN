"""Хэндлеры панели администратора: управление пользователями бота.

Доступно только пользователям с is_admin=1 в bot_users.db.
Попытка доступа без прав возвращает сообщение '⛔️ Доступ запрещён.'

FSM-состояния:
    AddUserFSM.username  — ввод логина нового пользователя.
    AddUserFSM.password  — ввод пароля нового пользователя.
    AddUserFSM.role      — выбор роли через инлайн-кнопки.
    SetTgIdFSM.username  — ввод логина пользователя для установки TG ID.
    SetTgIdFSM.tg_id     — ввод числового Telegram ID.
    DeleteUserFSM.username — ввод логина пользователя для удаления.
"""

import logging
from typing import Optional

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

from services.db import UsersDB
from services.formatter import fmt_user_list

logger = logging.getLogger(__name__)
router = Router()


class AddUserFSM(StatesGroup):
    username = State()
    password = State()
    role     = State()


class SetTgIdFSM(StatesGroup):
    username = State()
    tg_id    = State()


class DeleteUserFSM(StatesGroup):
    username = State()


def _admin(users_db: UsersDB, tg_id: str) -> Optional[dict]:
    u = users_db.get_by_telegram(tg_id)
    return u if (u and u["is_admin"]) else None


def _users_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ Добавить",     callback_data="adm:add"),
            InlineKeyboardButton(text="🔄 Обновить",     callback_data="adm:refresh"),
        ],
        [
            InlineKeyboardButton(text="🗑 Удалить",      callback_data="adm:del_start"),
            InlineKeyboardButton(text="🔗 Задать TG ID", callback_data="adm:set_tg"),
        ],
    ])


@router.message(F.text == "👥 Пользователи")
async def show_users(message: Message, users_db: UsersDB) -> None:
    if not _admin(users_db, str(message.from_user.id)):
        await message.answer("⛔️ Доступ запрещён.")
        return
    users = users_db.get_all()
    await message.answer(fmt_user_list(users), reply_markup=_users_kb(), parse_mode=None)


@router.callback_query(F.data == "adm:refresh")
async def cb_refresh(cb: CallbackQuery, users_db: UsersDB) -> None:
    await cb.answer("Обновлено ✅")
    if not _admin(users_db, str(cb.from_user.id)):
        return
    users = users_db.get_all()
    try:
        await cb.message.edit_text(fmt_user_list(users), reply_markup=_users_kb(), parse_mode=None)
    except Exception:
        pass


# ── Добавление ───────────────────────────────────────────────────────────────

@router.callback_query(F.data == "adm:add")
async def cb_add_start(cb: CallbackQuery, state: FSMContext, users_db: UsersDB) -> None:
    await cb.answer()
    if not _admin(users_db, str(cb.from_user.id)):
        return
    await state.set_state(AddUserFSM.username)
    await cb.message.answer("Введите логин нового пользователя:")


@router.message(AddUserFSM.username)
async def add_get_login(message: Message, state: FSMContext) -> None:
    username = message.text.strip()
    if not username or " " in username:
        await message.answer("❌ Логин не должен содержать пробелов. Введите снова:")
        return
    await state.update_data(new_username=username)
    await state.set_state(AddUserFSM.password)
    await message.answer("Введите пароль (минимум 6 символов):")


@router.message(AddUserFSM.password)
async def add_get_password(message: Message, state: FSMContext) -> None:
    try:
        await message.delete()
    except Exception:
        pass
    pw = message.text.strip()
    if len(pw) < 6:
        await message.answer("❌ Минимум 6 символов. Введите снова:")
        return
    await state.update_data(new_password=pw)
    await state.set_state(AddUserFSM.role)
    await message.answer(
        "Выберите роль:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="👤 Оператор",      callback_data="role:op"),
            InlineKeyboardButton(text="🔑 Администратор", callback_data="role:admin"),
        ]])
    )


@router.callback_query(F.data.in_({"role:op", "role:admin"}))
async def add_set_role(cb: CallbackQuery, state: FSMContext, users_db: UsersDB) -> None:
    await cb.answer()
    data     = await state.get_data()
    await state.clear()
    username = data.get("new_username")
    password = data.get("new_password")
    is_admin = cb.data == "role:admin"
    if not username or not password:
        await cb.message.answer("❌ Ошибка: начните заново.")
        return
    ok, msg  = users_db.add_user(username, password, is_admin)
    role_str = "Администратор" if is_admin else "Оператор"
    if ok:
        await cb.message.answer(f"✅ Создан: {username} | {role_str}")
    else:
        await cb.message.answer(f"❌ {msg}")


# ── Удаление ─────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "adm:del_start")
async def cb_del_start(cb: CallbackQuery, state: FSMContext, users_db: UsersDB) -> None:
    """Показывает список пользователей и запрашивает логин для удаления."""
    await cb.answer()
    if not _admin(users_db, str(cb.from_user.id)):
        return
    users = users_db.get_all()
    names = "\n".join(f"• {u['username']}" for u in users)
    await cb.message.answer(f"Введите логин пользователя для удаления:\n\n{names}")
    await state.set_state(DeleteUserFSM.username)


@router.message(DeleteUserFSM.username)
async def del_confirm(message: Message, state: FSMContext, users_db: UsersDB) -> None:
    """Удаляет пользователя по логину."""
    username = message.text.strip()
    await state.clear()
    ok = users_db.delete_user(username)
    if ok:
        await message.answer(f"🗑 Пользователь {username} удалён.")
    else:
        await message.answer(f"❌ Пользователь {username} не найден.")


# ── Установка Telegram ID вручную ────────────────────────────────────────────

@router.callback_query(F.data == "adm:set_tg")
async def cb_set_tg_start(cb: CallbackQuery, state: FSMContext, users_db: UsersDB) -> None:
    """Запускает FSM установки TG ID.

    Нужно если хочешь привязать ID заранее, не дожидаясь входа через /start.
    Если не задать — ID подтянется автоматически при первом /start пользователя.
    """
    await cb.answer()
    if not _admin(users_db, str(cb.from_user.id)):
        return
    users = users_db.get_all()
    names = "\n".join(
        f"• {u['username']} — {u['telegram_id'] or 'не задан'}"
        for u in users
    )
    await cb.message.answer(f"Введите логин пользователя:\n\n{names}", parse_mode=None)
    await state.set_state(SetTgIdFSM.username)


@router.message(SetTgIdFSM.username)
async def set_tg_get_login(message: Message, state: FSMContext, users_db: UsersDB) -> None:
    """Принимает логин и запрашивает Telegram ID."""
    username = message.text.strip()
    users    = {u["username"]: u for u in users_db.get_all()}
    if username not in users:
        await message.answer(f"❌ Пользователь {username} не найден. Введите снова:")
        return
    await state.update_data(target_username=username)
    await state.set_state(SetTgIdFSM.tg_id)
    current = users[username]['telegram_id'] or 'не задан'
    await message.answer(
        f"Введите числовой Telegram ID для {username}:\n"
        f"Текущий: {current}\n\n"
        f"Узнать ID можно у @userinfobot"
    )


@router.message(SetTgIdFSM.tg_id)
async def set_tg_save(message: Message, state: FSMContext, users_db: UsersDB) -> None:
    """Сохраняет Telegram ID. Валидация: только цифры."""
    tg_id = message.text.strip()
    if not tg_id.lstrip("-").isdigit():
        await message.answer("❌ ID должен быть числом. Введите снова:")
        return
    data     = await state.get_data()
    username = data.get("target_username")
    await state.clear()
    ok = users_db.link_telegram(username, tg_id)
    if ok:
        await message.answer(
            f"✅ TG ID {tg_id} привязан к {username}.\n"
            f"Пользователь будет получать уведомления."
        )
    else:
        await message.answer("❌ Ошибка при сохранении.")
