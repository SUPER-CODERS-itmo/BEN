"""Сервис авторизации и управления пользователями бота.

Работает исключительно с bot_users.db — база данных бота,
полностью отделённая от базы данных веб-сайта.

Схема таблицы users:
    id           INTEGER PRIMARY KEY AUTOINCREMENT
    username     TEXT UNIQUE NOT NULL   — логин для входа в бота
    tg_username  TEXT                   — @handle (справочно, не используется для отправки)
    telegram_id  TEXT UNIQUE            — числовой ID чата; заполняется при первом /start
    password     TEXT NOT NULL          — SHA-256 хэш с солью в формате "salt:hash"
    is_admin     INTEGER DEFAULT 0      — 0 = оператор, 1 = администратор
    created_at   TEXT DEFAULT (datetime('now'))
"""

import hashlib
import os
import sqlite3
from typing import Optional


class UsersDB:
    """Управляет пользователями бота: авторизация, привязка Telegram, CRUD.

    Attributes:
        db_path: Путь к файлу bot_users.db.
    """

    def __init__(self, db_path: str) -> None:
        """Инициализирует сервис.

        Args:
            db_path: Путь к SQLite-файлу bot_users.db.
        """
        self.db_path = db_path

    # ── Внутренние утилиты ───────────────────────────────────────────────

    def _conn(self) -> sqlite3.Connection:
        """Открывает соединение с БД и включает row_factory.

        Returns:
            Объект sqlite3.Connection с row_factory = sqlite3.Row.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _hash(password: str) -> str:
        """Хэширует пароль с солью по алгоритму SHA-256.

        Формат хранения: "salt:hash", где оба значения — hex-строки.

        Args:
            password: Пароль в открытом виде.

        Returns:
            Строка вида "salt:hash" для записи в БД.
        """
        salt = hashlib.sha256(os.urandom(32)).hexdigest()
        h    = hashlib.sha256((salt + password).encode()).hexdigest()
        return f"{salt}:{h}"

    @staticmethod
    def _verify(password: str, stored: str) -> bool:
        """Проверяет пароль против сохранённого хэша.

        Args:
            password: Пароль в открытом виде.
            stored:   Строка "salt:hash" из базы данных.

        Returns:
            True если пароль совпадает, иначе False.
        """
        salt, h = stored.split(":", 1)
        return hashlib.sha256((salt + password).encode()).hexdigest() == h

    # ── Авторизация ──────────────────────────────────────────────────────

    def authenticate(self, username: str, password: str) -> Optional[dict]:
        """Проверяет логин и пароль пользователя.

        Args:
            username: Логин пользователя.
            password: Пароль в открытом виде.

        Returns:
            Словарь с данными пользователя при успехе, иначе None.
        """
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE username = ?", (username,)
            ).fetchone()
        if row and self._verify(password, row["password"]):
            return dict(row)
        return None

    def get_by_telegram(self, telegram_id: str) -> Optional[dict]:
        """Ищет пользователя по числовому Telegram ID.

        Args:
            telegram_id: Числовой ID чата Telegram (строка).

        Returns:
            Словарь с данными пользователя или None, если не найден.
        """
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE telegram_id = ?", (telegram_id,)
            ).fetchone()
        return dict(row) if row else None

    # ── Привязка Telegram ────────────────────────────────────────────────

    def link_telegram(self, username: str, telegram_id: str) -> bool:
        """Привязывает числовой Telegram ID к аккаунту пользователя.

        Вызывается автоматически после успешного входа через /start.

        Args:
            username:    Логин пользователя.
            telegram_id: Числовой ID чата Telegram.

        Returns:
            True при успехе, False при ошибке.
        """
        try:
            with self._conn() as conn:
                conn.execute(
                    "UPDATE users SET telegram_id = ? WHERE username = ?",
                    (telegram_id, username),
                )
            return True
        except Exception:
            return False

    def unlink_telegram(self, telegram_id: str) -> bool:
        """Отвязывает Telegram от аккаунта (выход из бота).

        После отвязки пользователь перестаёт получать уведомления.

        Args:
            telegram_id: Числовой ID чата Telegram.

        Returns:
            True при успехе, False при ошибке.
        """
        try:
            with self._conn() as conn:
                conn.execute(
                    "UPDATE users SET telegram_id = NULL WHERE telegram_id = ?",
                    (telegram_id,),
                )
            return True
        except Exception:
            return False

    # ── Рассылка ─────────────────────────────────────────────────────────

    def get_notifiable(self) -> list[dict]:
        """Возвращает всех пользователей с привязанным Telegram ID.

        Используется поллером для определения списка получателей уведомлений.

        Returns:
            Список словарей с данными пользователей.
        """
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM users WHERE telegram_id IS NOT NULL"
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Список и управление (для админа) ─────────────────────────────────

    def get_all(self) -> list[dict]:
        """Возвращает всех пользователей без паролей, отсортированных по id.

        Returns:
            Список словарей: id, username, tg_username, telegram_id, is_admin, created_at.
        """
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id, username, tg_username, telegram_id, is_admin, created_at "
                "FROM users ORDER BY id"
            ).fetchall()
        return [dict(r) for r in rows]

    def add_user(
        self,
        username:    str,
        password:    str,
        is_admin:    bool = False,
        tg_username: Optional[str] = None,
        telegram_id: Optional[str] = None,
    ) -> tuple[bool, str]:
        """Создаёт нового пользователя бота.

        Args:
            username:    Уникальный логин.
            password:    Пароль в открытом виде (будет захэширован).
            is_admin:    True если пользователь — администратор.
            tg_username: @handle Telegram (справочно).
            telegram_id: Числовой ID чата (если известен заранее).

        Returns:
            Кортеж (успех: bool, сообщение: str).
        """
        try:
            with self._conn() as conn:
                conn.execute(
                    "INSERT INTO users "
                    "(username, tg_username, telegram_id, password, is_admin) "
                    "VALUES (?,?,?,?,?)",
                    (username, tg_username, telegram_id,
                     self._hash(password), int(is_admin)),
                )
            return True, "Пользователь создан"
        except sqlite3.IntegrityError:
            return False, "Логин уже занят"

    def delete_user(self, username: str) -> bool:
        """Удаляет пользователя по логину.

        Args:
            username: Логин удаляемого пользователя.

        Returns:
            True если пользователь найден и удалён, иначе False.
        """
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM users WHERE username = ?", (username,))
        return cur.rowcount > 0

    def change_password(self, username: str, new_password: str) -> bool:
        """Меняет пароль пользователя.

        Args:
            username:     Логин пользователя.
            new_password: Новый пароль в открытом виде.

        Returns:
            True если пользователь найден и пароль обновлён, иначе False.
        """
        with self._conn() as conn:
            cur = conn.execute(
                "UPDATE users SET password = ? WHERE username = ?",
                (self._hash(new_password), username),
            )
        return cur.rowcount > 0
