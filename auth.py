"""
Модуль аутентификации и авторизации для банковской системы.

Этот модуль предоставляет полный набор функций для управления пользователями:
- Хеширование и верификация паролей
- Создание, чтение, удаление пользователей
- Управление сессиями
- Проверка прав доступа

Зависимости:
    - sqlite3: для работы с базой данных
    - hashlib: для хеширования паролей
    - secrets: для генерации безопасных токенов

Пример использования:
    >>> from auth import authenticate_user, create_session, validate_token
    >>>
    >>> # Аутентификация пользователя
    >>> success, token, user = authenticate_user('data.db', 'admin', 'password')
    >>>
    >>> # Создание сессии
    >>> if success:
    ...     create_session(token, user)
    ...     print(f"Добро пожаловать, {user['username']}!")
"""

import sqlite3
import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple, List, Callable, Any

# Константы
SALT = "bankguard_salt_2024"  # Соль для хеширования паролей
TOKEN_EXPIRY_HOURS = 24  # Срок действия токена в часах


def hash_password(password: str) -> str:
    """
    Хеширует пароль с использованием SHA-256 и соли.

    Алгоритм:
        1. Добавляет соль к паролю
        2. Вычисляет SHA-256 хеш
        3. Возвращает хеш в шестнадцатеричном формате

    Args:
        password (str): Исходный пароль в открытом виде

    Returns:
        str: Хешированный пароль (64 символа в шестнадцатеричном формате)

    Example:
        >>> hash_password("mysecret123")
        'a1b2c3d4e5f6...'  # 64 символа

    Note:
        Хеширование необратимо. Для верификации используется функция verify_password().
        Соль (SALT) должна храниться в секрете.
    """
    return hashlib.sha256((password + SALT).encode()).hexdigest()


def verify_password(password: str, password_hash: str) -> bool:
    """
    Проверяет соответствие пароля сохранённому хешу.

    Args:
        password (str): Пароль для проверки (введённый пользователем)
        password_hash (str): Хеш пароля из базы данных

    Returns:
        bool: True, если пароль верен, иначе False

    Example:
        >>> stored_hash = hash_password("admin123")
        >>> verify_password("admin123", stored_hash)
        True
        >>> verify_password("wrong", stored_hash)
        False
    """
    return hash_password(password) == password_hash


def create_user(db_path: str, username: str, password: str,
                is_admin: bool = False, has_telegram: bool = False) -> Tuple[bool, str]:
    """
    Создаёт нового пользователя в системе.

    Эта функция предназначена для использования администраторами банка.
    Пароль автоматически хешируется перед сохранением.

    Args:
        db_path (str): Путь к файлу базы данных SQLite
        username (str): Уникальный логин сотрудника
        password (str): Временный пароль (должен быть заменён при первом входе)
        is_admin (bool, optional): Флаг администратора. Defaults to False.
        has_telegram (bool, optional): Наличие Telegram. Defaults to False.

    Returns:
        Tuple[bool, str]:
            - bool: True при успехе, False при ошибке
            - str: Сообщение о результате операции

    Example:
        >>> success, message = create_user('data.db', 'ivanov', 'pass123', is_admin=False)
        >>> if success:
        ...     print(f"Успех: {message}")
        ... else:
        ...     print(f"Ошибка: {message}")

    Raises:
        sqlite3.IntegrityError: Если пользователь с таким логином уже существует
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        password_hash = hash_password(password)
        is_admin_int = 1 if is_admin else 0
        has_telegram_int = 1 if has_telegram else 0

        cursor.execute('''
            INSERT INTO users (username, password_hash, is_admin, has_telegram)
            VALUES (?, ?, ?, ?)
        ''', (username, password_hash, is_admin_int, has_telegram_int))

        conn.commit()
        conn.close()
        return True, f" Пользователь '{username}' успешно создан"

    except sqlite3.IntegrityError:
        return False, f" Ошибка, пользователь '{username}' уже существует"
    except Exception as e:
        return False, f"❌ Ошибка, {str(e)}"


def authenticate_user(db_path: str, username: str, password: str) -> Tuple[bool, Optional[str], Optional[Dict]]:
    """
    Аутентифицирует пользователя по логину и паролю.

    При успешной аутентификации генерирует новый токен сессии.

    Args:
        db_path (str): Путь к файлу базы данных SQLite
        username (str): Логин сотрудника
        password (str): Пароль для проверки

    Returns:
        Tuple[bool, Optional[str], Optional[Dict]]:
            - bool: True при успешной аутентификации
            - str: Токен сессии (32 случайных байта в URL-safe формате)
            - Dict: Данные пользователя (id, username, is_admin, has_telegram)

    Example:
        >>> success, token, user_data = authenticate_user('data.db', 'admin', 'admin123')
        >>> if success:
        ...     print(f"Добро пожаловать, {user_data['username']}!")
        ...     print(f"Ваш токен: {token}")
        ... else:
        ...     print("Неверный логин или пароль")

    Note:
        Токен не сохраняется в базу данных. Сессия создаётся отдельным вызовом create_session().
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM users WHERE username = ?', (username,))
    user = cursor.fetchone()
    conn.close()

    if user and verify_password(password, user['password_hash']):
        # Генерируем криптостойкий токен
        token = secrets.token_urlsafe(32)
        user_data = {
            'id': user['id'],
            'username': user['username'],
            'is_admin': bool(user['is_admin']),
            'has_telegram': bool(user['has_telegram'])
        }
        return True, token, user_data

    return False, None, None


# Хранилище активных сессий
# Структура: {token: (user_data, expires_at)}
_active_tokens: Dict[str, Tuple[Dict, datetime]] = {}


def create_session(token: str, user_data: Dict) -> None:
    """
    Создаёт новую сессию для пользователя с ограниченным временем жизни.

    Args:
        token (str): Уникальный токен сессии
        user_data (Dict): Данные пользователя (id, username, is_admin, has_telegram)

    Returns:
        None

    Example:
        >>> success, token, user_data = authenticate_user('data.db', 'admin', 'pass')
        >>> if success:
        ...     create_session(token, user_data)
        ...     # Токен действителен в течение TOKEN_EXPIRY_HOURS часов

    Note:
        Токены хранятся в оперативной памяти и теряются при перезапуске сервера.
        Для production-среды рекомендуется использовать Redis или JWT.
    """
    expires_at = datetime.now() + timedelta(hours=TOKEN_EXPIRY_HOURS)
    _active_tokens[token] = (user_data, expires_at)


def validate_token(token: str) -> Optional[Dict]:
    """
    Проверяет валидность токена и возвращает данные пользователя.

    Проверяет:
        1. Существует ли токен в хранилище
        2. Не истёк ли срок действия токена

    Args:
        token (str): Токен для проверки

    Returns:
        Optional[Dict]: Данные пользователя, если токен валиден, иначе None

    Example:
        >>> user = validate_token(request.headers.get('Authorization'))
        >>> if user:
        ...     print(f"Авторизован как {user['username']}")
        ... else:
        ...     print("Требуется повторная авторизация")
    """
    if token in _active_tokens:
        user_data, expires_at = _active_tokens[token]
        if datetime.now() < expires_at:
            return user_data
        else:
            # Токен истёк — удаляем его
            del _active_tokens[token]
    return None


def get_token_expiry(token: str) -> Optional[datetime]:
    """
    Возвращает время истечения токена.

    Args:
        token (str): Токен для проверки

    Returns:
        Optional[datetime]: Время истечения токена или None, если токен не найден

    Example:
        >>> expiry = get_token_expiry(token)
        >>> if expiry:
        ...     remaining = expiry - datetime.now()
        ...     print(f"Токен истечёт через {remaining}")
    """
    if token in _active_tokens:
        _, expires_at = _active_tokens[token]
        return expires_at
    return None


def logout(token: str) -> bool:
    """
    Завершает сессию пользователя (удаляет токен).

    Args:
        token (str): Токен для удаления

    Returns:
        bool: True, если токен был удалён, иначе False

    Example:
        >>> if logout(request_token):
        ...     print("Вы вышли из системы")
    """
    if token in _active_tokens:
        del _active_tokens[token]
        return True
    return False


def get_all_users(db_path: str) -> List[Dict]:
    """
    Возвращает список всех пользователей системы.

    Args:
        db_path (str): Путь к файлу базы данных SQLite

    Returns:
        List[Dict]: Список словарей с данными пользователей

    Example:
        >>> users = get_all_users('data.db')
        >>> for user in users:
        ...     print(f"{user['id']}: {user['username']} (Admin: {user['is_admin']})")

    Note:
        Пароли (password_hash) не возвращаются по соображениям безопасности.
        Эта функция должна быть доступна только администраторам.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, username, is_admin, has_telegram, created_at 
        FROM users 
        ORDER BY created_at DESC
    ''')
    users = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return users


def get_user_by_id(db_path: str, user_id: int) -> Optional[Dict]:
    """
    Получает информацию о пользователе по его ID.

    Args:
        db_path (str): Путь к файлу базы данных SQLite
        user_id (int): Уникальный идентификатор пользователя

    Returns:
        Optional[Dict]: Данные пользователя или None, если пользователь не найден

    Example:
        >>> user = get_user_by_id('data.db', 1)
        >>> if user:
        ...     print(f"Найден: {user['username']}")
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, username, is_admin, has_telegram, created_at 
        FROM users 
        WHERE id = ?
    ''', (user_id,))
    user = cursor.fetchone()
    conn.close()
    return dict(user) if user else None


def delete_user(db_path: str, user_id: int) -> Tuple[bool, str]:
    """
    Удаляет пользователя из системы по его ID.

    Args:
        db_path (str): Путь к файлу базы данных SQLite
        user_id (int): ID пользователя для удаления

    Returns:
        Tuple[bool, str]:
            - bool: True при успехе, False при ошибке
            - str: Сообщение о результате операции

    Example:
        >>> success, message = delete_user('data.db', 5)
        >>> if success:
        ...     print(f"Пользователь удалён: {message}")

    Warning:
        Эту функцию должны вызывать только администраторы.
        Удаление необратимо.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Проверяем, существует ли пользователь
        cursor.execute('SELECT username FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()

        if not user:
            conn.close()
            return False, f" Пользователь с ID {user_id} не найден"

        cursor.execute('DELETE FROM users WHERE id = ?', (user_id,))
        conn.commit()
        conn.close()
        return True, f" Пользователь '{user[0]}' (ID: {user_id}) удалён"

    except Exception as e:
        return False, f" Ошибка при удалении, {str(e)}"


def update_user_password(db_path: str, user_id: int, new_password: str) -> Tuple[bool, str]:
    """
    Обновляет пароль пользователя.

    Args:
        db_path (str): Путь к файлу базы данных SQLite
        user_id (int): ID пользователя
        new_password (str): Новый пароль

    Returns:
        Tuple[bool, str]: Результат операции и сообщение

    Example:
        >>> success, message = update_user_password('data.db', 1, 'new_secure_pass')
        >>> if success:
        ...     print("Пароль обновлён")
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        password_hash = hash_password(new_password)
        cursor.execute('UPDATE users SET password_hash = ? WHERE id = ?', (password_hash, user_id))

        if cursor.rowcount == 0:
            conn.close()
            return False, f" Пользователь с ID {user_id} не найден"

        conn.commit()
        conn.close()
        return True, f" Пароль для пользователя ID {user_id} обновлён"

    except Exception as e:
        return False, f" Ошибка: {str(e)}"


def is_token_valid(token: str) -> bool:
    """
    Быстрая проверка валидности токена.

    Args:
        token (str): Токен для проверки

    Returns:
        bool: True, если токен валиден и не истёк

    Example:
        >>> if is_token_valid(request_token):
        ...     print("Токен действителен")
    """
    return validate_token(token) is not None


def get_active_sessions_count() -> int:
    """
    Возвращает количество активных сессий.

    Returns:
        int: Количество активных токенов в хранилище

    Example:
        >>> print(f"Активных сессий: {get_active_sessions_count()}")
    """
    return len(_active_tokens)


def clear_expired_tokens() -> int:
    """
    Очищает хранилище от истёкших токенов.

    Returns:
        int: Количество удалённых токенов

    Example:
        >>> removed = clear_expired_tokens()
        >>> print(f"Удалено истёкших токенов: {removed}")
    """
    expired_tokens = []
    now = datetime.now()

    for token, (_, expires_at) in _active_tokens.items():
        if now >= expires_at:
            expired_tokens.append(token)

    for token in expired_tokens:
        del _active_tokens[token]

    return len(expired_tokens)


def require_auth(func: Callable) -> Callable:
    """
    Декоратор для проверки авторизации перед выполнением функции.

    Args:
        func: Функция, требующая авторизации

    Returns:
        Callable: Обёрнутая функция с проверкой токена

    Example:
        >>> @require_auth
        ... def get_sensitive_data(token: str):
        ...     user = validate_token(token)
        ...     return {"data": "secret"}
    """

    def wrapper(token: str, *args, **kwargs):
        user = validate_token(token)
        if not user:
            return {"error": "Не авторизован. Требуется вход в систему."}, 401
        return func(user, *args, **kwargs)

    return wrapper


def require_admin(func: Callable) -> Callable:
    """
    Декоратор для проверки прав администратора.

    Args:
        func: Функция, требующая прав администратора

    Returns:
        Callable: Обёрнутая функция с проверкой прав

    Example:
        >>> @require_admin
        ... def create_new_user(token: str, username: str, password: str):
        ...     # Только администратор может выполнить эту функцию
        ...     return create_user(db_path, username, password)
    """

    def wrapper(token: str, *args, **kwargs):
        user = validate_token(token)
        if not user:
            return {"error": "Не авторизован. Требуется вход в систему."}, 401
        if not user.get('is_admin'):
            return {"error": "Доступ запрещён. Требуются права администратора."}, 403
        return func(user, *args, **kwargs)

    return wrapper


# Словарь с описанием ролей для документации
ROLE_DESCRIPTIONS = {
    'admin': {
        'name': 'Администратор',
        'description': 'Полный доступ к системе. Может создавать и удалять пользователей, '
                       'назначать роли, просматривать все отчёты.',
        'permissions': ['create_user', 'delete_user', 'view_all_reports', 'view_calls', 'export_data']
    },
    'investigator': {
        'name': 'Следователь',
        'description': 'Может просматривать жалобы, звонки и выгружать отчёты.',
        'permissions': ['view_complaints', 'view_calls', 'export_data']
    },
    'observer': {
        'name': 'Наблюдатель',
        'description': 'Может только просматривать список жалоб без деталей.',
        'permissions': ['view_complaints_list']
    }
}

if __name__ == "__main__":
    """
    Демонстрация работы модуля аутентификации.

    Запуск:
        python auth.py

    Показывает примеры использования основных функций.
    """

    DB_PATH = 'data/ecosystem_data.db'

    # Пример 1: Создание пользователя
    print("\n Пример 1: Создание пользователя")
    success, message = create_user(DB_PATH, "test_user", "test123", is_admin=False)
    print(f"  {message}")

    # Пример 2: Аутентификация
    print("\n🔐 Пример 2: Аутентификация")
    success, token, user_data = authenticate_user(DB_PATH, "test_user", "test123")
    if success:
        print(f"   Аутентификация успешна")
        print(f"   Пользователь: {user_data['username']}")
        print(f"   Токен: {token[:20]}...")

        # Пример 3: Создание сессии
        print("\n Пример 3: Создание сессии")
        create_session(token, user_data)
        print(f"   Сессия создана. Токен действителен {TOKEN_EXPIRY_HOURS} часов")

        # Пример 4: Проверка токена
        print("\n Пример 4: Проверка токена")
        validated_user = validate_token(token)
        if validated_user:
            print(f"   Токен валиден. Пользователь: {validated_user['username']}")

        # Пример 5: Выход
        print("\n Пример 5: Завершение сессии")
        logout(token)
        print(f"  Сессия завершена")
    else:
        print(f"  Ошибка аутентификации")

    print("Справка по ролям:")
    for role, info in ROLE_DESCRIPTIONS.items():
        print(f"\n  {role.upper()} - {info['name']}")
        print(f"    {info['description']}")
