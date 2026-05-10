# 🏦 BEN Fraud Monitor — Telegram Bot

Telegram-бот для уведомления операторов банка о новых случаях мошенничества.
Интегрируется с BEN API и базой сотрудников `bot_users.db`.

---

## 📁 Структура проекта

```
BankGuard-working-title-/
├── api.py                        # BEN API (FastAPI)
├── fraud_analysis.py             # Логика расследования
├── db_creator.py                 # Генератор тестовых данных
├── data/
│   ├── bot_users.db              # БД пользователей бота
│   ├── ecosystem_data.db         # Основная БД экосистемы
│   └── bank_complaints.tsv       # Жалобы клиентов
└── bot/
    ├── main.py                   # Точка входа бота
    ├── config.py                 # Настройки через env-переменные
    ├── requirements.txt          # Зависимости
    ├── handlers/
    │   ├── auth.py               # /start, вход, выход
    │   ├── cases.py              # Жалобы, расследования, топ-10
    │   └── admin.py              # Управление пользователями
    └── services/
        ├── db.py                 # Работа с bot_users.db
        ├── api_client.py         # Клиент BEN API
        ├── poller.py             # Фоновый поллер новых жалоб
        └── formatter.py          # Форматирование сообщений
```

---

## ⚙️ Первый запуск (делается один раз)

### Шаг 1 — Установка Python

Скачай и установи Python 3.12 с [python.org](https://www.python.org/downloads/release/python-3120/).

> ⚠️ Python 3.14 не поддерживается рядом библиотек. Рекомендуется именно 3.12.

После установки проверь:
```powershell
python --version
# Python 3.12.x
```

### Шаг 2 — Установка зависимостей

Открой PowerShell в корне проекта (`BankGuard-working-title-`):

```powershell
# Зависимости для API
python -m pip install fastapi uvicorn aiosqlite pandas faker

# Зависимости для бота
cd bot
python -m pip install -r requirements.txt
cd ..
```

`requirements.txt` содержит:
```
aiogram
httpx
aiosqlite
pandas
aiohttp-socks
```

### Шаг 3 — Генерация базы данных

```powershell
# Находясь в корне проекта
python db_creator.py
```

Создаст:
- `data/ecosystem_data.db` — база с 1500 клиентами и 150 кейсами мошенничества
- `data/bank_complaints.tsv` — жалобы клиентов

> ℹ️ Это единственный шаг генерации. Топ мошенников строится
> напрямую через BEN API (`GET /frauds`) — никакой генерации CSV не нужно.

### Шаг 4 — Получение токена бота

1. Открой Telegram, найди `@BotFather`
2. Напиши `/newbot`
3. Придумай название (например: `BEN Fraud Monitor`)
4. Придумай юзернейм — должен заканчиваться на `bot` (например: `ben_fraud_bot`)
5. BotFather выдаст токен вида `7123456789:AAF...` — сохрани его

---

## 🚀 Запуск (каждый раз)

Нужно запустить **два процесса** в двух отдельных терминалах.

### Терминал 1 — BEN API

```powershell
cd C:\Users\...\BankGuard-working-title-
python -m uvicorn api:app --port 8000
```

Успешный запуск выглядит так:
```
INFO: Started server process [XXXX]
INFO: Waiting for application startup.
INFO: Application startup complete.
INFO: Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```

Проверь что API работает — открой в браузере: `http://localhost:8000/docs`

### Терминал 2 — Telegram бот

```powershell
cd C:\Users\...\BankGuard-working-title-\bot

# Обязательно — без этого запросы к API идут через VPN и не работают
set NO_PROXY=localhost,127.0.0.1
set HTTP_PROXY=
set HTTPS_PROXY=

# Установи свой токен от BotFather
set BOT_TOKEN=7123456789:AAF...

python main.py
```

Успешный запуск выглядит так:
```
INFO: Starting BEN Fraud Monitor Bot...
INFO: Start polling
INFO: FraudPoller started (interval=60s)
INFO: Run polling for bot @ben_fraud_bot id=XXXXXXX - 'BEN HELPER'
```

> ⚠️ `set NO_PROXY` нужно писать в том же терминале где запускаешь бота,
> не в отдельном окне.

---

## 🔑 Вход в бота

1. Найди бота в Telegram по юзернейму который задал в BotFather
2. Напиши `/start`
3. Введи логин и пароль
4. После входа бот покажет приветственное сообщение с описанием всех функций

> 🔐 Тестовые аккаунты и пароли — в `TEAM.md` (не публикуется в открытый доступ)

---

## 🤖 Функциональность

### Меню оператора

| Кнопка | Действие |
|---|---|
| 📋 Жалобы | Последние 10 жалоб + кнопки быстрого расследования |
| 🔍 Расследовать | Ввести ID жалобы вручную (или команда `/case B_5400`) |
| 🏴‍☠️ Топ мошенников | Последние 10 выявленных мошенников с профилями из API |
| 🚪 Выйти | Отвязать TG ID, отключить уведомления |

### Дополнительно для администратора

| Кнопка | Действие |
|---|---|
| 👥 Пользователи | Список всех сотрудников (🟢 привязан / 🔴 не привязан) |
| ➕ Добавить | Создать нового пользователя |
| 🗑 Удалить | Удалить пользователя по логину |
| 🔗 Задать TG ID | Привязать Telegram ID вручную |

### Профиль мошенника

При нажатии на карточку из топ-10 бот показывает полный профиль через `GET /full-profile/{bank_id}`:
- ФИО, телефон, адрес, счёт
- Теги риска (город, уровень кражи, оператор, маркетплейс)
- Количество переводов, звонков, заказов
- Текст жалобы от жертвы

### Автоуведомления

Каждые 60 секунд бот проверяет новые жалобы через BEN API.
При появлении новой — автоматически расследует и рассылает сводку
всем операторам с привязанным Telegram ID.

Сводка содержит:
- ФИО жертвы и мошенника
- Сумма и дата транзакции
- История звонков между ними
- Адреса доставок маркетплейса

---

## 🌐 BEN API — эндпоинты

| Метод | Путь | Описание |
|---|---|---|
| GET | `/complaints` | Список жалоб с фильтрацией по дате |
| GET | `/complaints/{id}` | Текст конкретной жалобы |
| POST | `/investigate/{id}` | Запуск расследования по жалобе |
| GET | `/cases/{id}/calls` | Звонки между мошенником и жертвой |
| GET | `/cases/{id}/delivery` | Доставки маркетплейса мошенника |
| GET | `/frauds` | Список профилей выявленных мошенников |
| GET | `/full-profile/{id}` | Полный профиль пользователя |

Документация Swagger: `http://localhost:8000/docs`

---

## ⚙️ Конфигурация

Все параметры задаются через переменные окружения или в `config.py`:

| Переменная | По умолчанию | Описание |
|---|---|---|
| `BOT_TOKEN` | — | Токен от @BotFather (обязательно) |
| `BEN_API_URL` | `http://127.0.0.1:8000` | URL BEN API |
| `BEN_API_TOKEN` | `secret-token-123` | Bearer-токен для API |
| `USERS_DB` | `data/bot_users.db` | Путь к БД пользователей бота |
| `POLL_INTERVAL` | `60` | Интервал проверки новых жалоб (сек) |

---

## ❗ Частые проблемы

**503 Service Unavailable при обращении к API**
- Убедись что в терминале с ботом выполнено `set NO_PROXY=localhost,127.0.0.1`
- Убедись что API запущен: открой `http://localhost:8000/docs` в браузере

**ModuleNotFoundError: No module named 'aiosqlite'**
```powershell
python -m pip install aiosqlite
```

**Cannot connect to host api.telegram.org**
- Включи VPN
- Или добавь в `main.py` прокси через v2rayN (порт 10808):
```python
session = AiohttpSession(proxy="socks5://127.0.0.1:10808")
```

**404 при расследовании — жалоба не найдена**
- Данные рассинхронизированы. Останови всё и пересоздай базу:
```powershell
python db_creator.py
```

**Бот завис в состоянии расследования и кнопки меню не работают**
- Напиши `/start` в боте — сбросит FSM-состояние

---

## 🔄 Пересоздание данных

Если нужно сбросить все данные и начать заново:

```powershell
# 1. Останови API (Ctrl+C) и бота (Ctrl+C)

# 2. Пересоздай данные
python db_creator.py

# 3. Запусти снова
python -m uvicorn api:app --port 8000
# (в другом терминале)
cd bot && set NO_PROXY=localhost,127.0.0.1 && python main.py
```
