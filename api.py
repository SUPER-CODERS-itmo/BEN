import aiosqlite
import pandas as pd
from pathlib import Path
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, Depends, HTTPException, Query, Security
from fastapi.responses import RedirectResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from fraud_analysis import EcosystemDB, AmountExtractor

app = FastAPI(
    title="BEN API",
    description="BEN",
    version="1.1.0"
)
security = HTTPBearer()

DB_PATH = 'data/ecosystem_data.db'
COMPLAINTS_TSV = 'data/bank_complaints.tsv'


@app.on_event("startup")
async def startup_event() -> None:
    """Инициализирует базу данных и переносит данные из TSV в SQLite.

    Проверяет наличие исходных файлов, создает схему таблицы жалоб
    и выполняет первичную миграцию данных, если таблица пуста.
    """
    if not Path(COMPLAINTS_TSV).exists() or not Path(DB_PATH).exists():
        return

    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS complaints (
                id TEXT,
                victim_bank_id TEXT,
                text TEXT,
                event_date TEXT,
                status TEXT DEFAULT 'New'
            )
        """)

        cursor = await conn.execute("SELECT COUNT(*) FROM complaints")
        row = await cursor.fetchone()

        if row and row[0] == 0:
            df = pd.read_csv(COMPLAINTS_TSV, sep='\t')
            for _, row_df in df.iterrows():
                user_id = (
                    row_df.get('userId')
                    if pd.notnull(row_df.get('userId'))
                    else row_df.get('uerId')
                )
                await conn.execute(
                    """INSERT INTO complaints 
                       (victim_bank_id, text, event_date, status) 
                       VALUES (?, ?, ?, ?)""",
                    (user_id, row_df.get('text'), row_df.get('event_date'), 'New')
                )
            await conn.commit()


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    """Перенаправляет корневой URL на документацию API.

    Возвращает:
        Объект RedirectResponse, указывающий на эндпоинт /docs.
    """
    return RedirectResponse(url="/docs")


def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)) -> str:
    """Проверяет Bearer-токен и активирует кнопку Authorize в Swagger.

    Аргументы:
        credentials: Данные авторизации, автоматически извлеченные FastAPI.

    Возвращает:
        ID проверенного оператора.

    Исключения:
        HTTPException: Если токен отсутствует, неверно сформирован или невалиден.
    """
    token = credentials.credentials
    if token != "secret-token-123":
        raise HTTPException(
            status_code=403,
            detail="Неверный или просроченный токен доступа"
        )

    return "operator_01"


def audit_log(user_id: str, action: str) -> None:
    """Записывает события безопасности в системный лог.

    Аргументы:
        user_id: Уникальный идентификатор пользователя, совершающего действие.
        action: Описание выполненной операции.
    """
    print(f"[AUDIT LOG] User: {user_id} | Action: {action}")


@app.get("/complaints", dependencies=[Depends(verify_token)])
async def get_complaints(
        start_date: Optional[str] = Query(None, description="Дата от (YYYY-MM-DD)"),
        end_date: Optional[str] = Query(None, description="Дата до (YYYY-MM-DD)"),
        skip: int = Query(0, ge=0),
        limit: int = Query(20, le=100)
) -> List[Dict[str, Any]]:
    """Возвращает список жалоб с возможностью фильтрации и пагинации.

    Аргументы:
        start_date: Дата начала для фильтрации по event_date.
        end_date: Дата окончания для фильтрации по event_date.
        skip: Количество пропускаемых записей.
        limit: Максимальное количество возвращаемых записей.

    Возвращает:
        Список словарей, представляющих записи жалоб.
    """
    async with EcosystemDB(DB_PATH) as db:
        query = "SELECT * FROM complaints WHERE 1=1"
        params = []

        if start_date:
            query += " AND event_date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND event_date <= ?"
            params.append(f"{end_date} 23:59:59")

        query += " LIMIT ? OFFSET ?"
        params.extend([limit, skip])

        cursor = await db.conn.cursor()
        await cursor.execute(query, params)
        rows = await cursor.fetchall()

        return [dict(row) for row in rows]


@app.get("/complaints/{complaint_id}", dependencies=[Depends(verify_token)])
async def get_complaint(complaint_id: str) -> Dict[str, Any]:
    """Возвращает детали конкретной жалобы.

    Аргументы:
        complaint_id: Уникальный целочисленный ID жалобы.

    Возвращает:
        Словарь с данными жалобы.

    Исключения:
        HTTPException: Если жалоба с таким ID не найдена.
    """
    async with EcosystemDB(DB_PATH) as db:
        cursor = await db.conn.cursor()
        await cursor.execute("SELECT * FROM complaints WHERE id = ?", (complaint_id,))
        row = await cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Жалоба не найдена")
        return dict(row)


@app.post("/investigate/{complaint_id}")
async def investigate_complaint(
        complaint_id: str,
        user_id: str = Depends(verify_token)
) -> Dict[str, Any]:
    """Обрабатывает жалобу для выявления потенциального мошенничества.

    Извлекает сумму из текста жалобы, сопоставляет её с данными транзакций
    и обновляет статус жалобы в случае обнаружения мошенничества.

    Аргументы:
        complaint_id: ID жалобы для обработки.
        user_id: ID оператора, проводящего расследование.

    Возвращает:
        Словарь, содержащий статус успеха и детали обнаруженного мошенничества.

    Исключения:
        HTTPException: Если жалоба не найдена, не удалось извлечь сумму
            или не найдена подходящая транзакция.
    """
    extractor = AmountExtractor()

    async with EcosystemDB(DB_PATH) as db:
        cursor = await db.conn.cursor()
        await cursor.execute("SELECT * FROM complaints WHERE id = ?", (complaint_id,))
        complaint = await cursor.fetchone()

        if not complaint:
            raise HTTPException(status_code=404, detail="Жалоба не найдена")

        text = complaint['text']
        v_bank_id = complaint['victim_bank_id']

        amount = extractor.extract(text)
        if not amount:
            raise HTTPException(
                status_code=400,
                detail="Не удалось извлечь сумму из текста жалобы"
            )

        trans = await db.find_transaction_info(v_bank_id, amount)
        if not trans:
            raise HTTPException(
                status_code=404,
                detail="Подозрительная транзакция не найдена"
            )

        await cursor.execute(
            "UPDATE complaints SET status = 'Processed' WHERE id = ?",
            (complaint_id,)
        )
        await db.conn.commit()

        audit_log(user_id, f"Расследована жалоба #{complaint_id}")

        return {
            "status": "Success",
            "message": "Мошенник найден, жалоба обработана",
            "data": {
                "transaction_date": trans['transaction_date'],
                "amount": amount,
                "victim_account": trans['victim_account'],
                "fraud_account": trans['fraud_account'],
                "fraud_bank_id": trans['fraud_bank_id']
            }
        }


@app.get("/cases/{fraud_id}/calls", dependencies=[Depends(verify_token)])
async def get_fraud_calls(
        fraud_id: str,
        victim_id: str
) -> List[Dict[str, Any]]:
    """Возвращает логи звонков между подозреваемым и жертвой.

    Аргументы:
        fraud_id: Банковский ID пользователя подозреваемого.
        victim_id: Банковский ID пользователя жертвы.

    Возвращает:
        Список записей о взаимодействии через звонки.

    Исключения:
        HTTPException: Если не удается определить номера телефонов для любой из сторон.
    """
    async with EcosystemDB(DB_PATH) as db:
        cursor = await db.conn.cursor()

        await cursor.execute(
            "SELECT phone FROM bank_clients WHERE userId = ?", (fraud_id,)
        )
        f_row = await cursor.fetchone()
        await cursor.execute(
            "SELECT phone FROM bank_clients WHERE userId = ?", (victim_id,)
        )
        v_row = await cursor.fetchone()

        if not f_row or not v_row:
            raise HTTPException(
                status_code=404,
                detail="Не удалось найти номера телефонов по ID"
            )

        calls = await db.get_calls(str(v_row['phone']), str(f_row['phone']))

        return [
            {
                "from": c["from_call"],
                "to": c["to_call"],
                "duration": c["duration_sec"],
                "date": c["event_date"]
            }
            for c in calls
        ]


@app.get("/cases/{fraud_id}/delivery", dependencies=[Depends(verify_token)])
async def get_fraud_delivery(fraud_id: str) -> Dict[str, Any]:
    """Возвращает историю доставок на маркетплейсе для подозреваемого.

    Аргументы:
        fraud_id: Уникальный идентификатор подозреваемого.

    Возвращает:
        Словарь, содержащий записи о доставках, или сообщение об их отсутствии.
    """
    async with EcosystemDB(DB_PATH) as db:
        market_data = await db.get_market_activity(fraud_id)

        if not market_data:
            return {"message": "Активность на маркетплейсе не найдена", "data": []}

        return {
            "data": [
                {
                    "address": m["address"],
                    "contact_name": m["contact_fio"],
                    "contact_phone": m["contact_phone"],
                    "date": m["event_date"]
                }
                for m in market_data
            ]
        }
