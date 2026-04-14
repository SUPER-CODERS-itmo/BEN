import aiosqlite
import pandas as pd
from pathlib import Path
from fastapi import FastAPI, Depends, HTTPException, Query, Header
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from typing import List, Optional

from fraud_analysis import EcosystemDB, AmountExtractor

app = FastAPI(
    title="BEN API",
    description="BEN",
    version="1.0.0"
)

DB_PATH = 'data/ecosystem_data.db'
COMPLAINTS_TSV = 'data/bank_complaints.tsv'


@app.on_event("startup")
async def startup_event():
    """Переносим TSV в SQLite асинхронно."""
    if not Path(COMPLAINTS_TSV).exists() or not Path(DB_PATH).exists():
        print("Внимание: База данных или TSV файл не найдены. Сначала запусти db_creator.py!")
        return

    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS complaints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                victim_bank_id TEXT,
                text TEXT,
                event_date TEXT,
                status TEXT DEFAULT 'New'
            )
        """)

        cursor = await conn.execute("SELECT COUNT(*) FROM complaints")
        row = await cursor.fetchone()

        if row[0] == 0:
            df = pd.read_csv(COMPLAINTS_TSV, sep='\t')
            for _, row_df in df.iterrows():
                user_id = row_df.get('userId') if pd.notnull(row_df.get('userId')) else row_df.get('uerId')
                await conn.execute(
                    "INSERT INTO complaints (victim_bank_id, text, event_date, status) VALUES (?, ?, ?, ?)",
                    (user_id, row_df.get('text'), row_df.get('event_date'), 'New')
                )
            await conn.commit()
            print("Жалобы успешно загружены в базу SQLite!")


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")


def verify_token(authorization: Optional[str] = Header(None)):
    if authorization is None or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Отсутствует или неверный токен (ожидается 'Bearer <token>')")

    token = authorization.split(" ")[1]
    if token != "secret-token-123":
        raise HTTPException(status_code=401, detail="Неверный токен")

    return "operator_01"


def audit_log(user_id: str, action: str):
    print(f"[AUDIT LOG] User: {user_id} | Action: {action}")


@app.get("/complaints", dependencies=[Depends(verify_token)])
async def get_complaints(
        start_date: Optional[str] = Query(None, description="Дата от (YYYY-MM-DD)"),
        end_date: Optional[str] = Query(None, description="Дата до (YYYY-MM-DD)"),
        skip: int = Query(0, ge=0),
        limit: int = Query(20, le=100)
):
    async with EcosystemDB(DB_PATH) as db:
        query = "SELECT * FROM complaints WHERE 1=1"
        params = []

        if start_date:
            query += " AND event_date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND event_date <= ?"
            params.append(end_date + " 23:59:59")

        query += " LIMIT ? OFFSET ?"
        params.extend([limit, skip])

        cursor = await db.conn.cursor()
        await cursor.execute(query, params)
        rows = await cursor.fetchall()

        return [dict(row) for row in rows]


@app.get("/complaints/{complaint_id}", dependencies=[Depends(verify_token)])
async def get_complaint(complaint_id: int):
    async with EcosystemDB(DB_PATH) as db:
        cursor = await db.conn.cursor()
        await cursor.execute("SELECT * FROM complaints WHERE id = ?", (complaint_id,))
        row = await cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Жалоба не найдена")
        return dict(row)


@app.post("/investigate/{complaint_id}")
async def investigate_complaint(complaint_id: int, user_id: str = Depends(verify_token)):
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
            raise HTTPException(status_code=400, detail="Не удалось извлечь сумму из текста жалобы")

        trans = await db.find_transaction_info(v_bank_id, amount)
        if not trans:
            raise HTTPException(status_code=404, detail="Подозрительная транзакция не найдена")

        await cursor.execute("UPDATE complaints SET status = 'Processed' WHERE id = ?", (complaint_id,))
        await db.conn.commit()

        audit_log(user_id, action=f"Investigated complaint #{complaint_id}, Fraud found: {trans['fraud_bank_id']}")

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
async def get_fraud_calls(fraud_id: str, victim_id: str):
    async with EcosystemDB(DB_PATH) as db:
        cursor = await db.conn.cursor()

        await cursor.execute("SELECT phone FROM bank_clients WHERE userId = ?", (fraud_id,))
        f_row = await cursor.fetchone()
        await cursor.execute("SELECT phone FROM bank_clients WHERE userId = ?", (victim_id,))
        v_row = await cursor.fetchone()

        if not f_row or not v_row:
            raise HTTPException(status_code=404, detail="Не удалось найти номера телефонов по ID")

        fraud_phone = str(f_row['phone'])
        victim_phone = str(v_row['phone'])

        calls = await db.get_calls(victim_phone, fraud_phone)

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
async def get_fraud_delivery(fraud_id: str):
    async with EcosystemDB(DB_PATH) as db:
        market_data = await db.get_market_activity(fraud_id)

        if not market_data:
            return {"message": "Активность на маркетплейсе не найдена", "data": []}

        return [
            {
                "address": m["address"],
                "contact_name": m["contact_fio"],
                "contact_phone": m["contact_phone"],
                "date": m["event_date"]
            }
            for m in market_data
        ]
