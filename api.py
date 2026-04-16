"""Модуль API для системы анализа мошенничества (BEN API)."""

import json
import logging
import os
from typing import Any, Dict, List, Optional

import aiosqlite
from fastapi import Depends, FastAPI, HTTPException, Query, Security
from fastapi.responses import RedirectResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import pandas as pd

from fraud_analysis import FraudInvestigator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="BEN API", version="1.2.0")
security = HTTPBearer()

DB_PATH = 'data/ecosystem_data.db'
COMPLAINTS_TSV = 'data/bank_complaints.tsv'


def verify_token(
    credentials: HTTPAuthorizationCredentials = Security(security)
) -> str:
    """Проверяет токен авторизации.

    Args:
        credentials: Учетные данные из заголовка Authorization.

    Returns:
        Идентификатор оператора при успешной проверке.

    Raises:
        HTTPException: Если токен невалиден.
    """
    if credentials.credentials != "secret-token-123":
        raise HTTPException(status_code=403, detail="Invalid token")
    return "operator_01"


def audit_log(user_id: str, action: str) -> None:
    """Записывает действие пользователя в аудит-лог.

    Args:
        user_id: Идентификатор пользователя.
        action: Описание совершенного действия.
    """
    logger.info("[AUDIT] %s | Action: %s", user_id, action)


def read_complaints_safe() -> pd.DataFrame:
    """Безопасно читает данные из TSV файла с жалобами.

    Returns:
        DataFrame с данными жалоб.

    Raises:
        HTTPException: Если файл не найден.
    """
    if not os.path.exists(COMPLAINTS_TSV):
        raise HTTPException(status_code=500, detail="TSV file not found")

    return pd.read_csv(COMPLAINTS_TSV, sep='\t')


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    """Перенаправляет на документацию API."""
    return RedirectResponse(url="/docs")


@app.get("/complaints", dependencies=[Depends(verify_token)])
async def get_complaints(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    skip: int = 0,
    limit: int = 20
) -> List[Dict[str, Any]]:
    """Возвращает список жалоб с фильтрацией по дате.

    Args:
        start_date: Начальная дата в формате YYYY-MM-DD.
        end_date: Конечная дата в формате YYYY-MM-DD.
        skip: Количество пропускаемых записей.
        limit: Максимальное количество записей.

    Returns:
        Список словарей с данными жалоб.
    """
    df = read_complaints_safe()

    if start_date:
        df = df[df['event_date'] >= start_date]
    if end_date:
        df = df[df['event_date'] <= f"{end_date} 23:59:59"]

    return df.iloc[skip: skip + limit].to_dict(orient='records')


@app.get("/complaints/{complaint_id}", dependencies=[Depends(verify_token)])
async def get_complaint_text(complaint_id: str) -> Dict[str, str]:
    """Получает текст конкретной жалобы по ID.

    Args:
        complaint_id: Идентификатор жалобы (userId в TSV).

    Returns:
        Словарь с ID и текстом жалобы.

    Raises:
        HTTPException: Если жалоба не найдена.
    """
    df = read_complaints_safe()
    complaint = df[df['userId'].astype(str) == str(complaint_id)]

    if complaint.empty:
        raise HTTPException(status_code=404, detail="Complaint not found")

    return {"id": complaint_id, "text": complaint.iloc[0]['text']}


@app.post("/investigate/{complaint_id}")
async def investigate(
    complaint_id: str,
    user_id: str = Depends(verify_token)
) -> Dict[str, Any]:
    """Запускает процесс расследования по жалобе.

    Args:
        complaint_id: Идентификатор жалобы.
        user_id: ID оператора (из токена).

    Returns:
        Результаты расследования в виде словаря.

    Raises:
        HTTPException: Если в процессе расследования возникла ошибка.
    """
    investigator = FraudInvestigator(DB_PATH, COMPLAINTS_TSV)
    res_json = await investigator.investigate_single_case(complaint_id)
    result = json.loads(res_json)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    audit_log(user_id, f"Investigated complaint #{complaint_id}")
    return result


@app.get("/cases/{fraud_id}/calls", dependencies=[Depends(verify_token)])
async def get_calls(
    fraud_id: str,
    victim_id: str
) -> List[Dict[str, Any]]:
    """Получает историю звонков между предполагаемым мошенником и жертвой.

    Args:
        fraud_id: ID мошенника.
        victim_id: ID жертвы.

    Returns:
        Список звонков.

    Raises:
        HTTPException: Если телефоны участников не найдены.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # 1. Получаем телефонные номера участников
        async with db.execute(
            "SELECT phone FROM bank_clients WHERE userId = ?", (fraud_id,)
        ) as cursor:
            f_row = await cursor.fetchone()

        async with db.execute(
            "SELECT phone FROM bank_clients WHERE userId = ?", (victim_id,)
        ) as cursor:
            v_row = await cursor.fetchone()

        if not f_row or not v_row:
            raise HTTPException(status_code=404, detail="Phones not found")

        # 2. Поиск звонков в базе данных мобильного оператора
        f_phone, v_phone = f_row['phone'], v_row['phone']
        query = """
            SELECT from_call AS 'from', to_call AS 'to', 
                   duration_sec AS duration, event_date AS date 
            FROM mobile_build 
            WHERE (from_call = ? AND to_call = ?) 
               OR (from_call = ? AND to_call = ?)
        """
        async with db.execute(query, (f_phone, v_phone, v_phone, f_phone)) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


@app.get("/cases/{fraud_id}/delivery", dependencies=[Depends(verify_token)])
async def get_delivery(fraud_id: str) -> Dict[str, Any]:
    """Получает данные о доставках маркетплейса для указанного ID.

    Args:
        fraud_id: Банковский ID пользователя.

    Returns:
        Словарь со списком доставок или сообщением об их отсутствии.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # Маппинг банковского ID на ID маркетплейса
        mapping_query = (
            "SELECT marketplace_id FROM ecosystem_mapping WHERE bank_id = ?"
        )
        async with db.execute(mapping_query, (fraud_id,)) as cursor:
            mapping = await cursor.fetchone()

        if not mapping:
            return {"data": [], "message": "No marketplace account found"}

        delivery_query = """
            SELECT address, contact_fio, contact_phone, event_date AS date 
            FROM market_place_delivery 
            WHERE user_id = ?
        """
        async with db.execute(
            delivery_query, (mapping['marketplace_id'],)
        ) as cursor:
            rows = await cursor.fetchall()
            return {"data": [dict(r) for r in rows]}