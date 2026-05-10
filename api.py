"""Модуль API для системы анализа мошенничества (BEN API).

Предоставляет интерфейсы для работы с жалобами, проведения расследований
и получения полных профилей пользователей на основе данных экосистемы.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional

import aiosqlite
from fastapi import Depends, FastAPI, HTTPException, Query, Security
from fastapi.responses import RedirectResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import pandas as pd

# Предполагается, что код из предыдущего ответа находится в fraud_analysis.py
from fraud_analysis import FraudInvestigator

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="BEN API", version="1.2.0")
security = HTTPBearer()

# Пути к данным
DB_PATH = 'data/ecosystem_data.db'
COMPLAINTS_TSV = 'data/bank_complaints.tsv'
SECRET_TOKEN = "secret-token-123"


def verify_token(
    credentials: HTTPAuthorizationCredentials = Security(security)
) -> str:
    """Проверяет токен авторизации.

    Args:
        credentials: Учетные данные из заголовка Authorization.

    Returns:
        Идентификатор оператора при успешной проверке.

    Raises:
        HTTPException: Если токен невалиден (403).
    """
    if credentials.credentials != SECRET_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")
    return "operator_01"


def audit_log(user_id: str, action: str) -> None:
    """Записывает действие пользователя в аудит-лог.

    Args:
        user_id: Идентификатор пользователя (оператора).
        action: Описание совершенного действия.
    """
    logger.info("[AUDIT] %s | Action: %s", user_id, action)


def read_complaints_safe() -> pd.DataFrame:
    """Безопасно читает данные из TSV файла с жалобами.

    Returns:
        DataFrame с данными жалоб.

    Raises:
        HTTPException: Если файл базы данных жалоб не найден (500).
    """
    if not os.path.exists(COMPLAINTS_TSV):
        logger.error("Complaints file not found at %s", COMPLAINTS_TSV)
        raise HTTPException(status_code=500, detail="Complaints file not found")

    return pd.read_csv(COMPLAINTS_TSV, sep='\t')


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    """Перенаправляет корневой запрос на документацию API (Swagger)."""
    return RedirectResponse(url="/docs")


@app.get("/complaints", dependencies=[Depends(verify_token)])
async def get_complaints(
    start_date: Optional[str] = Query(None, description="Format: YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="Format: YYYY-MM-DD"),
    skip: int = 0,
    limit: int = 20
) -> List[Dict[str, Any]]:
    """Возвращает список жалоб с фильтрацией по дате.

    Args:
        start_date: Начальная дата (включительно).
        end_date: Конечная дата (включительно).
        skip: Смещение для пагинации.
        limit: Лимит выдачи.

    Returns:
        Список записей о жалобах.
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
        complaint_id: Идентификатор жалобы.

    Returns:
        Словарь с идентификатором и текстом.

    Raises:
        HTTPException: Если жалоба с таким ID не найдена.
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
    """Запускает процесс автоматизированного расследования по жалобе.

    Args:
        complaint_id: Идентификатор жалобы для анализа.
        user_id: ID оператора (извлекается автоматически из токена).

    Returns:
        Данные о транзакции и ID предполагаемого мошенника.

    Raises:
        HTTPException: Если не удалось найти транзакцию по жалобе.
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
        Список записей о звонках.

    Raises:
        HTTPException: Если контактные данные участников отсутствуют в системе.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # 1. Получаем телефоны участников
        phone_query = "SELECT phone FROM bank_clients WHERE userId = ?"
        async with db.execute(phone_query, (fraud_id,)) as cursor:
            f_row = await cursor.fetchone()
        async with db.execute(phone_query, (victim_id,)) as cursor:
            v_row = await cursor.fetchone()

        if not f_row or not v_row:
            raise HTTPException(status_code=404, detail="Phones not found")

        # 2. Ищем звонки в обоих направлениях
        f_phone, v_phone = f_row['phone'], v_row['phone']
        calls_query = """
            SELECT from_call AS 'from', to_call AS 'to', 
                   duration_sec AS duration, event_date AS date 
            FROM mobile_build 
            WHERE (from_call = ? AND to_call = ?) 
               OR (from_call = ? AND to_call = ?)
        """
        async with db.execute(calls_query, (f_phone, v_phone, v_phone, f_phone)) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


@app.get("/cases/{fraud_id}/delivery", dependencies=[Depends(verify_token)])
async def get_delivery(fraud_id: str) -> Dict[str, Any]:
    """Получает данные о доставках маркетплейса для аккаунта.

    Args:
        fraud_id: Банковский ID пользователя.

    Returns:
        Словарь со списком доставок и мета-сообщением.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

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
        async with db.execute(delivery_query, (mapping['marketplace_id'],)) as cursor:
            rows = await cursor.fetchall()
            return {"data": [dict(r) for r in rows]}


@app.get("/full-profile/{bank_id}", dependencies=[Depends(verify_token)])
async def get_full_profile_endpoint(bank_id: str) -> Dict[str, Any]:
    """Получает полный профиль пользователя (единое окно).

    Args:
        bank_id: Банковский идентификатор пользователя.

    Returns:
        Комплексный объект профиля: транзакции, заказы, звонки, теги.

    Raises:
        HTTPException: Если пользователь с таким ID не найден.
    """
    investigator = FraudInvestigator(DB_PATH, COMPLAINTS_TSV)
    profile = await investigator.fetch_full_user_profile(bank_id)

    if not profile:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    return profile


@app.get("/frauds", dependencies=[Depends(verify_token)])
async def get_frauds(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    skip: int = 0,
    limit: int = 10
) -> List[Dict[str, Any]]:
    """Возвращает список полных профилей мошенников, выявленных по жалобам.

    Проходит по жалобам, находит связанную транзакцию и выгружает профиль мошенника.

    Args:
        start_date: Фильтр даты жалобы (начало).
        end_date: Фильтр даты жалобы (конец).
        skip: Смещение.
        limit: Количество профилей.

    Returns:
        Список детализированных профилей мошенников.
    """
    df = read_complaints_safe()

    if start_date:
        df = df[df['event_date'] >= start_date]
    if end_date:
        df = df[df['event_date'] <= f"{end_date} 23:59:59"]

    investigator = FraudInvestigator(DB_PATH, COMPLAINTS_TSV)
    results = []
    victim_ids = df.iloc[skip: skip + limit]['userId'].tolist()

    for v_id in victim_ids:
        res_json = await investigator.investigate_single_case(str(v_id))
        res_data = json.loads(res_json)

        if "fraud_bank_id" in res_data:
            fraud_bank_id = res_data["fraud_bank_id"]
            full_profile = await investigator.fetch_full_user_profile(fraud_bank_id)
            if full_profile:
                results.append(full_profile)

    return results