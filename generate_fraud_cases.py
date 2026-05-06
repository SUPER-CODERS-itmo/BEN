"""Генератор fraud_cases_detected.csv.

Прогоняет все жалобы из bank_complaints.tsv через FraudInvestigator
и сохраняет результаты в data/fraud_cases_detected.csv.

Запуск:
    python generate_fraud_cases.py
"""

import asyncio
import logging
import sqlite3

import pandas as pd

from fraud_analysis import FraudInvestigator, AmountExtractor

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

DB_PATH         = 'data/ecosystem_data.db'
COMPLAINTS_TSV  = 'data/bank_complaints.tsv'
OUTPUT_CSV      = 'data/fraud_cases_detected.csv'


def get_extra_info(db_path: str, fraud_bank_id: str, victim_id: str) -> dict:
    """Получает дополнительные данные о мошеннике из БД.

    Args:
        db_path:       Путь к ecosystem_data.db.
        fraud_bank_id: userId мошенника.
        victim_id:     userId жертвы.

    Returns:
        Словарь с account, phone мошенника и флагами has_calls, has_market_activity.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    result = {
        'fraud_account':          None,
        'victim_phone':           None,
        'fraud_bank_owner_phone': None,
        'has_calls':              0,
        'has_market_activity':    0,
    }

    try:
        # Счёт и телефон мошенника
        row = conn.execute(
            "SELECT account, phone FROM bank_clients WHERE userId = ?",
            (fraud_bank_id,)
        ).fetchone()
        if row:
            result['fraud_account']          = row['account']
            result['fraud_bank_owner_phone'] = row['phone']

        # Телефон жертвы
        v_row = conn.execute(
            "SELECT account, phone FROM bank_clients WHERE userId = ?",
            (victim_id,)
        ).fetchone()
        if v_row:
            result['victim_phone'] = v_row['phone']

        # Флаг звонков
        if result['fraud_bank_owner_phone'] and result['victim_phone']:
            f_phone = result['fraud_bank_owner_phone']
            v_phone = result['victim_phone']
            calls = conn.execute(
                "SELECT COUNT(*) FROM mobile_build "
                "WHERE (from_call=? AND to_call=?) OR (from_call=? AND to_call=?)",
                (f_phone, v_phone, v_phone, f_phone)
            ).fetchone()[0]
            result['has_calls'] = 1 if calls > 0 else 0

        # Флаг маркетплейса
        mapping = conn.execute(
            "SELECT marketplace_id FROM ecosystem_mapping WHERE bank_id = ?",
            (fraud_bank_id,)
        ).fetchone()
        if mapping:
            deliveries = conn.execute(
                "SELECT COUNT(*) FROM market_place_delivery WHERE user_id = ?",
                (mapping['marketplace_id'],)
            ).fetchone()[0]
            result['has_market_activity'] = 1 if deliveries > 0 else 0

    finally:
        conn.close()

    return result


async def generate() -> None:
    """Основная функция генерации CSV."""
    df = pd.read_csv(COMPLAINTS_TSV, sep='\t')
    extractor   = AmountExtractor()
    investigator = FraudInvestigator(DB_PATH, COMPLAINTS_TSV)

    rows = []
    total = len(df)

    for i, row in df.iterrows():
        victim_id      = row['userId']
        complaint_text = row['text']
        complaint_date = row['event_date']
        amount         = extractor.extract(complaint_text)

        logger.info("[%d/%d] Расследую %s...", i + 1, total, victim_id)

        import json
        res_json = await investigator.investigate_single_case(victim_id)
        result   = json.loads(res_json)

        if 'error' in result:
            logger.warning("Пропускаю %s: %s", victim_id, result['error'])
            continue

        tx            = result['transaction_info']
        fraud_bank_id = result['fraud_bank_id']
        extra         = get_extra_info(DB_PATH, fraud_bank_id, victim_id)

        # Получаем ФИО мошенника
        conn = sqlite3.connect(DB_PATH)
        fio_row = conn.execute(
            "SELECT fio FROM bank_clients WHERE userId = ?", (fraud_bank_id,)
        ).fetchone()
        fraud_fio = fio_row[0] if fio_row else None
        conn.close()

        rows.append({
            'complaint_id':           victim_id,
            'complaint_text':         complaint_text,
            'complaint_date':         complaint_date,
            'extracted_amount':       amount,
            'victim_account':         None,
            'victim_phone':           extra['victim_phone'],
            'fraud_account':          extra['fraud_account'],
            'transaction_date':       tx['when'],
            'fraud_bank_owner_id':    fraud_bank_id,
            'fraud_bank_owner_fio':   fraud_fio,
            'fraud_bank_owner_phone': extra['fraud_bank_owner_phone'],
            'has_calls':              extra['has_calls'],
            'has_market_activity':    extra['has_market_activity'],
        })

    out = pd.DataFrame(rows)
    out.to_csv(OUTPUT_CSV, index=False)
    logger.info("Готово! Сохранено %d кейсов в %s", len(rows), OUTPUT_CSV)


if __name__ == "__main__":
    asyncio.run(generate())
