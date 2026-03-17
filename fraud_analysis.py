import sqlite3
import pandas as pd
import re

DB_PATH = 'data/ecosystem_data.db'
DATA_PATH = 'data/'
COMPLAINTS_FILE = 'data/bank_complaints.tsv'

conn = sqlite3.connect(DB_PATH)


def extract_amount_from_complaint(text):
    patterns = [
        r'(\d+)[\s]?р',
        r'(\d+)[\s]?руб',
        r'(\d+)[\s]?рублей',
        r'(\d+)[\s]?₽',
        r'(\d+)[\s]?[\.,]?[\s]?р',
    ]
    for pattern in patterns:
        match = re.search(pattern, str(text).lower())
        if match:
            return int(match.group(1))

    if 'пропали' in str(text).lower():
        words = str(text).split()
        for i, word in enumerate(words):
            if word.lower() == 'пропали' and i + 1 < len(words):
                numbers = re.findall(r'\d+', words[i + 1])
                if numbers:
                    return int(numbers[0])
    return None


def find_fraud_by_complaint(cursor, victim_id, complaint_text, complaint_date):
    amount = extract_amount_from_complaint(complaint_text)
    if not amount:
        return None

    query = """
        SELECT 
            v.account as victim_account,
            v.phone as victim_phone,
            t.account_in as fraud_account,
            t.event_date as transaction_date,
            f.userId as fraud_bank_id,
            f.fio as fraud_fio,
            f.phone as fraud_phone
        FROM bank_clients v
        JOIN bank_transactions t ON t.account_out = v.account
        LEFT JOIN bank_clients f ON f.account = t.account_in
        WHERE v.userId = ? AND t.value = ?
        LIMIT 1
    """
    cursor.execute(query, (victim_id, amount))
    row = cursor.fetchone()

    if not row:
        return None

    return {
        'complaint_id': victim_id,
        'complaint_text': complaint_text,
        'complaint_date': complaint_date,
        'extracted_amount': amount,
        'victim_account': row[0],
        'victim_phone': row[1],
        'fraud_account': row[2],
        'transaction_date': row[3],
        'fraud_bank_owner_id': row[4],
        'fraud_bank_owner_fio': row[5],
        'fraud_bank_owner_phone': row[6]
    }


def find_calls_between(cursor, victim_phone, fraud_account):
    query = """
        SELECT 
            mb.event_date, mb.from_call, mb.to_call, mb.duration_sec,
            mc.phone as fraud_phone, mc.client_id as fraud_mobile_id, mc.fio as fraud_fio
        FROM bank_clients bc
        JOIN ecosystem_mapping em ON em.bank_id = bc.userId
        JOIN mobile_clients mc ON mc.client_id = em.mobile_id
        JOIN mobile_build mb ON 
            (mb.from_call = mc.phone AND mb.to_call = ?) OR 
            (mb.from_call = ? AND mb.to_call = mc.phone)
        WHERE bc.account = ?
    """

    cursor.execute(query, (victim_phone, victim_phone, fraud_account))
    columns = [column[0] for column in cursor.description] if cursor.description else []
    calls = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return calls if calls else None


def find_market_activity(cursor, fraud_account):
    query = """
        SELECT 
            md.event_date, md.user_id as fraud_market_id, md.contact_fio, md.contact_phone, md.address,
            bc.userId as fraud_bank_id
        FROM bank_clients bc
        JOIN ecosystem_mapping em ON em.bank_id = bc.userId
        JOIN market_place_delivery md ON md.user_id = em.marketplace_id
        WHERE bc.account = ?
    """
    cursor.execute(query, (fraud_account,))
    columns = [column[0] for column in cursor.description] if cursor.description else []
    deliveries = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return deliveries if deliveries else None


cursor = conn.cursor()

print("Загрузка жалоб...")
bank_complaints = pd.read_csv(COMPLAINTS_FILE, sep='\t')

fraud_cases = []

print("Поиск мошенников...")
for idx, complaint in bank_complaints.iterrows():
    victim_id = complaint.get('uerId', complaint.get('userId'))

    case = find_fraud_by_complaint(cursor, victim_id, complaint['text'], complaint['event_date'])

    if case:
        calls = find_calls_between(cursor, case['victim_phone'], case['fraud_account'])
        case['has_calls'] = 1 if calls else 0
        case['calls_data'] = calls

        market_data = find_market_activity(cursor, case['fraud_account'])
        case['has_market_activity'] = 1 if market_data else 0
        case['market_deliveries_count'] = len(market_data) if market_data else 0
        case['market_data'] = market_data

        fraud_cases.append(case)

conn.close()

if fraud_cases:
    print(f"Найдено мошенников: {len(fraud_cases)}")

    df_main = pd.DataFrame(fraud_cases).drop(columns=['calls_data', 'market_data'])
    df_main.to_csv(DATA_PATH + 'fraud_cases_detected.csv', index=False)

    for case in fraud_cases:
        f_acc = case['fraud_account']
        if case['has_market_activity']:
            pd.DataFrame(case['market_data']).to_csv(DATA_PATH + f'fraud_market_{f_acc}.csv', index=False)
        if case['has_calls']:
            pd.DataFrame(case['calls_data']).to_csv(DATA_PATH + f'fraud_calls_{f_acc}.csv', index=False)

    neo4j_nodes, neo4j_edges = [], []
    for case in fraud_cases:
        neo4j_nodes.append({
            'id': f"victim_{case['complaint_id']}", 'type': 'person', 'role': 'victim',
            'bank_id': case['complaint_id'], 'account': case['victim_account'], 'phone': case['victim_phone']
        })
        if case['fraud_bank_owner_id']:
            neo4j_nodes.append({
                'id': f"fraud_{case['fraud_bank_owner_id']}", 'type': 'person', 'role': 'fraud',
                'bank_id': case['fraud_bank_owner_id'], 'account': case['fraud_account'],
                'phone': case['fraud_bank_owner_phone'], 'fio': case['fraud_bank_owner_fio']
            })
            neo4j_edges.append({
                'from': f"victim_{case['complaint_id']}", 'to': f"fraud_{case['fraud_bank_owner_id']}",
                'type': 'TRANSFERRED', 'amount': case['extracted_amount'], 'date': case['transaction_date']
            })

    pd.DataFrame(neo4j_nodes).to_csv(DATA_PATH + 'neo4j_nodes.csv', index=False)
    pd.DataFrame(neo4j_edges).to_csv(DATA_PATH + 'neo4j_edges.csv', index=False)

    print("Все результаты успешно сохранены в папку data/")
else:
    print("Мошенники не найдены.")
