import pandas as pd
import numpy as np
import os
import re
from datetime import datetime

# --- Модуль 1: Загрузка данных ---
DATA_PATH = 'data/'

bank_clients = pd.read_csv(DATA_PATH + 'bank_clients.tsv', sep='\t')
bank_complaints = pd.read_csv(DATA_PATH + 'bank_complaints.tsv', sep='\t')
bank_transactions = pd.read_csv(DATA_PATH + 'bank_transactions.tsv', sep='\t')
ecosystem_mapping = pd.read_csv(DATA_PATH + 'ecosystem_mapping.tsv', sep='\t')
market_delivery = pd.read_csv(DATA_PATH + 'market_place_delivery.tsv', sep='\t')
mobile_build = pd.read_csv(DATA_PATH + 'mobile_build.tsv', sep='\t')
mobile_clients = pd.read_csv(DATA_PATH + 'mobile_clients.tsv', sep='\t')


# --- Модуль 2: Поиск мошенников по жалобе ---
def extract_amount_from_complaint(text):
    patterns = [
        r'(\d+)[\s]?р',  # 15000р
        r'(\d+)[\s]?руб',  # 15000руб
        r'(\d+)[\s]?рублей',  # 15000 рублей
        r'(\d+)[\s]?₽',  # 15000₽
        r'(\d+)[\s]?[\.,]?[\s]?р',  # 15000 р. или 15000,р
    ]
    for pattern in patterns:
        match = re.search(pattern, text.lower())
        if match:
            return int(match.group(1))

    if 'пропали' in text.lower():
        words = text.split()
        for i, word in enumerate(words):
            if word.lower() == 'пропали' and i + 1 < len(words):
                next_word = words[i + 1]
                numbers = re.findall(r'\d+', next_word)
                if numbers:
                    return int(numbers[0])
    return None


def find_fraud_by_complaint(complaint_row, bank_clients_df, bank_transactions_df):
    victim_id = complaint_row['uerId']
    complaint_text = complaint_row['text']
    complaint_date = complaint_row['event_date']

    amount = extract_amount_from_complaint(complaint_text)
    if not amount: return None

    victim_data = bank_clients_df[bank_clients_df['userId'] == victim_id]
    if victim_data.empty: return None

    victim_account = victim_data.iloc[0]['accout']
    victim_phone = victim_data.iloc[0]['phone']

    fraud_transaction = bank_transactions_df[
        (bank_transactions_df['account_out'] == victim_account) &
        (bank_transactions_df['value'] == amount)
        ]
    if fraud_transaction.empty: return None

    transaction = fraud_transaction.iloc[0]
    fraud_account = transaction['account_in']
    transaction_date = transaction['event_date']

    fraud_bank_owner = bank_clients_df[bank_clients_df['accout'] == fraud_account]

    return {
        'complaint_id': victim_id,
        'complaint_text': complaint_text,
        'complaint_date': complaint_date,
        'extracted_amount': amount,
        'victim_account': victim_account,
        'victim_phone': victim_phone,
        'transaction_date': transaction_date,
        'fraud_account': fraud_account,
        'fraud_bank_owner_id': fraud_bank_owner.iloc[0]['userId'] if not fraud_bank_owner.empty else None,
        'fraud_bank_owner_fio': fraud_bank_owner.iloc[0]['fio'] if not fraud_bank_owner.empty else None,
        'fraud_bank_owner_phone': fraud_bank_owner.iloc[0]['phone'] if not fraud_bank_owner.empty else None
    }


fraud_cases = []
for idx, complaint in bank_complaints.iterrows():
    case = find_fraud_by_complaint(complaint, bank_clients, bank_transactions)
    if case: fraud_cases.append(case)

fraud_cases_df = pd.DataFrame(fraud_cases)


# --- Модуль 3: Поиск телефонных связей ---
def find_calls_between(victim_phone, fraud_account, mobile_build_df, mobile_clients_df, ecosystem_mapping_df,
                       transaction_date):
    victim_phone_int = int(victim_phone)
    fraud_bank_owner = bank_clients[bank_clients['accout'] == fraud_account]
    if fraud_bank_owner.empty: return None
    fraud_bank_id = fraud_bank_owner.iloc[0]['userId']

    mapping = ecosystem_mapping_df[ecosystem_mapping_df['bank_id'] == fraud_bank_id]
    if mapping.empty: return None
    fraud_mobile_id = mapping.iloc[0]['mobile_user_id']
    if pd.isna(fraud_mobile_id): return None

    fraud_mobile = mobile_clients_df[mobile_clients_df['client_id'] == fraud_mobile_id]
    if fraud_mobile.empty: return None
    fraud_phone_int = int(fraud_mobile.iloc[0]['phone'])

    calls = mobile_build_df[
        ((mobile_build_df['from_call'] == victim_phone_int) & (mobile_build_df['to_call'] == fraud_phone_int)) |
        ((mobile_build_df['from_call'] == fraud_phone_int) & (mobile_build_df['to_call'] == victim_phone_int))
        ]
    if calls.empty: return None

    result = calls.copy()
    result['fraud_phone'] = fraud_phone_int
    result['fraud_mobile_id'] = fraud_mobile_id
    result['fraud_fio'] = fraud_mobile.iloc[0]['fio']
    return result


for idx, case in fraud_cases_df.iterrows():
    calls = find_calls_between(case['victim_phone'], case['fraud_account'], mobile_build, mobile_clients,
                               ecosystem_mapping, case['transaction_date'])
    fraud_cases_df.loc[idx, 'has_calls'] = 0 if calls is None else 1


# --- Модуль 4: Маркетплейсы ---
def find_market_activity(fraud_account, bank_clients_df, ecosystem_mapping_df, market_delivery_df):
    fraud_bank_owner = bank_clients_df[bank_clients_df['accout'] == fraud_account]
    if fraud_bank_owner.empty: return None
    fraud_bank_id = fraud_bank_owner.iloc[0]['userId']

    mapping = ecosystem_mapping_df[ecosystem_mapping_df['bank_id'] == fraud_bank_id]
    if mapping.empty: return None
    fraud_market_id = mapping.iloc[0]['market_plece_user_id']
    if pd.isna(fraud_market_id): return None

    deliveries = market_delivery_df[market_delivery_df['user_id'] == fraud_market_id]
    if deliveries.empty: return None

    result = deliveries.copy()
    result['fraud_bank_id'] = fraud_bank_id
    result['fraud_market_id'] = fraud_market_id
    return result


for idx, case in fraud_cases_df.iterrows():
    market_data = find_market_activity(case['fraud_account'], bank_clients, ecosystem_mapping, market_delivery)
    fraud_cases_df.loc[idx, 'has_market_activity'] = 0 if market_data is None else 1
    if market_data is not None:
        fraud_cases_df.loc[idx, 'market_deliveries_count'] = len(market_data)

# --- Модуль 5: Сохранение результатов ---
fraud_cases_df.to_csv(DATA_PATH + 'fraud_cases_detected.csv', index=False)

for idx, case in fraud_cases_df.iterrows():
    if case['has_market_activity']:
        market_data = find_market_activity(case['fraud_account'], bank_clients, ecosystem_mapping, market_delivery)
        if market_data is not None:
            market_data.to_csv(DATA_PATH + f"fraud_market_{case['fraud_account']}.csv", index=False)

    if case['has_calls']:
        calls = find_calls_between(case['victim_phone'], case['fraud_account'], mobile_build, mobile_clients,
                                   ecosystem_mapping, case['transaction_date'])
        if calls is not None:
            calls.to_csv(DATA_PATH + f"fraud_calls_{case['fraud_account']}.csv", index=False)


# --- Подготовка данных для Neo4j ---
def prepare_neo4j_data(fraud_cases_df, bank_clients_df, mobile_clients_df, ecosystem_mapping_df):
    neo4j_nodes, neo4j_edges = [], []
    for idx, case in fraud_cases_df.iterrows():
        neo4j_nodes.append({
            'id': f"victim_{case['complaint_id']}", 'type': 'person', 'role': 'victim',
            'bank_id': case['complaint_id'], 'account': case['victim_account'], 'phone': case['victim_phone']
        })
        if pd.notna(case['fraud_bank_owner_id']):
            neo4j_nodes.append({
                'id': f"fraud_{case['fraud_bank_owner_id']}", 'type': 'person', 'role': 'fraud',
                'bank_id': case['fraud_bank_owner_id'], 'account': case['fraud_account'],
                'phone': case['fraud_bank_owner_phone'], 'fio': case['fraud_bank_owner_fio']
            })
            neo4j_edges.append({
                'from': f"victim_{case['complaint_id']}", 'to': f"fraud_{case['fraud_bank_owner_id']}",
                'type': 'TRANSFERRED', 'amount': case['extracted_amount'], 'date': case['transaction_date']
            })
    return pd.DataFrame(neo4j_nodes), pd.DataFrame(neo4j_edges)


nodes_df, edges_df = prepare_neo4j_data(fraud_cases_df, bank_clients, mobile_clients, ecosystem_mapping)
nodes_df.to_csv(DATA_PATH + 'neo4j_nodes.csv', index=False)
edges_df.to_csv(DATA_PATH + 'neo4j_edges.csv', index=False)

# --- Вывод результатов ---
print(f"\nВсего жалоб в базе: {len(bank_complaints)}")
print(f" Найдено мошенников: {len(fraud_cases_df)}")

if len(fraud_cases_df) > 0:
    print("\nСписок мошенников:")
    print(fraud_cases_df[['complaint_id', 'extracted_amount', 'fraud_account', 'fraud_bank_owner_fio']])
    fraud_cases_df.to_csv('data/all_fraud_cases.csv', index=False)