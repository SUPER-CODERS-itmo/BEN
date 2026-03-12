"""
Модуль для сопоставления учетных записей пользователей из различных источников данных.
Выполняет объединение идентификаторов мобильного оператора, банка и маркетплейса
на основе нормализованных номеров телефонов.
"""

import pandas as pd
import re


def normalize_phone(phone):
    """
    Очищает номер телефона от любых символов, кроме цифр.

    Args:
        phone (str|float|None): Исходный номер телефона.

    Returns:
        str|None: Строка, состоящая только из цифр, или None, если входные данные пусты.
    """
    if pd.isna(phone):
        return None
    return re.sub(r'\D', '', str(phone))


def normalize_fio(fio):
    """
    Приводит ФИО к единому формату для сравнения: нижний регистр, без пробелов и точек.

    Args:
        fio (str|float|None): Исходная строка с ФИО.

    Returns:
        str: Нормализованная строка ФИО.
    """
    if pd.isna(fio):
        return ""
    return re.sub(r'[\s.]', '', str(fio)).lower()


registry = {}


def add_to_registry(uid, source_type, phone, fio):
    """
    Добавляет или обновляет информацию о пользователе в глобальном реестре.

    Использует нормализованный телефон в качестве ключа для связывания сущностей.
    Группирует ID из разных систем (mobile, bank, marketplace) под единым ключом.

    Args:
        uid (str|int): Идентификатор пользователя в системе-источнике.
        source_type (str): Тип источника ('mobile', 'bank' или 'market').
        phone (str): Номер телефона пользователя.
        fio (str): ФИО пользователя.
    """
    norm_p = normalize_phone(phone)
    if not norm_p:
        return

    if norm_p not in registry:
        registry[norm_p] = {'mobile_id': None, 'bank_id': None, 'marketplace_id': []}

    if source_type == 'mobile':
        registry[norm_p]['mobile_id'] = uid
    elif source_type == 'bank':
        registry[norm_p]['bank_id'] = uid
    elif source_type == 'market':
        if uid not in registry[norm_p]['marketplace_id']:
            registry[norm_p]['marketplace_id'].append(str(uid))


def run_entity_resolution():
    """
    Основная функция процесса обработки данных.
    Загружает файлы, наполняет реестр и сохраняет итоговый результат в CSV.
    """
    try:
        bank = pd.read_csv(r'tables\bank_clients.tsv', sep='\t')
        mobile = pd.read_csv(r'tables\mobile_clients.tsv', sep='\t')
        market = pd.read_csv(r'tables\market_place_delivery.tsv', sep='\t')
    except FileNotFoundError:
        print("Ошибка: Файлы данных не найдены.")
        return

    for _, row in mobile.iterrows():
        add_to_registry(row['client_id'], 'mobile', row['phone'], row['fio'])

    for _, row in bank.iterrows():
        add_to_registry(row['userId'], 'bank', row['phone'], row['fio'])

    for _, row in market.iterrows():
        add_to_registry(row['user_id'], 'market', row['contact_phone'], row['contact_fio'])

    output = []
    for i, (phone, ids) in enumerate(registry.items()):
        output.append({
            'unique_id': f'UID_{i + 1:03d}',
            'mobile_id': ids['mobile_id'] if ids['mobile_id'] is not None else r'\N',
            'bank_id': ids['bank_id'] if ids['bank_id'] is not None else r'\N',
            'marketplace_id': ", ".join(ids['marketplace_id']) if ids['marketplace_id'] else r'\N'
        })

    df_final = pd.DataFrame(output)
    print(df_final.to_string(index=False))
    df_final.to_csv(r'tables\resolved_entities.tsv', index=False, sep='\t')


if __name__ == "__main__":
    run_entity_resolution()
