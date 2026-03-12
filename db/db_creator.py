import pandas as pd
import sqlite3
from pathlib import Path

BASE_DIR = Path("tables")
DATABASE_NAME = "ecosystem_data.db"
DB_PATH = BASE_DIR / DATABASE_NAME


def load_data(file_name, rename_cols=None):
    """
    Выполняет загрузку данных из TSV-файла, нормализует заголовки и обрабатывает пропуски.

    Args:
        file_name (str): Имя файла в директории BASE_DIR.
        rename_cols (dict, optional): Словарь для переименования столбцов (исправление опечаток).

    Returns:
        pd.DataFrame or None: Очищенный DataFrame или None, если файл не найден.
    """
    file_path = BASE_DIR / file_name
    if not file_path.exists():
        return None

    df = pd.read_csv(file_path, sep='\t', na_values='\\N')

    if rename_cols:
        df = df.rename(columns=rename_cols)

    df.columns = [c.strip() for c in df.columns]
    return df


def main():
    """
    Основной управляющий процесс: загрузка, объединение данных в "Золотой профиль"
    и сохранение в SQLite.
    """
    BASE_DIR.mkdir(parents=True, exist_ok=True)

    mapping = load_data("resolved_entities.tsv")

    bank_clients = load_data("bank_clients.tsv", rename_cols={
        'uerId': 'userId',
        'accout': 'account'
    })

    mobile_clients = load_data("mobile_clients.tsv")
    market_delivery = load_data("market_place_delivery.tsv")
    bank_tx = load_data("bank_transactions.tsv")
    mobile_calls = load_data("mobile_build.tsv")

    master_df = mapping.copy() if mapping is not None else pd.DataFrame()

    if mobile_clients is not None:
        master_df = master_df.merge(
            mobile_clients,
            left_on='mobile_id',
            right_on='client_id',
            how='left'
        ).drop(columns=['client_id'], errors='ignore')

    if bank_clients is not None:
        master_df = master_df.merge(
            bank_clients,
            left_on='bank_id',
            right_on='userId',
            how='left',
            suffixes=('_mobile', '_bank')
        ).drop(columns=['userId'], errors='ignore')

    if market_delivery is not None:
        market_users = market_delivery.sort_values('event_date').drop_duplicates('user_id', keep='last')
        master_df = master_df.merge(
            market_users,
            left_on='marketplace_id',
            right_on='user_id',
            how='left',
            suffixes=('', '_market')
        ).drop(columns=['user_id'], errors='ignore')

    conn = sqlite3.connect(DB_PATH)

    master_df.to_sql("unified_users", conn, if_exists="replace", index=False)

    tables_to_save = {
        "bank_clients": bank_clients,
        "bank_transactions": bank_tx,
        "market_place_delivery": market_delivery,
        "mobile_build": mobile_calls,
        "mobile_clients": mobile_clients,
        "ecosystem_mapping": mapping
    }

    for table_name, df in tables_to_save.items():
        if df is not None:
            df.to_sql(table_name, conn, if_exists="replace", index=False)

    conn.close()


if __name__ == "__main__":
    main()