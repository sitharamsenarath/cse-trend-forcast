import os
import polars as pl

from dotenv import load_dotenv
from src.utils.database_utils import get_db_engine
from sqlalchemy import text

load_dotenv()

CLOUD_DATABASE_URL = os.getenv("CLOUD_DATABASE_URL")
LOCAL_DATABASE_URL = os.getenv("LOCAL_DATABASE_URL")

def sync_table(table_name, date_column, local_engine):
    
    with local_engine.connect() as conn:
        result = conn.execute(text(f"SELECT MAX({date_column}) FROM {table_name}"))
        last_sync_date = result.scalar()

    print(f"Syncing {table_name}. Last local record: {last_sync_date}")

    query = f"SELECT * FROM {table_name}"
    if last_sync_date:
        query += f" WHERE {date_column} >= '{last_sync_date}'"

    df_new_data = pl.read_database_uri(query, uri=CLOUD_DATABASE_URL)

    if df_new_data.is_empty():
        print("{table_name} is up to date.")
        return None
    
    print(f"Found {df_new_data.height} new rows in Cloud. Syncing...")
    
    new_max_date = df_new_data[date_column].max()
    overlap_timestamps = df_new_data[date_column].unique().to_list()

    with local_engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {table_name} WHERE {date_column} IN :ts"),
            {"ts": tuple(overlap_timestamps)}
        )
        print(f"Cleared {len(overlap_timestamps)} overlapping timestamps from local warehouse.")
    
    df_new_data.write_database(
        table_name=table_name,
        connection=LOCAL_DATABASE_URL,
        if_table_exists="append",
        engine="adbc"
    )
    
    print(f"Synced {df_new_data.height} rows to local {table_name}.")
    return df_new_data[date_column].max()


def cleanup_cloud(prices_max_date, sectors_max_date):
    cloud_engine = get_db_engine("CLOUD_DATABASE_URL")

    with cloud_engine.begin() as conn:
        if prices_max_date:
            conn.execute(text("DELETE FROM fact_stock_prices WHERE extracted_at <= :d"), {"d": prices_max_date})
        if sectors_max_date:
            conn.execute(text("DELETE FROM fact_sector_indices WHERE extracted_at <= :d"), {"d": sectors_max_date})

        print(f"Cloud buffer cleared.")


def sync_dim_stocks(local_engine):
    df_dims = pl.read_database_uri("SELECT * FROM dim_stocks", uri=CLOUD_DATABASE_URL)

    if df_dims.is_empty():
        return
    
    with local_engine.begin() as conn:
        for row in df_dims.to_dicts():
            conn.execute(text("""
                INSERT INTO dim_stocks (symbol, name, sector, isin, beta_value, market_cap_total, issued_quantity)
                VALUES (:symbol, :name, :sector, :isin, :beta_value, :market_cap_total, :issued_quantity)
                ON CONFLICT (symbol) DO UPDATE SET 
                    name = EXCLUDED.name, 
                    isin = EXCLUDED.isin,
                    beta_value = EXCLUDED.beta_value,
                    market_cap_total = EXCLUDED.market_cap_total,
                    issued_quantity = EXCLUDED.issued_quantity,
                    last_updated = CURRENT_TIMESTAMP;
            """), row)

    print(f"Synced {len(df_dims)} stock definitions to local warehouse")


def main():
    local_engine = get_db_engine("LOCAL_DATABASE_URL")

    sync_dim_stocks(local_engine)

    max_price_date = sync_table("fact_stock_prices", "extracted_at", local_engine)
    max_sector_date = sync_table("fact_sector_indices", "extracted_at", local_engine)

    cleanup_cloud(max_price_date, max_sector_date)


if __name__ == "__main__":
    main()