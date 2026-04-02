import os
import polars as pl

from dotenv import load_dotenv
from src.utils.database_utils import get_db_engine
from sqlalchemy import text

load_dotenv()

CLOUD_DATABASE_URL = os.getenv("CLOUD_DATABASE_URL")
LOCAL_DATABASE_URL = os.getenv("LOCAL_DATABASE_URL")

def sync_cloud_to_local():
    local_engine = get_db_engine("LOCAL_DATABASE_URL")
    sync_dim_stocks(local_engine)

    with local_engine.connect() as conn:
        result = conn.execute(text("SELECT MAX(extracted_at) FROM fact_stock_prices"))
        last_sync_date = result.scalar()

    print(f"Last local record found: {last_sync_date}")

    query = "SELECT * FROM fact_stock_prices"
    if last_sync_date:
        query += f" WHERE extracted_at >= '{last_sync_date}'"

    df_new_data = pl.read_database_uri(query, uri=CLOUD_DATABASE_URL)

    if df_new_data.is_empty():
        print("Everythin is up to date. No new data in cloud.")
        return
    
    print(f"Found {df_new_data.height} new rows in Cloud. Syncing...")
    
    new_max_date = df_new_data["extracted_at"].max()
    overlap_timestamps = df_new_data["extracted_at"].unique().to_list()

    with local_engine.begin() as conn:
        conn.execute(
            text("DELETE FROM fact_stock_prices WHERE extracted_at IN :ts"),
            {"ts": tuple(overlap_timestamps)}
        )
        print(f"Cleared {len(overlap_timestamps)} overlapping timestamps from local warehouse.")
    
    df_new_data.write_database(
        table_name="fact_stock_prices",
        connection=LOCAL_DATABASE_URL,
        if_table_exists="append",
        engine="adbc"
    )
    
    print("Sync complete..!")
    cleanup_cloud_storage(new_max_date)


def cleanup_cloud_storage(date):
    cloud_engine = get_db_engine("CLOUD_DATABASE_URL")

    with cloud_engine.begin() as conn:
        print(f"last sync date : {date}")
        result = conn.execute(
            text("DELETE from fact_stock_prices WHERE extracted_at <= :sync_date"),
            {"sync_date": date}
        )

        print(f"Cloud Cleanup: Removed {result.rowcount} old rows from online database")


def sync_dim_stocks(local_engine):
    df_dims = pl.read_database_uri("SELECT symbol, name, sector FROM dim_stocks", uri=CLOUD_DATABASE_URL)

    if df_dims.is_empty():
        return
    
    with local_engine.begin() as connection:
        for row in df_dims.to_dicts():
            connection.execute(text("""
                INSERT INTO dim_stocks (symbol, name, sector)
                VALUES (:symbol, :name, :sector)
                ON CONFLICT (symbol)
                DO UPDATE SET name = EXCLUDED.name, sector = EXCLUDED.sector, last_updated = CURRENT_TIMESTAMP;
            """), row)

    print(f"Synced {len(df_dims)} stock definitions to local warehouse")


if __name__ == "__main__":
    sync_cloud_to_local()