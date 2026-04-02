from datetime import datetime
import os

import requests
import polars as pl
from src.ingestion.models import SectorFact, StockPriceFact
from sqlalchemy import text
from src.utils.database_utils import get_db_engine


CLOUD_DATABASE_URL = os.getenv("CLOUD_DATABASE_URL")
BASE_URL = "https://www.cse.lk/api"

def fetch_fast_sync_data():
    print(f"Fetching Daily Market Data: {datetime.now().date()}")

    resp_stocks = requests.post(f"{BASE_URL}/tradeSummary")
    resp_stocks.raise_for_status()
    raw_stocks = resp_stocks.json().get("reqTradeSummery", [])

    resp_sectors = requests.post(f"{BASE_URL}/allSectors")
    resp_sectors.raise_for_status()
    raw_sectors = resp_sectors.json()

    print(raw_sectors)

    valid_stocks = [StockPriceFact(**st).model_dump() for st in raw_stocks]
    valid_sectors = [SectorFact(**sec).model_dump() for sec in raw_sectors]

    return valid_stocks, valid_sectors


def enrich_slow_sync_metadata(engine, symbols):
    print(f"Running Slow Sync (Metadata) for {len(symbols)} symbols...")

    for symbol in symbols:
        try:
            resp = requests.post(f"{BASE_URL}/companyInfoSummery?symbol={symbol}")
            if resp.status_code == 200:
                data = resp.json()
                info = data.get("reqSymbolInfo", {})
                beta = data.get("reqSymbolBetaInfo", {})

                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE dim_stocks SET 
                            isin = :isin, 
                            beta_value = :beta,
                            market_cap_total = :mcap,
                            issued_quantity = :qty,
                            last_updated = CURRENT_TIMESTAMP
                        WHERE symbol = :s
                    """), {
                        "isin": info.get("isin"),
                        "beta": beta.get("triASIBetaValue"),
                        "mcap": info.get("marketCap"),
                        "qty": info.get("quantityIssued"),
                        "s": symbol
                    })
        except Exception as e:
            print(f"Metadata sync failed for {symbol}: {e}")


def run_pipeline(force_slow_sync: bool = False):
    stocks, sectors = fetch_fast_sync_data()
    engine = get_db_engine("CLOUD_DATABASE_URL")

    with engine.begin() as conn:
        for s in stocks:
            conn.execute(text("""
                INSERT INTO dim_stocks (symbol, name)
                VALUES (:s, :n)
                ON CONFLICT (symbol) DO UPDATE SET name = EXCLUDED.name;
            """), {"s":s['symbol'], "n":s['name']})

        today = datetime.now().strftime('%Y-%m-%d')
        conn.execute(text("DELETE FROM fact_stock_prices WHERE extracted_at::date = :d"), {"d": today})
        conn.execute(text("DELETE FROM fact_sector_indices WHERE extracted_at::date = :d"), {"d": today})
    
    if stocks:
        df_stocks = pl.DataFrame(stocks)
        
        df_stocks = df_stocks.with_columns([
            pl.col("price").cast(pl.Float64),
            pl.col("volume").cast(pl.Int64), 
            pl.col("trade_count").cast(pl.Int32), 
            pl.col("turnover").cast(pl.Float64)
        ])

        df_stocks.write_database(
            table_name="fact_stock_prices",
            connection=CLOUD_DATABASE_URL,
            engine="adbc",
            if_table_exists="append"
        )
        print(f"Inserted {len(stocks)} Stock Price rows.")

    if sectors:
        pl.DataFrame(sectors).write_database(
            table_name="fact_sector_indices",
            connection=CLOUD_DATABASE_URL,
            engine="adbc",
            if_table_exists="append"
        )
        print(f"Inserted {len(sectors)} Sector Index rows.")

    # is_sunday = datetime.now().weekday() == 4
    # if force_slow_sync or is_sunday:
    symbols = [s['symbol'] for s in stocks]
    enrich_slow_sync_metadata(engine, symbols)


if __name__ == "__main__":
    run_pipeline()