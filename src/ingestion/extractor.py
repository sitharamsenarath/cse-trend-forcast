import os

import requests
import polars as pl
from models import StockPriceFact
from sqlalchemy import create_engine, text
import time


DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_engine(retries=5):
    for i in range(retries):
        try:
            engine = create_engine(DATABASE_URL)
            # Try to actually execute a tiny query to test the connection
            with engine.connect() as conn:
                return engine
        except Exception as e:
            print(f"Database not ready (Attempt {i+1}/{retries}). Error: {e}")
            time.sleep(3)
    raise Exception(f"Failed to connect to DB at {DATABASE_URL}")


def fetch_and_validate():
    url = "https://www.cse.lk/api/tradeSummary"

    print(f"Fetching data from url: {url}")
    response = requests.post(url)
    response.raise_for_status()  # Raise an error for bad status codes

    raw_data = response.json().get("reqTradeSummery", [])

    validated_data = []
    dim_data = {}

    for item in raw_data:
        try:
            obj = StockPriceFact(**item)
            validated_data.append(obj.model_dump())

            symbol = item.get("symbol", "Unknown")
            if symbol and symbol not in dim_data:
                dim_data[symbol] = item.get("name", "Unknown")
        except Exception as e:
            print(f"Validation failed for a item {item.get('symbol')}: {e}")
            continue

    return validated_data, dim_data


def run_pipeline():
    data, dimensions = fetch_and_validate()
    if not data:
        print("No data fetched from API.")
        return
    
    df = pl.DataFrame(data)
    print(f"Polars DataFrame created with {df.height} rows.")

    df = df.with_columns([
        pl.col("price").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("change_percentage").cast(pl.Float64),
    ])

    engine = get_db_engine()

    with engine.begin() as connection:
        for symbol, name in dimensions.items():
            connection.execute(text("""
                                    INSERT INTO dim_stocks (symbol, name) 
                                    VALUES (:s, :n) 
                                    ON CONFLICT (symbol) 
                                    DO UPDATE SET name = EXCLUDED.name, last_updated = CURRENT_TIMESTAMP;
                                """), {"s": symbol, "n": name}
                            )

    df.write_database(
        table_name="fact_stock_prices", 
        connection=DATABASE_URL,
        engine="adbc", 
        if_table_exists="append"
    )
    print("Data inserted into the database successfully.")


if __name__ == "__main__":
    run_pipeline()