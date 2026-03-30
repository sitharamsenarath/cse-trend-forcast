import os

from sqlalchemy import create_engine, text
import time

def get_db_engine(env_var_name: str, retries=5):
    db_url = os.getenv(env_var_name)

    if not db_url:
        raise ValueError(f"Environment variable {env_var_name} is not set.")

    for i in range(retries):
        try:
            engine = create_engine(db_url)
            # Try to actually execute a tiny query to test the connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                return engine
        except Exception as e:
            print(f"Database not ready (Attempt {i+1}/{retries}). Error: {e}")
            time.sleep(3)
    raise Exception(f"Failed to connect to DB at {env_var_name}")
