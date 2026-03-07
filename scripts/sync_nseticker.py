import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

load_dotenv()

# Build database connection string locally or construct from Azure parts
DB_URL = os.environ.get("DB_URL")
if not DB_URL:
    server = os.environ.get("AZURE_SQL_SERVER")
    db = os.environ.get("AZURE_SQL_DATABASE")
    user = os.environ.get("AZURE_SQL_USERNAME")
    pwd = os.environ.get("AZURE_SQL_PASSWORD")
    if all([server, db, user, pwd]):
        DB_URL = f"mssql+pyodbc://{user}:{pwd}@{server}.database.windows.net/{db}?driver=ODBC+Driver+17+for+SQL+Server"
    else:
        raise ValueError("DB_URL or complete Azure SQL credentials environment variables are not set")

engine = create_engine(DB_URL)

def sync_tickers():
    logging.info("Starting synchronization from recommendations to nseticker...")
    
    # Using 'NULL' string literal because update nse tickers.py uses WHERE nse_ticker_yfinance = 'NULL'
    # Grouping by stock_code and taking MAX(stock_name) to avoid duplicates if names vary slightly
    upsert_query = """
    INSERT INTO nseticker (mc_ticker, company, nse_ticker_yfinance)
    SELECT DISTINCT stock_code, COALESCE(MAX(stock_name), MAX(stock_code)) as stock_name, 'NULL'
    FROM recommendations r
    WHERE stock_code IS NOT NULL AND stock_code != ''
      AND NOT EXISTS (
          SELECT 1 FROM nseticker n WHERE n.mc_ticker = r.stock_code
      )
    GROUP BY stock_code
    """
    
    with engine.begin() as conn:
        result = conn.execute(text(upsert_query))
        logging.info(f"Inserted {result.rowcount} new distinct tickers into nseticker table.")

if __name__ == "__main__":
    sync_tickers()
