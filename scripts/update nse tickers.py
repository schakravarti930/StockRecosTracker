import requests
import pandas as pd
from sqlalchemy import create_engine, text
import time
import logging
import os
import urllib.parse
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.environ.get("DB_URL")
if not DB_URL:
    server = os.environ.get("AZURE_SQL_SERVER")
    db = os.environ.get("AZURE_SQL_DATABASE")
    user = os.environ.get("AZURE_SQL_USERNAME")
    pwd = os.environ.get("AZURE_SQL_PASSWORD")
    if all([server, db, user, pwd]):
        encoded_user = urllib.parse.quote_plus(user)
        encoded_pwd = urllib.parse.quote_plus(pwd)
        DB_URL = f"mssql+pyodbc://{encoded_user}:{encoded_pwd}@{server}.database.windows.net/{db}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no&Connection+Timeout=30"
    else:
        raise ValueError("DB_URL or complete Azure SQL credentials environment variables are not set")

MC_URL = "https://priceapi.moneycontrol.com/pricefeed/nse/equitycash/{}"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

SLEEP = 0.5   # rate limit (seconds)

logging.basicConfig(
    filename="ticker_fill.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

engine = create_engine(DB_URL)

def get_mc_data(mc_code):
    url = MC_URL.format(mc_code)
    r = requests.get(url,headers = headers, timeout=10)
    r.raise_for_status()
    return r.json()


def extract_nseid(js):
    return js.get("data", {}).get("NSEID")


with engine.connect() as conn:

    df = pd.read_sql("""
        SELECT *
        FROM nseticker
        WHERE nse_ticker_yfinance = 'NULL'
    """, conn)

print(f"Found {len(df)} rows to update")

updated = 0
failed = 0


with engine.begin() as conn:
    for _, row in df.iterrows():

        #rid = row["id"]
        mc = row["mc_ticker"]

        try:
            js = get_mc_data(mc)
            nseid = extract_nseid(js)

            if not nseid:
                raise ValueError("No NSEID found")

            yahoo = f"{nseid}.NS"

            conn.execute(
                text("""
                    UPDATE nseticker
                    SET nse_ticker_yfinance = :ticker
                    WHERE mc_ticker = :id
                """),
                {"ticker": yahoo, "id": mc}
            )

            updated += 1
            logging.info(f"Updated {mc} -> {yahoo}")

        except Exception as e:

            failed += 1
            logging.error(f"Failed {mc}: {e}")

        time.sleep(SLEEP)


logging.info(f"Updated: {updated}")
logging.info(f"Failed : {failed}")