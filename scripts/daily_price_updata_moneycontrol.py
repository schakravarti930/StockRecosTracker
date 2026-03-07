import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import time
import random
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%H:%M:%S'
)

# ══════════════════════════════════════════════════
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


DEFAULT_LOOKBACK = 365   # days back for symbols with no existing data

SLEEP_MIN  = 1.5         # seconds between requests (be polite)
SLEEP_MAX  = 3.5
MAX_RETRY  = 4
BACKOFF    = 20          # seconds; multiplied per retry attempt

BASE_URL = "https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history"

# Browser-like headers — required, MoneyControl blocks bare requests
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.moneycontrol.com/",
    "Origin":  "https://www.moneycontrol.com",
    "Accept":  "application/json, text/plain, */*",
}

# ══════════════════════════════════════════════════
#  DB ENGINE
# ══════════════════════════════════════════════════

engine = create_engine(DB_URL, fast_executemany=True)
session = requests.Session()
session.headers.update(HEADERS)

print("✅ Connections initialised")


# ══════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════

def to_unix(dt) -> int:
    """Convert date/datetime to Unix timestamp (seconds)."""
    if isinstance(dt, (pd.Timestamp, datetime)):
        return int(dt.timestamp())
    # assume date object
    return int(datetime(dt.year, dt.month, dt.day, 0, 0, 0).timestamp())


def get_symbols():
    """Pull NSE tickers from DB and derive MoneyControl chart symbols."""
    with engine.connect() as conn:
        df = pd.read_sql("""
            SELECT DISTINCT nse_ticker_yfinance
            FROM nseticker
            WHERE nse_ticker_yfinance IS NOT NULL
        """, conn)
    # SBIN.NS → SBIN
    df["mc_symbol"] = df["nse_ticker_yfinance"].str.replace(".NS", "", regex=False)
    return df


def get_last_dates():
    """Latest date already stored per symbol."""
    with engine.connect() as conn:
        df = pd.read_sql("""
            SELECT symbol, MAX(date) AS last_date
            FROM dailyohlc
            GROUP BY symbol
        """, conn)
    df["last_date"] = pd.to_datetime(df["last_date"])
    return dict(zip(df["symbol"], df["last_date"]))


def is_rate_limit(resp: requests.Response) -> bool:
    return resp.status_code in (429, 403)


# Errors that are permanent — retrying is pointless
PERMANENT_ERRORS = ["wrong symbol", "invalid symbol", "symbol not found", "no data found"]

class PermanentAPIError(Exception):
    """Raised when the API signals a non-retryable error (e.g. bad symbol)."""
    pass


def fetch_ohlc(mc_symbol: str, start_dt, end_dt) -> pd.DataFrame:
    """
    Fetch daily OHLCV from MoneyControl chart API for one symbol.
    Returns DataFrame with columns: date, open, high, low, close, volume

    Raises PermanentAPIError immediately (no retries) for bad symbols.
    Retries up to MAX_RETRY times for transient errors (timeouts, 429s).
    """
    from_ts   = to_unix(start_dt)
    to_ts     = to_unix(end_dt)
    countback = (end_dt - start_dt).days + 5   # a few extra bars as buffer

    params = {
        "symbol"      : mc_symbol,
        "resolution"  : "1D",
        "from"        : from_ts,
        "to"          : to_ts,
        "countback"   : countback,
        "currencyCode": "INR",
    }

    for attempt in range(MAX_RETRY):
        try:
            resp = session.get(BASE_URL, params=params, timeout=15)

            if is_rate_limit(resp):
                wait = BACKOFF * (2 ** attempt)
                logging.warning(f"   🚦 Rate limit ({resp.status_code}) — sleeping {wait}s")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()

            # ── Check API-level status before parsing ──────────────────
            if data.get("s") != "ok":
                errmsg = data.get("errmsg", "").lower()

                # Permanent errors — bail immediately, no retries
                if any(p in errmsg for p in PERMANENT_ERRORS):
                    raise PermanentAPIError(f"{data.get('errmsg', 'Unknown permanent error')}")

                # Unknown API error — treat as transient and retry
                raise ValueError(f"API status: {data.get('s')} — {data.get('errmsg', '')}")

            df = pd.DataFrame({
                "date"  : pd.to_datetime(data["t"], unit="s").tz_localize("UTC").tz_convert("Asia/Kolkata").tz_localize(None),
                "open"  : data["o"],
                "high"  : data["h"],
                "low"   : data["l"],
                "close" : data["c"],
                "volume": [int(v) for v in data["v"]],
            })

            # Normalise date to midnight (strip time component)
            df["date"] = df["date"].dt.normalize()

            # Filter strictly to requested window
            start_ts = pd.Timestamp(start_dt)
            end_ts   = pd.Timestamp(end_dt)
            df = df[(df["date"] >= start_ts) & (df["date"] <= end_ts)]

            df["adjclose"] = df["close"]   # MC doesn't provide adj close separately

            return df.reset_index(drop=True)

        except PermanentAPIError:
            raise   # propagate immediately — no retry

        except Exception as e:
            wait = BACKOFF * (attempt + 1)
            logging.warning(f"   ⏳ Attempt {attempt+1}/{MAX_RETRY} failed: {e} — retrying in {wait}s")
            if attempt < MAX_RETRY - 1:
                time.sleep(wait)
            else:
                raise

# ══════════════════════════════════════════════════
#  PREP
# ══════════════════════════════════════════════════

today      = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
sym_df     = get_symbols()
last_dates = get_last_dates()

print(f"📊 Total symbols : {len(sym_df)}")
print(f"📅 Today         : {today.date()}")

# Build work list
work = []   # (mc_symbol, yf_symbol, start_dt)

for _, row in sym_df.iterrows():
    yf_sym = row["nse_ticker_yfinance"]
    mc_sym = row["mc_symbol"]

    if yf_sym in last_dates and pd.notna(last_dates[yf_sym]):
        start = last_dates[yf_sym] + timedelta(days=1)
    else:
        start = today - timedelta(days=DEFAULT_LOOKBACK)

    start = start.to_pydatetime() if hasattr(start, 'to_pydatetime') else start

    if start.date() <= today.date():
        work.append((mc_sym, yf_sym, start))

print(f"🔄 Symbols needing update : {len(work)}")
print(f"⏱  Est. time (at {SLEEP_MAX}s/symbol) : ~{len(work)*SLEEP_MAX/60:.1f} min")


# ══════════════════════════════════════════════════
#  MAIN LOOP
# ══════════════════════════════════════════════════

inserted      = 0
failed        = []   # (mc_sym, yf_sym) — transient failures, worth retrying
bad_symbols   = []   # (mc_sym, yf_sym) — permanent failures, don't retry
total         = len(work)

with engine.begin() as conn:

    for i, (mc_sym, yf_sym, start_dt) in enumerate(work, start=1):

        logging.info(f"[{i:>3}/{total}]  {mc_sym:25s}  {start_dt.date()} → {today.date()}")

        try:
            df = fetch_ohlc(mc_sym, start_dt, today)

            if df.empty:
                logging.warning("⚠ No data")
                failed.append((mc_sym, yf_sym))
                continue

            df["symbol"] = yf_sym   # store using yfinance-style key for consistency
            rows = df[["symbol","date","open","high","low","close","volume","adjclose"]].to_dict(orient="records")

            # Upsert — skip rows that already exist
            conn.execute(
                text("""
                    INSERT INTO dailyohlc
                        (symbol, [date], [open], high, low, [close], volume, adjclose)
                    SELECT
                        :symbol, :date, :open, :high, :low, :close, :volume, :adjclose
                    WHERE NOT EXISTS (
                        SELECT 1 FROM dailyohlc
                        WHERE symbol = :symbol AND [date] = :date
                    )
                """),
                rows
            )

            inserted += len(rows)
            logging.info(f"✅ {len(rows)} rows")

        except PermanentAPIError as e:
            # Bad symbol / delisted — log once, never retry
            logging.error(f"⛔ {e}")
            bad_symbols.append((mc_sym, yf_sym))

        except Exception as e:
            # Transient error — queue for retry
            logging.error(f"❌ {e}")
            failed.append((mc_sym, yf_sym))

        # Polite sleep between requests
        if i < total:
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))


logging.info("\n" + "═" * 50)
logging.info("SUMMARY")
logging.info("═" * 50)
logging.info(f"Rows inserted    : {inserted}")
logging.info(f"Transient errors : {len(failed)}  ← run retry cell")
logging.info(f"Bad symbols      : {len(bad_symbols)}  ← fix MC symbol mapping")
if failed:
    logging.info(f"Retry these: {[yf for _, yf in failed]}")
if bad_symbols:
    logging.info(f"Bad symbols : {[f'{mc} ({yf})' for mc, yf in bad_symbols]}")
logging.info("═" * 50)