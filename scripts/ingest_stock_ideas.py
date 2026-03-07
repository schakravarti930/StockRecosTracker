import requests
from bs4 import BeautifulSoup
import re
import requests
import json

url = "https://www.moneycontrol.com/markets/stock-ideas/"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

base_api = "https://api.moneycontrol.com/mcapi/v1/broker-research/stock-ideas"
api_url = "https://api.moneycontrol.com/mcapi/v1/broker-research/stock-ideas?start=0&limit=15"

from sqlalchemy import (
    Column, Integer, BigInteger, String, Date, DateTime,
    DECIMAL, ForeignKey, create_engine
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import func
from datetime import datetime

Base = declarative_base()

class Recommendation(Base):
    __tablename__ = "recommendations"

    id = id = Column(BigInteger, primary_key=True, autoincrement=False)

    organization = Column(String(100))
    analyst_recommendation = Column(String(10))   # BUY / SELL / HOLD

    stock_code = Column(String(20))                # scid
    stock_name = Column(String(100))
    stock_short_name = Column(String(100))
    exchange = Column(String(1))                   # N / B

    heading = Column(String(300))
    attachment_url = Column(String(500))
    stock_url = Column(String(200))

    recommend_date = Column(Date)
    entry_date = Column(Date)
    target_price_date = Column(Date)
    target_price_date_epoch = Column(Integer)

    creation_timestamp = Column(DateTime)

    recommended_price = Column(DECIMAL(10, 2))
    target_price = Column(DECIMAL(10, 2))
    cmp = Column(DECIMAL(10, 2))

    price_change = Column(DECIMAL(10, 2))
    percent_change = Column(DECIMAL(6, 2))

    current_returns = Column(DECIMAL(6, 2))
    potential_returns = Column(DECIMAL(6, 2))

    target_price_flag = Column(String(10))

    created_at = Column(DateTime, default=datetime.utcnow)

    # relationship
    history = relationship(
        "RecommendationHistory",
        back_populates="recommendation",
        cascade="all, delete-orphan"
    )


class RecommendationHistory(Base):
    __tablename__ = "recommendation_history"

    id = Column(Integer, primary_key=True, autoincrement=True)

    recommendation_id = Column(
        BigInteger,
        ForeignKey("recommendations.id"),
        nullable=False
    )

    record_type = Column(String(10))        # current / previous
    recommend_flag = Column(String(1))      # B / S / H

    target_price = Column(DECIMAL(10, 2))
    target_price_date = Column(Date)

    organization = Column(String(100))

    recommendation = relationship(
        "Recommendation",
        back_populates="history"
    )


from datetime import datetime

def parse_date_safe(value, fmt=None, dayfirst=False):
    if not value:
        return None

    try:
        if fmt:
            return datetime.strptime(value, fmt).date()

        # Handle formats like "January 06, 2026"
        if isinstance(value, str) and value[0].isalpha():
            return datetime.strptime(value, "%B %d, %Y").date()

        # Handle YYYY-MM-DD
        if "-" in value:
            return datetime.strptime(value, "%Y-%m-%d").date()

        # Handle DD/MM/YYYY
        if "/" in value:
            day, month, year = value.split("/")
            if dayfirst:
                return datetime(int(year), int(month), int(day)).date()
            return datetime(int(year), int(month), int(day)).date()

    except Exception:
        return None

def safe_int(val):
    try:
        return int(val)
    except Exception:
        return None

def safe_float(val):
    try:
        return float(val)
    except Exception:
        return None

def safe_str(val):
    return str(val) if val is not None else None

import os
from dotenv import load_dotenv

load_dotenv()

db_url = os.environ.get("DB_URL")
if not db_url:
    server = os.environ.get("AZURE_SQL_SERVER")
    db = os.environ.get("AZURE_SQL_DATABASE")
    user = os.environ.get("AZURE_SQL_USERNAME")
    pwd = os.environ.get("AZURE_SQL_PASSWORD")
    if all([server, db, user, pwd]):
        db_url = f"mssql+pyodbc://{user}:{pwd}@{server}.database.windows.net/{db}?driver=ODBC+Driver+17+for+SQL+Server"
    else:
        raise ValueError("DB_URL or complete Azure SQL credentials environment variables are not set")



engine = create_engine(db_url)

Base.metadata.create_all(engine)

def map_to_recommendation(item):
    # 🔴 Critical fields check
    critical_keys = ["id", "scid", "recommend_flag", "creation_date"]
    for key in critical_keys:
        if key not in item or item[key] in (None, ""):
            raise ValueError(f"Missing critical field: {key}")

    return Recommendation(
        id=safe_int(item.get("id")),
        organization=safe_str(item.get("organization")),
        analyst_recommendation=safe_str(item.get("recommend_flag")),

        stock_code=safe_str(item.get("scid")),
        stock_name=safe_str(item.get("stkname")),
        stock_short_name=safe_str(item.get("stockShortName")),
        exchange=safe_str(item.get("exchange")),

        heading=safe_str(item.get("heading")),
        attachment_url=safe_str(item.get("attachment")),
        stock_url=safe_str(item.get("stk_url")),

        recommend_date=parse_date_safe(item.get("recommend_date")),
        entry_date=parse_date_safe(item.get("entry_date")),
        target_price_date=parse_date_safe(item.get("target_price_date")),
        target_price_date_epoch=safe_int(item.get("target_price_date_epoch")),

        creation_timestamp=datetime.strptime(
            item["creation_date"], "%Y%m%d%H%M%S"
        ),

        recommended_price=safe_float(item.get("recommended_price")),
        target_price=safe_float(item.get("target_price")),
        cmp=safe_float(item.get("cmp")),

        price_change=safe_float(item.get("change")),
        percent_change=safe_float(item.get("perChange")),

        current_returns=safe_float(item.get("current_returns")),
        potential_returns=safe_float(item.get("potential_returns_per")),

        target_price_flag=safe_str(item.get("target_price_flag"))
    )


from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

Session = sessionmaker(bind=engine)

def insert_recommendations(api_response: dict):
    session = Session()

    for item in api_response.get("data", []):
        try:
            # --- 1️⃣ Avoid duplicate inserts ---
            exists = session.get(Recommendation, int(item["id"]))
            if exists:
                continue

            # --- 2️⃣ Insert main recommendation ---
            rec = map_to_recommendation(item)

            session.add(rec)

            # --- 3️⃣ Insert stock_data history ---
            stock_data = item.get("stock_data", {})

            for record_type in ["current", "previous"]:
                data = stock_data.get(record_type)

                # Handle cases where previous = []
                if not data or isinstance(data, list):
                    continue

                hist = RecommendationHistory(
                    recommendation=rec,
                    record_type=record_type,
                    recommend_flag=data["recommend_flag"],
                    target_price=float(data["target_price"]),
                    target_price_date=parse_date_safe(
                        data["target_price_date"], dayfirst=True
                    ),
                    organization=data["P_ORGANIZATION"]
                )

                session.add(hist)

        except Exception as e:
            session.rollback()
            print(f"Failed for ID {item.get('id')}: {e}")
            continue

    session.commit()
    session.close()


from sqlalchemy.exc import IntegrityError
import requests
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def ingest_stock_ideas():
    session = Session()

    try:
        # 🔹 1. High-water mark
        max_id = session.query(func.max(Recommendation.id)).scalar() or 0
        logging.info(f"High-water mark (max_id): {max_id}")

        start = 0
        page_size = 9
        inserted = 0
        stop_fetching = False
        count = 0

        while not stop_fetching:
            # 🔹 2. Call paginated API (guarded)
            logging.info(f"Fetching API batch at start={start}")
            try:
                response = requests.get(
                    base_api,
                    params={"start": start, "limit": start + page_size},
                    headers=headers,
                    timeout=10
                )
                response.raise_for_status()
                payload = response.json()
            except requests.RequestException as e:
                logging.error(f"API error at start={start}: {e}")
                break

            records = payload.get("data", [])
            if not records:
                break

            for item in records:
                try:
                    item_id = int(item["id"])
                    
                    exists = session.get(Recommendation, int(item["id"]))

                    # 🔹 3. Stop condition
                    if exists:
                        count = count + 1
                        continue

                    rec = map_to_recommendation(item)
                    session.add(rec)
                    inserted += 1

                    # 🔹 4. stock_data history
                    stock_data = item.get("stock_data", {})
                    for record_type in ["current", "previous"]:
                        hist = stock_data.get(record_type)
                        if not hist or isinstance(hist, list):
                            continue

                        session.add(
                            RecommendationHistory(
                                recommendation=rec,
                                record_type=record_type,
                                recommend_flag=hist["recommend_flag"],
                                target_price=float(hist["target_price"]),
                                target_price_date=parse_date_safe(
                                    hist["target_price_date"]
                                ),
                                organization=hist["P_ORGANIZATION"]
                            )
                        )
                

                except (KeyError, ValueError, TypeError) as e:
                    # Bad data — skip record, continue
                    logging.warning(f"Skipping bad record ID={item.get('id')}: {e}")
                    session.rollback()
                    continue

                except IntegrityError as e:
                    # Rare race condition — skip
                    logging.warning(f"DB integrity error for ID={item.get('id')}")
                    session.rollback()
                    continue

            start += page_size
            if count >= 3:
                stop_fetching = True

        session.commit()
        logging.info(f"Inserted {inserted} new records")

    except Exception as e:
        # Catch anything unexpected
        session.rollback()
        logging.error(f"Fatal error in ingestion: {e}")

    finally:
        session.close()


ingest_stock_ideas()