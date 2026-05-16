import argparse
import json
import os
import re
from datetime import date, datetime, time, timezone, timedelta
from decimal import Decimal
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "public" / "data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Azure SQL dashboard data to static JSON files."
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where JSON files will be written.",
    )
    parser.add_argument(
        "--price-days",
        type=int,
        default=190,
        help="Number of recent calendar days to export from dailyohlc.",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Write small representative files without connecting to Azure SQL.",
    )
    return parser.parse_args()


def clean_record(value):
    if pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.date().isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, separators=(",", ":"), default=clean_record)


def frame_to_records(frame: pd.DataFrame):
    return [
        {column: clean_record(value) for column, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]


def normalize_organization_name(name):
    if pd.isna(name):
        return name
    normalized = re.sub(r"\s+", " ", str(name).strip())
    if normalized.isupper() or normalized.islower():
        normalized = normalized.title()
    return normalized


def clean_env_value(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {"'", '"'}:
        cleaned = cleaned[1:-1].strip()
    return cleaned


def normalize_server_host(server: str) -> str:
    host = server.strip()
    if host.lower().startswith("tcp:"):
        host = host[4:]
    if "," in host:
        host = host.split(",", 1)[0]
    if ":" in host and not host.endswith(".database.windows.net"):
        host = host.split(":", 1)[0]
    if not host.endswith(".database.windows.net"):
        host = f"{host}.database.windows.net"
    return host


def get_engine():
    load_dotenv(ROOT / ".env")
    server = clean_env_value(os.environ.get("AZURE_SQL_SERVER"))
    database = clean_env_value(os.environ.get("AZURE_SQL_DATABASE"))
    username = clean_env_value(os.environ.get("AZURE_SQL_USERNAME"))
    password = clean_env_value(os.environ.get("AZURE_SQL_PASSWORD"))
    driver = clean_env_value(os.environ.get("AZURE_SQL_ODBC_DRIVER")) or "ODBC Driver 17 for SQL Server"
    missing = [
        name
        for name, value in {
            "AZURE_SQL_SERVER": server,
            "AZURE_SQL_DATABASE": database,
            "AZURE_SQL_USERNAME": username,
            "AZURE_SQL_PASSWORD": password,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    url = URL.create(
        "mssql+pyodbc",
        username=username,
        password=password,
        host=normalize_server_host(server),
        port=1433,
        database=database,
        query={
            "driver": driver,
            "Encrypt": "yes",
            "TrustServerCertificate": "no",
            "Connection Timeout": "30",
        },
    )
    return create_engine(url)


def load_dashboard_frames(engine):
    scorecard = pd.read_sql(
        "SELECT * FROM vw_analyst_scorecard ORDER BY total_calls DESC",
        engine,
    )
    returns = pd.read_sql(
        "SELECT * FROM vw_recommendation_returns ORDER BY recommend_date DESC",
        engine,
    )
    target_hit = pd.read_sql(
        "SELECT * FROM vw_target_hit ORDER BY recommend_date DESC",
        engine,
    )

    for frame in (scorecard, returns, target_hit):
        if "organization" in frame.columns:
            frame["organization"] = frame["organization"].apply(normalize_organization_name)

    return scorecard, returns, target_hit


def export_price_history(engine, output_dir: Path, price_days: int):
    ticker_map = pd.read_sql(
        """
        SELECT DISTINCT r.stock_name, n.nse_ticker_yfinance AS symbol
        FROM nseticker n
        JOIN recommendations r ON r.stock_code = n.mc_ticker
        WHERE n.nse_ticker_yfinance IS NOT NULL
          AND n.nse_ticker_yfinance != 'NULL'
        ORDER BY r.stock_name
        """,
        engine,
    )
    price_dir = output_dir / "price-history"
    price_dir.mkdir(parents=True, exist_ok=True)

    exported_symbols = []
    cutoff_date = date.today() - timedelta(days=price_days)
    with engine.connect() as conn:
        for row in ticker_map.itertuples(index=False):
            symbol = str(row.symbol)
            price_frame = pd.read_sql(
                text(
                    """
                    SELECT [date], [close]
                    FROM dailyohlc
                    WHERE symbol = :symbol
                      AND [date] >= :cutoff_date
                    ORDER BY [date]
                    """
                ),
                conn,
                params={"symbol": symbol, "cutoff_date": cutoff_date},
            )
            if price_frame.empty:
                continue
            rows = [
                [clean_record(item.date), clean_record(item.close)]
                for item in price_frame.itertuples(index=False)
            ]
            write_json(price_dir / f"{symbol}.json", rows)
            exported_symbols.append(symbol)

    return ticker_map, exported_symbols


def stock_summary(returns: pd.DataFrame, ticker_map: pd.DataFrame) -> list[dict]:
    symbol_by_stock = {}
    if not ticker_map.empty:
        symbol_by_stock = dict(zip(ticker_map["stock_name"], ticker_map["symbol"]))

    stocks = []
    for stock_name, stock_frame in returns.groupby("stock_name", dropna=True):
        stocks.append(
            {
                "stock_name": stock_name,
                "symbol": symbol_by_stock.get(stock_name),
                "recommendation_count": int(len(stock_frame)),
                "firm_count": int(stock_frame["organization"].nunique()),
                "latest_recommend_date": clean_record(pd.to_datetime(stock_frame["recommend_date"]).max()),
            }
        )
    return sorted(stocks, key=lambda item: item["stock_name"])


def write_real_export(output_dir: Path, price_days: int) -> None:
    engine = get_engine()
    scorecard, returns, target_hit = load_dashboard_frames(engine)
    ticker_map, exported_symbols = export_price_history(engine, output_dir, price_days)

    write_json(output_dir / "scorecard.json", frame_to_records(scorecard))
    write_json(output_dir / "returns.json", frame_to_records(returns))
    write_json(output_dir / "target-hit.json", frame_to_records(target_hit))
    write_json(output_dir / "stocks.json", stock_summary(returns, ticker_map))
    write_json(
        output_dir / "manifest.json",
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "price_days": price_days,
            "row_counts": {
                "scorecard": int(len(scorecard)),
                "returns": int(len(returns)),
                "target_hit": int(len(target_hit)),
                "stocks": int(returns["stock_name"].nunique()),
                "price_history_symbols": len(exported_symbols),
            },
        },
    )


def write_sample_export(output_dir: Path) -> None:
    scorecard = [
        {
            "organization": "ICICI Securities",
            "total_calls": 8,
            "earliest_call": "2025-11-17",
            "hit_rate_pct": 62.5,
            "avg_return_30d": 3.4,
            "avg_return_current": 8.1,
            "best_call_current": 18.2,
            "worst_call_current": -5.1,
            "target_hit_rate_pct": 37.5,
            "stdev_return_current": 7.2,
        },
        {
            "organization": "Motilal Oswal",
            "total_calls": 6,
            "earliest_call": "2025-12-05",
            "hit_rate_pct": 50.0,
            "avg_return_30d": -1.2,
            "avg_return_current": 4.7,
            "best_call_current": 14.6,
            "worst_call_current": -8.9,
            "target_hit_rate_pct": 33.3,
            "stdev_return_current": 6.5,
        },
    ]
    returns = [
        {
            "organization": "ICICI Securities",
            "stock_name": "Reliance Industries",
            "analyst_recommendation": "BUY",
            "recommend_date": "2026-01-02",
            "recommended_price": 1220.0,
            "target_price": 1380.0,
            "potential_returns": 13.1,
            "return_30d": 2.4,
            "return_60d": 5.2,
            "return_current": 8.7,
            "direction_correct": 1,
            "days_alive": 134,
        },
        {
            "organization": "Motilal Oswal",
            "stock_name": "TCS",
            "analyst_recommendation": "HOLD",
            "recommend_date": "2026-02-11",
            "recommended_price": 3840.0,
            "target_price": 3920.0,
            "potential_returns": 2.1,
            "return_30d": -1.1,
            "return_60d": 1.3,
            "return_current": 3.4,
            "direction_correct": 1,
            "days_alive": 94,
        },
    ]
    target_hit = [
        {
            "organization": "ICICI Securities",
            "stock_name": "Reliance Industries",
            "analyst_recommendation": "BUY",
            "recommend_date": "2026-01-02",
            "recommended_price": 1220.0,
            "target_price": 1380.0,
            "target_upside_pct": 13.1,
            "target_hit": 0,
            "days_to_target": None,
            "max_return_achieved": 9.6,
        },
        {
            "organization": "Motilal Oswal",
            "stock_name": "TCS",
            "analyst_recommendation": "HOLD",
            "recommend_date": "2026-02-11",
            "recommended_price": 3840.0,
            "target_price": 3920.0,
            "target_upside_pct": 2.1,
            "target_hit": 1,
            "days_to_target": 34,
            "max_return_achieved": 4.0,
        },
    ]
    stocks = [
        {
            "stock_name": "Reliance Industries",
            "symbol": "RELIANCE.NS",
            "recommendation_count": 1,
            "firm_count": 1,
            "latest_recommend_date": "2026-01-02",
        },
        {
            "stock_name": "TCS",
            "symbol": "TCS.NS",
            "recommendation_count": 1,
            "firm_count": 1,
            "latest_recommend_date": "2026-02-11",
        },
    ]
    write_json(output_dir / "scorecard.json", scorecard)
    write_json(output_dir / "returns.json", returns)
    write_json(output_dir / "target-hit.json", target_hit)
    write_json(output_dir / "stocks.json", stocks)
    write_json(
        output_dir / "manifest.json",
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "sample": True,
            "price_days": 190,
            "row_counts": {
                "scorecard": len(scorecard),
                "returns": len(returns),
                "target_hit": len(target_hit),
                "stocks": len(stocks),
                "price_history_symbols": 2,
            },
        },
    )
    write_json(
        output_dir / "price-history" / "RELIANCE.NS.json",
        [["2026-01-02", 1220.0], ["2026-02-02", 1250.0], ["2026-03-02", 1298.0], ["2026-04-02", 1326.0]],
    )
    write_json(
        output_dir / "price-history" / "TCS.NS.json",
        [["2026-02-11", 3840.0], ["2026-03-11", 3798.0], ["2026-04-11", 3890.0], ["2026-05-11", 3970.0]],
    )


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    if args.sample:
        write_sample_export(output_dir)
    else:
        write_real_export(output_dir, args.price_days)
    print(f"Exported dashboard data to {output_dir}")


if __name__ == "__main__":
    main()
