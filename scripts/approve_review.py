"""
approve_review.py — CLI tool to approve or reject records in recommendations_review.

Usage:
    python scripts/approve_review.py

Commands inside the prompt:
    approve <id>     — approve a single record, moves it to recommendations
    reject  <id>     — reject a single record, keeps it in review as rejected
    approve all      — approve every pending record
    reject  all      — reject every pending record
    list             — reprint the pending queue
    quit             — exit
"""

import os
import sys
import urllib.parse
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import create_engine

# Allow importing models from ingest_stock_ideas without re-running the script
sys.path.insert(0, os.path.dirname(__file__))

# Import models and helpers only — the module-level ingest_stock_ideas() call
# won't run because we're importing via module, but to be safe we reload selectively.
from ingest_stock_ideas import (
    Base,
    Recommendation,
    RecommendationHistory,
    RecommendationReview,
    RecommendationReviewHistory,
    parse_date_safe,
    safe_float,
)
from sqlalchemy.orm import sessionmaker

# ─────────────────────────────────────────
# DB CONNECTION
# ─────────────────────────────────────────

load_dotenv()

db_url = os.environ.get("DB_URL")
if not db_url:
    server = os.environ.get("AZURE_SQL_SERVER")
    db_name = os.environ.get("AZURE_SQL_DATABASE")
    user = os.environ.get("AZURE_SQL_USERNAME")
    pwd  = os.environ.get("AZURE_SQL_PASSWORD")
    if all([server, db_name, user, pwd]):
        eu = urllib.parse.quote_plus(user)
        ep = urllib.parse.quote_plus(pwd)
        db_url = (
            f"mssql+pyodbc://{eu}:{ep}@{server}.database.windows.net/{db_name}"
            "?driver=ODBC+Driver+17+for+SQL+Server"
            "&Encrypt=yes&TrustServerCertificate=no&Connection+Timeout=30"
        )
    else:
        raise ValueError("DB credentials not set — check your .env file")

engine = create_engine(db_url)
Session = sessionmaker(bind=engine)

# ─────────────────────────────────────────
# DISPLAY
# ─────────────────────────────────────────

COL = dict(id=14, stock=22, org=26, date=12)
SEP = "─" * 100

def _print_queue(records):
    if not records:
        print("\n  No pending records in the review queue.\n")
        return
    print(f"\n{SEP}")
    print(
        f"  {'ID':>{COL['id']}}  {'Stock':<{COL['stock']}}  "
        f"{'Organisation':<{COL['org']}}  {'Rec Date':<{COL['date']}}  Violations"
    )
    print(SEP)
    for r in records:
        print(
            f"  {r.id:>{COL['id']}}  {(r.stock_name or '?')[:COL['stock']]:<{COL['stock']}}  "
            f"  {(r.organization or '?')[:COL['org']]:<{COL['org']}}  "
            f"{str(r.recommend_date):<{COL['date']}}"
        )
        for reason in r.review_reason.split(" | "):
            print(f"  {'':>{COL['id']}}  {'':>{COL['stock']}}  {'':>{COL['org']}}  {'':>{COL['date']}}  ⚠  {reason}")
    print(f"{SEP}\n")


def _fetch_pending(session):
    return (
        session.query(RecommendationReview)
        .filter_by(review_status="pending")
        .order_by(RecommendationReview.id)
        .all()
    )

# ─────────────────────────────────────────
# APPROVE / REJECT ACTIONS
# ─────────────────────────────────────────

def approve_record(record_id: int, session) -> bool:
    review_rec = session.get(RecommendationReview, record_id)

    if not review_rec:
        print(f"  ✗  ID {record_id} not found in review queue.")
        return False

    if review_rec.review_status != "pending":
        print(f"  ✗  ID {record_id} is already '{review_rec.review_status}' — nothing to do.")
        return False

    # If already landed in main table somehow, just mark approved
    if session.get(Recommendation, record_id):
        review_rec.review_status = "approved"
        review_rec.reviewed_at   = datetime.utcnow()
        print(f"  ✓  ID {record_id} already in recommendations — marked approved.")
        return True

    # Copy to main recommendations table
    rec = Recommendation(
        id=review_rec.id,
        organization=review_rec.organization,
        analyst_recommendation=review_rec.analyst_recommendation,
        stock_code=review_rec.stock_code,
        stock_name=review_rec.stock_name,
        stock_short_name=review_rec.stock_short_name,
        exchange=review_rec.exchange,
        heading=review_rec.heading,
        attachment_url=review_rec.attachment_url,
        stock_url=review_rec.stock_url,
        recommend_date=review_rec.recommend_date,
        entry_date=review_rec.entry_date,
        target_price_date=review_rec.target_price_date,
        target_price_date_epoch=review_rec.target_price_date_epoch,
        creation_timestamp=review_rec.creation_timestamp,
        recommended_price=review_rec.recommended_price,
        target_price=review_rec.target_price,
        cmp=review_rec.cmp,
        price_change=review_rec.price_change,
        percent_change=review_rec.percent_change,
        current_returns=review_rec.current_returns,
        potential_returns=review_rec.potential_returns,
        target_price_flag=review_rec.target_price_flag,
        created_at=review_rec.created_at,
    )
    session.add(rec)
    session.flush()  # ensure rec.id is available for FK

    # Copy history rows
    for h in review_rec.history:
        session.add(RecommendationHistory(
            recommendation=rec,
            record_type=h.record_type,
            recommend_flag=h.recommend_flag,
            target_price=h.target_price,
            target_price_date=h.target_price_date,
            organization=h.organization,
        ))

    review_rec.review_status = "approved"
    review_rec.reviewed_at   = datetime.utcnow()

    print(
        f"  ✓  ID {record_id}  ({review_rec.stock_name or '?'})  "
        f"approved and moved to recommendations."
    )
    return True


def reject_record(record_id: int, session) -> bool:
    review_rec = session.get(RecommendationReview, record_id)

    if not review_rec:
        print(f"  ✗  ID {record_id} not found in review queue.")
        return False

    if review_rec.review_status != "pending":
        print(f"  ✗  ID {record_id} is already '{review_rec.review_status}' — nothing to do.")
        return False

    review_rec.review_status = "rejected"
    review_rec.reviewed_at   = datetime.utcnow()

    print(f"  ✗  ID {record_id}  ({review_rec.stock_name or '?'})  rejected.")
    return True

# ─────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────

def main():
    print("\n📋  REVIEW QUEUE — Stock Recommendations")

    session = Session()
    try:
        records = _fetch_pending(session)
        _print_queue(records)

        if not records:
            return

        print("Commands:")
        print("  approve <id>   | approve all   — move record(s) to main table")
        print("  reject  <id>   | reject  all   — discard record(s) (kept for audit)")
        print("  list                           — refresh the pending list")
        print("  quit                           — exit\n")

        while True:
            try:
                raw = input(">> ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                print("\nExiting.")
                break

            if not raw:
                continue

            if raw in ("quit", "exit", "q"):
                break

            if raw == "list":
                _print_queue(_fetch_pending(session))
                continue

            parts = raw.split(maxsplit=1)
            if len(parts) < 2:
                print("  Usage: approve <id|all>  |  reject <id|all>  |  list  |  quit")
                continue

            action, target = parts[0], parts[1].strip()

            if action not in ("approve", "reject"):
                print(f"  Unknown action '{action}'. Use approve or reject.")
                continue

            fn = approve_record if action == "approve" else reject_record

            if target == "all":
                pending_ids = [
                    r.id for r in session.query(RecommendationReview)
                    .filter_by(review_status="pending").all()
                ]
                if not pending_ids:
                    print("  No pending records.")
                    continue
                for rid in pending_ids:
                    fn(rid, session)
                session.commit()
                print(f"\n  Committed {len(pending_ids)} records.\n")
            else:
                try:
                    rid = int(target)
                except ValueError:
                    print(f"  '{target}' is not a valid ID.")
                    continue
                fn(rid, session)
                session.commit()

    except Exception as e:
        session.rollback()
        print(f"\n  Error: {e}")

    finally:
        session.close()


if __name__ == "__main__":
    main()
