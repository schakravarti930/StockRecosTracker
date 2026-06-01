import streamlit as st
import pandas as pd
import json
import os
import urllib.parse
from datetime import datetime
from dotenv import load_dotenv

from sqlalchemy import (
    Column, Integer, BigInteger, String, Date, DateTime,
    DECIMAL, ForeignKey, Text, create_engine, func
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.exc import IntegrityError

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

st.set_page_config(
    page_title="Quarantine Review",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=Syne:wght@400;600;700;800&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Mono', monospace;
    background-color: #0a0a0f;
    color: #e8e8f0;
}
.stApp { background-color: #0a0a0f; }
h1, h2, h3 { font-family: 'Syne', sans-serif; }

.stat-card {
    background: linear-gradient(135deg, #12121a 0%, #1a1a2e 100%);
    border: 1px solid #2a2a3e;
    border-radius: 12px;
    padding: 18px 22px;
    margin-bottom: 12px;
}
.stat-label {
    font-size: 10px;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: #6b6b8a;
    margin-bottom: 4px;
}
.stat-value {
    font-family: 'Syne', sans-serif;
    font-size: 32px;
    font-weight: 700;
}
.stat-value.pending  { color: #ffb400; }
.stat-value.approved { color: #00d4aa; }
.stat-value.rejected { color: #ff5577; }
.stat-value.total    { color: #e8e8f0; }

.section-header {
    font-family: 'Syne', sans-serif;
    font-size: 12px;
    letter-spacing: 3px;
    text-transform: uppercase;
    color: #6b6b8a;
    border-bottom: 1px solid #2a2a3e;
    padding-bottom: 10px;
    margin: 24px 0 16px 0;
}

.flag-chip {
    display: inline-block;
    background: rgba(255,180,0,0.1);
    border: 1px solid rgba(255,180,0,0.25);
    border-radius: 4px;
    padding: 2px 8px;
    font-size: 11px;
    color: #ffb400;
    margin: 2px 3px 2px 0;
}

.info-box {
    background: rgba(123,159,255,0.08);
    border: 1px solid rgba(123,159,255,0.2);
    border-radius: 8px;
    padding: 12px 16px;
    font-size: 12px;
    color: #7b9fff;
    margin-bottom: 16px;
}

[data-testid="stDataFrame"] {
    border: 1px solid #2a2a3e;
    border-radius: 8px;
}

div[data-testid="stSelectbox"] label,
div[data-testid="stMultiSelect"] label,
div[data-testid="stTextInput"] label {
    font-size: 11px;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: #6b6b8a;
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# DB CONNECTION
# ─────────────────────────────────────────

load_dotenv()

@st.cache_resource
def get_engine():
    db_url = os.environ.get("DB_URL")
    if not db_url:
        server = os.environ.get("AZURE_SQL_SERVER")
        db     = os.environ.get("AZURE_SQL_DATABASE")
        user   = os.environ.get("AZURE_SQL_USERNAME")
        pwd    = os.environ.get("AZURE_SQL_PASSWORD")
        if all([server, db, user, pwd]):
            encoded_user = urllib.parse.quote_plus(user)
            encoded_pwd  = urllib.parse.quote_plus(pwd)
            db_url = (
                f"mssql+pyodbc://{encoded_user}:{encoded_pwd}"
                f"@{server}.database.windows.net/{db}"
                "?driver=ODBC+Driver+17+for+SQL+Server"
                "&Encrypt=yes&TrustServerCertificate=no&Connection+Timeout=30"
            )
        else:
            st.error("Database credentials are not set. Check your .env file.")
            st.stop()
    return create_engine(db_url)


# ─────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────

Base = declarative_base()

class Recommendation(Base):
    __tablename__ = "recommendations"
    id = Column(BigInteger, primary_key=True, autoincrement=False)
    organization            = Column(String(100))
    analyst_recommendation  = Column(String(10))
    stock_code              = Column(String(20))
    stock_name              = Column(String(100))
    stock_short_name        = Column(String(100))
    exchange                = Column(String(1))
    heading                 = Column(String(300))
    attachment_url          = Column(String(500))
    stock_url               = Column(String(200))
    recommend_date          = Column(Date)
    entry_date              = Column(Date)
    target_price_date       = Column(Date)
    target_price_date_epoch = Column(Integer)
    creation_timestamp      = Column(DateTime)
    recommended_price       = Column(DECIMAL(10, 2))
    target_price            = Column(DECIMAL(10, 2))
    cmp                     = Column(DECIMAL(10, 2))
    price_change            = Column(DECIMAL(10, 2))
    percent_change          = Column(DECIMAL(6, 2))
    current_returns         = Column(DECIMAL(6, 2))
    potential_returns       = Column(DECIMAL(6, 2))
    target_price_flag       = Column(String(10))
    created_at              = Column(DateTime, default=datetime.utcnow)
    history = relationship("RecommendationHistory", back_populates="recommendation", cascade="all, delete-orphan")

class RecommendationHistory(Base):
    __tablename__ = "recommendation_history"
    id                = Column(Integer, primary_key=True, autoincrement=True)
    recommendation_id = Column(BigInteger, ForeignKey("recommendations.id"), nullable=False)
    record_type       = Column(String(10))
    recommend_flag    = Column(String(1))
    target_price      = Column(DECIMAL(10, 2))
    target_price_date = Column(Date)
    organization      = Column(String(100))
    recommendation    = relationship("Recommendation", back_populates="history")

class RecommendationQuarantine(Base):
    __tablename__ = "recommendations_quarantine"
    id          = Column(Integer, primary_key=True, autoincrement=True)
    source_id   = Column(BigInteger, nullable=True)
    raw_json    = Column(Text)
    flag_reason = Column(String(1000))
    flagged_at  = Column(DateTime, default=datetime.utcnow)
    status      = Column(String(20), default="PENDING")
    reviewed_at = Column(DateTime, nullable=True)
    review_note = Column(String(500), nullable=True)


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def safe_float(val):
    try:    return float(val)
    except: return None

def safe_int(val):
    try:    return int(val)
    except: return None

def safe_str(val):
    return str(val) if val is not None else None

def parse_date_safe(value, fmt=None, dayfirst=False):
    if not value:
        return None
    try:
        if fmt:
            return datetime.strptime(value, fmt).date()
        if isinstance(value, str) and value[0].isalpha():
            return datetime.strptime(value, "%B %d, %Y").date()
        if "-" in value:
            return datetime.strptime(value, "%Y-%m-%d").date()
        if "/" in value:
            day, month, year = value.split("/")
            return datetime(int(year), int(month), int(day)).date()
    except Exception:
        return None

def map_to_recommendation(item):
    """Hard validation + ORM mapping. Raises ValueError on critical failures."""
    critical_keys = ["id", "scid", "recommend_flag", "creation_date"]
    for key in critical_keys:
        if key not in item or item[key] in (None, ""):
            raise ValueError(f"Missing critical field: {key}")
    rec_price = safe_float(item.get("recommended_price"))
    if rec_price is None or rec_price == 0:
        raise ValueError(f"Invalid recommended_price: {item.get('recommended_price')}")

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
        creation_timestamp=datetime.strptime(item["creation_date"], "%Y%m%d%H%M%S"),
        recommended_price=safe_float(item.get("recommended_price")),
        target_price=safe_float(item.get("target_price")),
        cmp=safe_float(item.get("cmp")),
        price_change=safe_float(item.get("change")),
        percent_change=safe_float(item.get("perChange")),
        current_returns=safe_float(item.get("current_returns")),
        potential_returns=safe_float(item.get("potential_returns_per")),
        target_price_flag=safe_str(item.get("target_price_flag"))
    )


# ─────────────────────────────────────────
# DATA LOADERS
# ─────────────────────────────────────────

@st.cache_data(ttl=60)
def load_quarantine(status_filter: str):
    query = "SELECT * FROM recommendations_quarantine"
    if status_filter != "ALL":
        query += f" WHERE status = '{status_filter}'"
    query += " ORDER BY flagged_at DESC"
    return pd.read_sql(query, get_engine())

def get_counts():
    engine = get_engine()
    df = pd.read_sql(
        "SELECT status, COUNT(*) as cnt FROM recommendations_quarantine GROUP BY status",
        engine
    )
    counts = dict(zip(df["status"], df["cnt"]))
    return counts


# ─────────────────────────────────────────
# APPROVE ACTION
# ─────────────────────────────────────────

def approve_records(quarantine_ids: list, note: str = "") -> tuple:
    """
    Moves approved quarantine records into the recommendations table.
    Returns (success_ids, failed_ids, error_messages).
    """
    Session = sessionmaker(bind=get_engine())
    session = Session()
    success_ids, failed_ids, errors = [], [], []

    try:
        for qid in quarantine_ids:
            q_record = session.get(RecommendationQuarantine, qid)
            if not q_record:
                errors.append(f"Quarantine ID {qid} not found.")
                failed_ids.append(qid)
                continue

            try:
                item = json.loads(q_record.raw_json)

                # Check not already in main table
                existing = session.get(Recommendation, safe_int(item.get("id")))
                if existing:
                    # Already approved separately — just mark this quarantine record
                    q_record.status      = "APPROVED"
                    q_record.reviewed_at = datetime.utcnow()
                    q_record.review_note = "Already exists in recommendations table"
                    success_ids.append(qid)
                    continue

                # Hard validation + map
                rec = map_to_recommendation(item)
                session.add(rec)

                # History records
                stock_data = item.get("stock_data", {})
                for record_type in ["current", "previous"]:
                    hist = stock_data.get(record_type)
                    if not hist or isinstance(hist, list):
                        continue
                    session.add(RecommendationHistory(
                        recommendation=rec,
                        record_type=record_type,
                        recommend_flag=hist.get("recommend_flag"),
                        target_price=safe_float(hist.get("target_price")),
                        target_price_date=parse_date_safe(hist.get("target_price_date")),
                        organization=hist.get("P_ORGANIZATION")
                    ))

                # Update quarantine record
                q_record.status      = "APPROVED"
                q_record.reviewed_at = datetime.utcnow()
                q_record.review_note = note or "Manually approved"
                success_ids.append(qid)

            except (ValueError, KeyError, IntegrityError) as e:
                session.rollback()
                err_msg = f"ID {qid}: {e}"
                errors.append(err_msg)
                failed_ids.append(qid)
                continue

        session.commit()
    except Exception as e:
        session.rollback()
        errors.append(f"Fatal error: {e}")
    finally:
        session.close()

    return success_ids, failed_ids, errors


# ─────────────────────────────────────────
# REJECT ACTION
# ─────────────────────────────────────────

def reject_records(quarantine_ids: list, note: str = "") -> int:
    Session = sessionmaker(bind=get_engine())
    session = Session()
    count = 0
    try:
        for qid in quarantine_ids:
            q_record = session.get(RecommendationQuarantine, qid)
            if q_record:
                q_record.status      = "REJECTED"
                q_record.reviewed_at = datetime.utcnow()
                q_record.review_note = note or "Manually rejected"
                count += 1
        session.commit()
    except Exception as e:
        session.rollback()
        st.error(f"Error rejecting records: {e}")
    finally:
        session.close()
    return count


# ─────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────

st.markdown("""
<div style="padding: 28px 0 20px 0;">
    <div style="font-family:'DM Mono'; font-size:10px; letter-spacing:4px;
                color:#6b6b8a; text-transform:uppercase; margin-bottom:6px;">
        Stock Recos · Data Quality
    </div>
    <h1 style="font-family:'Syne',sans-serif; font-size:36px; font-weight:800;
               margin:0; letter-spacing:-1px; color:#e8e8f0;">
        Quarantine Review
    </h1>
    <div style="font-size:12px; color:#6b6b8a; margin-top:6px;">
        Records flagged during ingestion for potential data issues
    </div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────
# STATS
# ─────────────────────────────────────────

try:
    counts = get_counts()
except Exception as e:
    st.error(f"Failed to connect to database: {e}")
    st.stop()

pending_count  = counts.get("PENDING", 0)
approved_count = counts.get("APPROVED", 0)
rejected_count = counts.get("REJECTED", 0)
total_count    = pending_count + approved_count + rejected_count

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f"""<div class="stat-card">
        <div class="stat-label">Pending Review</div>
        <div class="stat-value pending">{pending_count}</div>
    </div>""", unsafe_allow_html=True)
with c2:
    st.markdown(f"""<div class="stat-card">
        <div class="stat-label">Approved</div>
        <div class="stat-value approved">{approved_count}</div>
    </div>""", unsafe_allow_html=True)
with c3:
    st.markdown(f"""<div class="stat-card">
        <div class="stat-label">Rejected</div>
        <div class="stat-value rejected">{rejected_count}</div>
    </div>""", unsafe_allow_html=True)
with c4:
    st.markdown(f"""<div class="stat-card">
        <div class="stat-label">Total Flagged</div>
        <div class="stat-value total">{total_count}</div>
    </div>""", unsafe_allow_html=True)

# ─────────────────────────────────────────
# FILTERS
# ─────────────────────────────────────────

st.markdown('<div class="section-header">Records</div>', unsafe_allow_html=True)

f1, f2, f3 = st.columns([1, 2, 1])
with f1:
    status_filter = st.selectbox("Status", ["PENDING", "ALL", "APPROVED", "REJECTED"])
with f2:
    flag_search = st.text_input("Search flag reason", placeholder="e.g. BUY call, potential_returns...")
with f3:
    st.write("")  # spacer
    st.write("")
    if st.button("🔄  Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ─────────────────────────────────────────
# LOAD + DISPLAY TABLE
# ─────────────────────────────────────────

raw_df = load_quarantine(status_filter)

if raw_df.empty:
    st.markdown(
        f'<div style="color:#6b6b8a; padding:48px; text-align:center; font-size:13px;">'
        f'No {status_filter.lower()} records found</div>',
        unsafe_allow_html=True
    )
    st.stop()

# Apply flag reason search filter
if flag_search:
    raw_df = raw_df[raw_df["flag_reason"].str.contains(flag_search, case=False, na=False)]

if raw_df.empty:
    st.warning("No records match your search.")
    st.stop()

# Build display dataframe
display_df = raw_df[["id", "source_id", "flag_reason", "flagged_at", "status", "review_note"]].copy()
display_df["flagged_at"] = pd.to_datetime(display_df["flagged_at"]).dt.strftime("%d %b %Y %H:%M")
display_df.insert(0, "Select", False)
display_df.columns = ["Select", "QID", "Rec ID", "Flag Reason", "Flagged At", "Status", "Review Note"]

# Render editable table with checkboxes
edited_df = st.data_editor(
    display_df,
    column_config={
        "Select":      st.column_config.CheckboxColumn("✓ Select", width="small"),
        "QID":         st.column_config.NumberColumn("QID", width="small"),
        "Rec ID":      st.column_config.NumberColumn("Rec ID", width="small"),
        "Flag Reason": st.column_config.TextColumn("Flag Reason", width="large"),
        "Flagged At":  st.column_config.TextColumn("Flagged At", width="medium"),
        "Status":      st.column_config.TextColumn("Status", width="small"),
        "Review Note": st.column_config.TextColumn("Review Note", width="medium"),
    },
    disabled=["QID", "Rec ID", "Flag Reason", "Flagged At", "Status", "Review Note"],
    use_container_width=True,
    height=420,
    hide_index=True,
)

selected_rows = edited_df[edited_df["Select"] == True]
selected_qids = selected_rows["QID"].tolist()

st.markdown(
    f'<div style="font-size:11px; color:#6b6b8a; margin-top:4px;">'
    f'{len(selected_rows)} of {len(display_df)} record(s) selected</div>',
    unsafe_allow_html=True
)

# ─────────────────────────────────────────
# ACTIONS
# ─────────────────────────────────────────

st.markdown('<div class="section-header">Actions</div>', unsafe_allow_html=True)

note_col, approve_col, reject_col = st.columns([3, 1, 1])

with note_col:
    review_note = st.text_input(
        "Review note (optional)",
        placeholder="e.g. Verified against NSE website — data is correct",
    )

with approve_col:
    approve_clicked = st.button(
        "✅  Approve Selected",
        use_container_width=True,
        type="primary",
        disabled=(len(selected_qids) == 0)
    )

with reject_col:
    reject_clicked = st.button(
        "❌  Reject Selected",
        use_container_width=True,
        disabled=(len(selected_qids) == 0)
    )

if approve_clicked and selected_qids:
    with st.spinner(f"Approving {len(selected_qids)} record(s)..."):
        success_ids, failed_ids, errors = approve_records(selected_qids, note=review_note)

    if success_ids:
        st.success(f"✅ Approved {len(success_ids)} record(s) → moved to recommendations table.")
    if failed_ids:
        st.error(f"❌ {len(failed_ids)} record(s) failed hard validation and were NOT approved:")
        for err in errors:
            st.markdown(f"- `{err}`")
        st.markdown(
            '<div class="info-box">💡 These records failed hard validation (e.g. missing required field, '
            'recommended_price=0). Fix the data in the raw JSON before approving, or reject them.</div>',
            unsafe_allow_html=True
        )
    st.cache_data.clear()
    st.rerun()

if reject_clicked and selected_qids:
    count = reject_records(selected_qids, note=review_note)
    st.success(f"🗑 Rejected {count} record(s). They remain in the quarantine table for audit purposes.")
    st.cache_data.clear()
    st.rerun()

# ─────────────────────────────────────────
# RAW JSON INSPECTOR
# ─────────────────────────────────────────

if len(selected_qids) == 1:
    st.markdown('<div class="section-header">Record Inspector</div>', unsafe_allow_html=True)

    q_row = raw_df[raw_df["id"] == selected_qids[0]].iloc[0]

    col_a, col_b = st.columns([2, 3])

    with col_a:
        st.markdown("**Flags detected:**")
        for flag in q_row["flag_reason"].split(";"):
            flag = flag.strip()
            if flag:
                st.markdown(f'<span class="flag-chip">⚠ {flag}</span>', unsafe_allow_html=True)

        st.markdown("")
        st.markdown(f"**Quarantine ID:** `{q_row['id']}`")
        st.markdown(f"**Source Rec ID:** `{q_row['source_id']}`")
        st.markdown(f"**Flagged at:** `{q_row['flagged_at']}`")
        st.markdown(f"**Status:** `{q_row['status']}`")
        if q_row.get("review_note"):
            st.markdown(f"**Review note:** {q_row['review_note']}")

    with col_b:
        with st.expander("📄 Raw API payload (JSON)", expanded=True):
            try:
                parsed = json.loads(q_row["raw_json"])
                # Show key fields cleanly first
                key_fields = {
                    "id":                   parsed.get("id"),
                    "stkname":              parsed.get("stkname"),
                    "scid":                 parsed.get("scid"),
                    "organization":         parsed.get("organization"),
                    "recommend_flag":       parsed.get("recommend_flag"),
                    "recommend_date":       parsed.get("recommend_date"),
                    "recommended_price":    parsed.get("recommended_price"),
                    "target_price":         parsed.get("target_price"),
                    "potential_returns_per":parsed.get("potential_returns_per"),
                    "cmp":                  parsed.get("cmp"),
                    "exchange":             parsed.get("exchange"),
                    "target_price_date":    parsed.get("target_price_date"),
                    "current_returns":      parsed.get("current_returns"),
                }
                st.json(key_fields)
                with st.expander("Full payload"):
                    st.json(parsed)
            except Exception:
                st.code(q_row["raw_json"])

elif len(selected_qids) > 1:
    st.markdown(
        '<div class="info-box">💡 Select a single record to inspect its raw API payload and flag details.</div>',
        unsafe_allow_html=True
    )

# ─────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────

st.markdown("""
<div style="margin-top:48px; padding-top:16px; border-top:1px solid #2a2a3e;
            font-size:11px; color:#3a3a5e; letter-spacing:1px; text-align:center;">
    QUARANTINE REVIEW · STOCK RECOS TRACKER · FOR INTERNAL USE ONLY
</div>
""", unsafe_allow_html=True)
