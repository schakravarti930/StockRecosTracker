import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import os
import re

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

st.set_page_config(
    page_title="Analyst Tracker",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ─────────────────────────────────────────
# STYLING
# ─────────────────────────────────────────

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

.metric-card {
    background: linear-gradient(135deg, #12121a 0%, #1a1a2e 100%);
    border: 1px solid #2a2a3e;
    border-radius: 12px;
    padding: 20px 24px;
    margin-bottom: 12px;
}
.metric-label {
    font-size: 11px;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: #6b6b8a;
    margin-bottom: 6px;
}
.metric-value {
    font-family: 'Syne', sans-serif;
    font-size: 28px;
    font-weight: 700;
    color: #e8e8f0;
}
.metric-value.positive { color: #00d4aa; }
.metric-value.negative { color: #ff5577; }

.section-header {
    font-family: 'Syne', sans-serif;
    font-size: 13px;
    letter-spacing: 3px;
    text-transform: uppercase;
    color: #6b6b8a;
    border-bottom: 1px solid #2a2a3e;
    padding-bottom: 10px;
    margin: 28px 0 18px 0;
}

.tag {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 500;
    letter-spacing: 1px;
}
.tag-buy  { background: rgba(0,212,170,0.15); color: #00d4aa; border: 1px solid rgba(0,212,170,0.3); }
.tag-sell { background: rgba(255,85,119,0.15); color: #ff5577; border: 1px solid rgba(255,85,119,0.3); }
.tag-hold { background: rgba(255,180,0,0.15);  color: #ffb400; border: 1px solid rgba(255,180,0,0.3); }

[data-testid="stDataFrame"] {
    border: 1px solid #2a2a3e;
    border-radius: 8px;
}

.stTabs [data-baseweb="tab-list"] {
    background: #12121a;
    border-bottom: 1px solid #2a2a3e;
    gap: 0;
}
.stTabs [data-baseweb="tab"] {
    font-family: 'DM Mono', monospace;
    font-size: 12px;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    color: #6b6b8a;
    padding: 12px 24px;
    border-bottom: 2px solid transparent;
}
.stTabs [aria-selected="true"] {
    color: #e8e8f0 !important;
    border-bottom: 2px solid #00d4aa !important;
    background: transparent !important;
}

div[data-testid="stSelectbox"] label,
div[data-testid="stMultiSelect"] label {
    font-size: 11px;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: #6b6b8a;
}

.stSelectbox > div > div,
.stMultiSelect > div > div {
    background: #12121a;
    border: 1px solid #2a2a3e;
    color: #e8e8f0;
}

.warning-note {
    background: rgba(255,180,0,0.08);
    border: 1px solid rgba(255,180,0,0.2);
    border-radius: 8px;
    padding: 10px 16px;
    font-size: 12px;
    color: #ffb400;
    margin-bottom: 16px;
}
</style>
""", unsafe_allow_html=True)

PLOTLY_THEME = dict(
    paper_bgcolor="#0a0a0f",
    plot_bgcolor="#0a0a0f",
    font=dict(family="DM Mono, monospace", color="#e8e8f0", size=12),
    colorway=["#00d4aa", "#ff5577", "#ffb400", "#7b9fff", "#c77dff", "#ff9e40"],
)

AXIS_STYLE = dict(gridcolor="#1e1e2e", linecolor="#2a2a3e", tickcolor="#2a2a3e")

ORGANIZATION_PALETTE = {
    "Motilal Oswal": "#00D4AA",
    "ICICI Securities": "#7B9FFF",
    "Prabhudas Lilladher": "#C77DFF",
    "Emkay Global Financial Services": "#FF9E40",
    "Nuvama": "#56B4E9",
    "JM Financial": "#E69F00",
    "HDFC Securities": "#009E73",
    "Kotak Securities": "#D55E00",
    "Axis Securities": "#CC79A7",
    "Geojit Financial Services": "#F0E442",
    "Anand Rathi": "#EE6677",
}
FALLBACK_ORGANIZATION_COLORS = px.colors.qualitative.Safe
DEFAULT_SYMBOL = "circle"
SYMBOL_CYCLE = ["diamond", "square", "x", "cross", "triangle-up", "triangle-down", "star", "pentagon"]

# ─────────────────────────────────────────
# DB CONNECTION
# ─────────────────────────────────────────

@st.cache_resource
def get_engine():
    server   = os.environ.get("AZURE_SQL_SERVER")
    database = os.environ.get("AZURE_SQL_DATABASE")
    username = os.environ.get("AZURE_SQL_USERNAME")
    password = os.environ.get("AZURE_SQL_PASSWORD")

    url = (
        f"mssql+pyodbc://{username}:{password}"
        f"@{server}.database.windows.net:1433/{database}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&Encrypt=yes&TrustServerCertificate=no&Connection+Timeout=30"
    )
    return create_engine(url)


# ─────────────────────────────────────────
# DATA LOADERS
# ─────────────────────────────────────────

@st.cache_data(ttl=3600)
def load_scorecard():
    return pd.read_sql("""
        SELECT * FROM vw_analyst_scorecard
        ORDER BY total_calls DESC
    """, get_engine())

@st.cache_data(ttl=3600)
def load_returns():
    return pd.read_sql("""
        SELECT * FROM vw_recommendation_returns
        ORDER BY recommend_date DESC
    """, get_engine())

@st.cache_data(ttl=3600)
def load_target_hit():
    return pd.read_sql("""
        SELECT * FROM vw_target_hit
        ORDER BY recommend_date DESC
    """, get_engine())


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def color_return(val):
    if pd.isna(val): return "color: #3a3a5e"
    return "color: #00d4aa" if val >= 0 else "color: #ff5577"

def fmt_pct(val, decimals=1):
    if pd.isna(val): return "—"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.{decimals}f}%"

def fmt_num(val, decimals=0):
    if pd.isna(val): return "—"
    return f"{val:.{decimals}f}"

def normalize_organization_name(name):
    if pd.isna(name):
        return name
    normalized = re.sub(r"\s+", " ", str(name).strip())
    if normalized.isupper() or normalized.islower():
        normalized = normalized.title()
    return normalized

def build_firm_color_map(organizations):
    firms = sorted(pd.Series(organizations).dropna().unique().tolist())
    known_map = ORGANIZATION_PALETTE.copy()
    fallback_idx = 0
    for firm in firms:
        if firm not in known_map:
            known_map[firm] = FALLBACK_ORGANIZATION_COLORS[fallback_idx % len(FALLBACK_ORGANIZATION_COLORS)]
            fallback_idx += 1
    return firms, known_map

def build_overflow_symbol_map(plot_df, firms):
    symbol_map = {firm: DEFAULT_SYMBOL for firm in firms}
    if len(firms) > len(ORGANIZATION_PALETTE):
        configured_firms = list(ORGANIZATION_PALETTE.keys())
        overflow_firms = [firm for firm in firms if firm not in configured_firms]
        high_frequency_firms = (
            plot_df["organization"].value_counts()
            .head(min(4, len(firms)))
            .index
            .tolist()
        )
        emphasized_firms = sorted(set(overflow_firms).union(high_frequency_firms))
        for idx, firm in enumerate(emphasized_firms):
            symbol_map[firm] = SYMBOL_CYCLE[idx % len(SYMBOL_CYCLE)]
    return symbol_map


# ─────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────

try:
    scorecard  = load_scorecard()
    returns    = load_returns()
    target_hit = load_target_hit()
    for frame in (scorecard, returns, target_hit):
        if "organization" in frame.columns:
            frame["organization"] = frame["organization"].apply(normalize_organization_name)
    ORGANIZATION_ORDER, FIRM_COLOR_MAP = build_firm_color_map(
        pd.concat(
            [returns["organization"], target_hit["organization"]],
            ignore_index=True
        )
    )
    db_ok = True
except Exception as e:
    st.error(f"Database connection failed: {e}")
    db_ok = False
    st.stop()


# ─────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────

st.markdown("""
<div style="padding: 32px 0 24px 0;">
    <div style="font-family: 'DM Mono'; font-size:11px; letter-spacing:4px; 
                color:#6b6b8a; text-transform:uppercase; margin-bottom:8px;">
        NSE · Equity Research
    </div>
    <h1 style="font-family:'Syne',sans-serif; font-size:42px; font-weight:800; 
               margin:0; letter-spacing:-1px; color:#e8e8f0;">
        Analyst Tracker
    </h1>
</div>
""", unsafe_allow_html=True)

_, header_right = st.columns([5, 1])
with header_right:
    if st.button("Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ─────────────────────────────────────────
# TOP METRICS
# ─────────────────────────────────────────

total_recs   = len(returns)
total_firms  = returns["organization"].nunique()
avg_return   = returns["return_current"].mean()
hit_rate     = (returns["direction_correct"] == 1).mean() * 100
targets_hit  = target_hit["target_hit"].sum()
target_rate  = target_hit["target_hit"].mean() * 100

c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">Total Calls</div>
        <div class="metric-value">{total_recs}</div>
    </div>""", unsafe_allow_html=True)

with c2:
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">Firms Tracked</div>
        <div class="metric-value">{total_firms}</div>
    </div>""", unsafe_allow_html=True)

with c3:
    cls = "positive" if avg_return >= 0 else "negative"
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">Avg Return (Current)</div>
        <div class="metric-value {cls}">{fmt_pct(avg_return)}</div>
    </div>""", unsafe_allow_html=True)

with c4:
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">Direction Accuracy</div>
        <div class="metric-value">{hit_rate:.1f}%</div>
    </div>""", unsafe_allow_html=True)

with c5:
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">Targets Hit</div>
        <div class="metric-value">{int(targets_hit)} <span style="font-size:16px;color:#6b6b8a">/ {len(target_hit)}</span></div>
    </div>""", unsafe_allow_html=True)

st.markdown('<div class="warning-note">⚠ Data window is 2–3 months old. 90d/180d/365d returns will populate over time. Current return uses latest CMP.</div>', unsafe_allow_html=True)

# ─────────────────────────────────────────
# TABS
# ─────────────────────────────────────────

tab1, tab2, tab3, tab4 = st.tabs(["Scorecard", "All Calls", "Target Analysis", "Stock Lookup"])


# ══════════════════════════════════════════
# TAB 1 — SCORECARD
# ══════════════════════════════════════════

with tab1:

    col_l, col_r = st.columns([3, 2])

    with col_l:
        st.markdown('<div class="section-header">Performance by Firm</div>', unsafe_allow_html=True)

        sc = scorecard.copy()
        sc["earliest_call"] = pd.to_datetime(sc["earliest_call"]).dt.strftime("%d %b %Y")

        display_cols = {
            "organization":        "Firm",
            "total_calls":         "Calls",
            "earliest_call":       "Since",
            "hit_rate_pct":        "Hit Rate",
            "avg_return_30d":      "Avg 30d",
            "avg_return_current":  "Avg Current",
            "best_call_current":   "Best",
            "worst_call_current":  "Worst",
            "target_hit_rate_pct": "Target Hit%",
            "stdev_return_current":"Stdev",
        }

        sc_display = sc[list(display_cols.keys())].rename(columns=display_cols)

        pct_cols = ["Hit Rate", "Avg 30d", "Avg Current", "Best", "Worst", "Target Hit%", "Stdev"]

        def style_scorecard(df):
            styles = pd.DataFrame("", index=df.index, columns=df.columns)
            for col in ["Avg 30d", "Avg Current", "Best", "Worst"]:
                if col in df.columns:
                    styles[col] = df[col].apply(
                        lambda v: "color: #00d4aa" if pd.notna(v) and v >= 0
                        else ("color: #ff5577" if pd.notna(v) else "color: #3a3a5e")
                    )
            return styles

        fmt_dict = {c: lambda x: fmt_pct(x) for c in pct_cols}
        fmt_dict["Calls"] = lambda x: str(int(x)) if pd.notna(x) else "—"

        st.dataframe(
            sc_display.style
                .format(fmt_dict, na_rep="—")
                .apply(style_scorecard, axis=None)
                .set_properties(**{"background-color": "#12121a", "border-color": "#2a2a3e"}),
            use_container_width=True,
            height=420
        )

    with col_r:
        st.markdown('<div class="section-header">Hit Rate Ranking</div>', unsafe_allow_html=True)

        sc_sorted = scorecard[scorecard["total_calls"] >= 3].sort_values("hit_rate_pct", ascending=True)

        fig = go.Figure(go.Bar(
            x=sc_sorted["hit_rate_pct"],
            y=sc_sorted["organization"],
            orientation="h",
            marker=dict(
                color=sc_sorted["hit_rate_pct"],
                colorscale=[[0, "#ff5577"], [0.5, "#ffb400"], [1, "#00d4aa"]],
                cmin=0, cmax=100
            ),
            text=sc_sorted["hit_rate_pct"].apply(lambda x: f"{x:.1f}%"),
            textposition="outside",
            textfont=dict(size=11)
        ))
        fig.update_layout(
            **PLOTLY_THEME,
            height=380,
            margin=dict(l=10, r=60, t=10, b=10),
            xaxis=dict(**AXIS_STYLE, range=[0, 110], showgrid=False, showticklabels=False),
            yaxis=dict(**AXIS_STYLE, showgrid=False),
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)

    # Return distribution
    st.markdown('<div class="section-header">Return Distribution by Firm</div>', unsafe_allow_html=True)

    min_calls = st.slider("Minimum calls to include firm", 1, 20, 3, key="dist_slider")
    firms_filtered = scorecard[scorecard["total_calls"] >= min_calls]["organization"].tolist()
    ret_filtered = returns[returns["organization"].isin(firms_filtered)]

    fig2 = px.box(
        ret_filtered,
        x="organization", y="return_current",
        color="organization",
        color_discrete_map=FIRM_COLOR_MAP,
        category_orders={"organization": ORGANIZATION_ORDER},
        labels={"organization": "", "return_current": "Return (%)"},
        hover_data=["stock_name"]
    )
    fig2.add_hline(y=0, line_dash="dot", line_color="#3a3a5e", line_width=1)
    fig2.update_layout(**PLOTLY_THEME, height=380, margin=dict(l=10, r=10, t=10, b=60),
                       showlegend=False, xaxis=dict(**AXIS_STYLE, tickangle=-30),
                       yaxis=dict(**AXIS_STYLE))
    st.plotly_chart(fig2, use_container_width=True)


# ══════════════════════════════════════════
# TAB 2 — ALL CALLS
# ══════════════════════════════════════════

with tab2:

    # Filters
    f1, f2, f3 = st.columns(3)

    with f1:
        org_options = ["All"] + sorted(returns["organization"].unique().tolist())
        org_filter = st.selectbox("Firm", org_options)

    with f2:
        rec_options = ["All"] + sorted(returns["analyst_recommendation"].dropna().unique().tolist())
        rec_filter = st.selectbox("Recommendation", rec_options)

    with f3:
        direction_filter = st.selectbox("Direction", ["All", "Correct", "Incorrect"])

    df = returns.copy()
    df["organization"] = df["organization"].apply(normalize_organization_name)
    if org_filter != "All":
        df = df[df["organization"] == org_filter]
    if rec_filter != "All":
        df = df[df["analyst_recommendation"] == rec_filter]
    if direction_filter == "Correct":
        df = df[df["direction_correct"] == 1]
    elif direction_filter == "Incorrect":
        df = df[df["direction_correct"] == 0]

    st.markdown(f'<div class="section-header">{len(df)} Calls</div>', unsafe_allow_html=True)

    plot_df = df.copy()
    plot_df["organization"] = plot_df["organization"].apply(normalize_organization_name)
    plot_df["analyst_recommendation"] = (
        plot_df["analyst_recommendation"]
        .astype("string")
        .str.strip()
        .str.title()
    )
    plot_df["promise_delta"] = plot_df["return_current"] - plot_df["potential_returns"]
    plot_df["promise_result"] = plot_df["promise_delta"].apply(
        lambda delta: "Outperformed" if pd.notna(delta) and delta > 0
        else ("Underperformed" if pd.notna(delta) and delta < 0 else "Met")
    )
    known_map = ORGANIZATION_PALETTE.copy()
    missing_firms = sorted(
        firm for firm in plot_df["organization"].dropna().unique().tolist()
        if firm not in known_map
    )
    for idx, firm in enumerate(missing_firms):
        known_map[firm] = FALLBACK_ORGANIZATION_COLORS[idx % len(FALLBACK_ORGANIZATION_COLORS)]

    tab2_firms = sorted(plot_df["organization"].dropna().unique().tolist())
    symbol_map = build_overflow_symbol_map(plot_df, tab2_firms)

    # Scatter: potential vs actual return
    fig3 = px.scatter(
        plot_df,
        x="potential_returns",
        y="return_current",
        color="organization",
        color_discrete_map=known_map,
        category_orders={"organization": tab2_firms},
        symbol="organization",
        symbol_map=symbol_map,
        hover_data=[
            "stock_name",
            "recommend_date",
            "recommended_price",
            "target_price",
            "promise_delta",
            "promise_result",
        ],
        labels={
            "potential_returns": "Potential Return % (at recommendation)",
            "return_current":    "Actual Return % (current)",
            "organization":      "Firm",
            "promise_delta":     "Actual - Potential %",
            "promise_result":    "Promise Outcome",
        },
        title="Promised vs Actual Return"
    )
    fig3.add_hline(y=0, line_dash="dot", line_color="#3a3a5e", line_width=1)

    x_vals = df["potential_returns"].dropna()
    y_vals = df["return_current"].dropna()
    xaxis_cfg = dict(**AXIS_STYLE)
    yaxis_cfg = dict(**AXIS_STYLE)
    x_lo, x_hi = -20.0, 30.0
    y_lo, y_hi = -20.0, 20.0

    if not x_vals.empty and not y_vals.empty:
        x_min, x_max = float(x_vals.min()), float(x_vals.max())
        y_min, y_max = float(y_vals.min()), float(y_vals.max())
        x_min = min(x_min, 0.0)
        x_max = max(x_max, 15.0)

        x_span = max(x_max - x_min, 1.0)
        y_span = max(y_max - y_min, 1.0)
        x_pad = max(x_span * 0.08, 5.0)
        y_pad = max(y_span * 0.08, 3.0)

        x_lo, x_hi = x_min - x_pad, x_max + x_pad
        y_lo, y_hi = y_min - y_pad, y_max + y_pad
        xaxis_cfg["range"] = [x_lo, x_hi]
        yaxis_cfg["range"] = [y_lo, y_hi]

    # Visible y=x segment based on final axis ranges.
    d0 = max(x_lo, y_lo)
    d1 = min(x_hi, y_hi)
    diag_visible = d1 > d0

    sell_hold_boundary = 0.0
    hold_buy_boundary = 15.0
    fig3.add_vline(x=sell_hold_boundary, line_dash="dash", line_color="#6c728f", line_width=1)
    fig3.add_vline(x=hold_buy_boundary, line_dash="dash", line_color="#6c728f", line_width=1)
    fig3.add_vrect(
        x0=x_lo,
        x1=min(sell_hold_boundary, x_hi),
        fillcolor="rgba(255, 99, 132, 0.07)",
        line_width=0,
        layer="below",
    )
    fig3.add_vrect(
        x0=max(sell_hold_boundary, x_lo),
        x1=min(hold_buy_boundary, x_hi),
        fillcolor="rgba(255, 206, 86, 0.08)",
        line_width=0,
        layer="below",
    )
    fig3.add_vrect(
        x0=max(hold_buy_boundary, x_lo),
        x1=x_hi,
        fillcolor="rgba(75, 192, 192, 0.08)",
        line_width=0,
        layer="below",
    )
    annotation_style = dict(
        showarrow=False,
        xref="paper",
        yref="paper",
        align="left",
        bgcolor="rgba(18,18,26,0.75)",
        bordercolor="#2a2a3e",
        borderwidth=1,
        font=dict(size=10, color="#cfd3e6"),
    )
    zone_annotation_style = dict(
        annotation_style,
        xref="x",
        yref="paper",
        xanchor="center",
        yanchor="top",
    )
    sell_segment = (x_lo, min(sell_hold_boundary, x_hi))
    hold_segment = (max(sell_hold_boundary, x_lo), min(hold_buy_boundary, x_hi))
    buy_segment = (max(hold_buy_boundary, x_lo), x_hi)

    zone_labels = [
        ("Sell (&lt;0%)", sell_segment, None),
        ("Hold (0–15%)", hold_segment, dict(font=dict(size=9, color="#cfd3e6"), y=1.01, yanchor="bottom")),
        ("Buy (≥15%)", buy_segment, None),
    ]
    for label_text, (seg_left, seg_right), overrides in zone_labels:
        if seg_right <= seg_left:
            continue
        segment_width = seg_right - seg_left
        if label_text.startswith("Hold") and segment_width < 2:
            continue
        annotation_props = dict(zone_annotation_style)
        if label_text.startswith("Hold") and segment_width < 4:
            annotation_props.update(overrides or {})
        fig3.add_annotation(
            x=(seg_left + seg_right) / 2,
            text=label_text,
            **dict(annotation_props, y=annotation_props.get("y", 0.98)),
        )
    fig3.add_annotation(
        x=0.99,
        y=0.01,
        text="y = x (met promise)",
        xanchor="right",
        yanchor="bottom",
        **annotation_style,
    )
    if diag_visible:
        diag_span = d1 - d0
        fig3.add_shape(
            type="line",
            x0=d0,
            y0=d0,
            x1=d1,
            y1=d1,
            line=dict(color="rgba(220, 220, 230, 0.8)", width=2, dash="dash"),
        )

        def _interp_point(t):
            point = d0 + (t * diag_span)
            return point, point

        def _clamp(val, low, high):
            return max(low, min(high, val))

        offset = max(diag_span * 0.03, 0.6)
        y_margin = max((y_hi - y_lo) * 0.03, 0.4)
        y_min_safe = y_lo + y_margin
        y_max_safe = y_hi - y_margin

        out_x0, out_y0 = _interp_point(0.78)  # upper-right side of visible segment
        out_y = _clamp(out_y0 + offset, y_min_safe, y_max_safe)
        fig3.add_annotation(
            x=out_x0 - offset,
            y=out_y,
            xref="x",
            yref="y",
            xanchor="left",
            yanchor="bottom",
            text="Outperformed promise<br>(actual &gt; potential)",
            showarrow=False,
            align="left",
            opacity=0.9,
            bgcolor="rgba(0, 212, 170, 0.18)",
            bordercolor="rgba(0, 212, 170, 0.45)",
            borderwidth=1,
            font=dict(size=10, color="#9EF5C6"),
            xshift=6,
            yshift=4,
        )

        under_x0, under_y0 = _interp_point(0.22)  # lower-left side of visible segment
        under_y = _clamp(under_y0 - offset, y_min_safe, y_max_safe)
        fig3.add_annotation(
            x=under_x0 + offset,
            y=under_y,
            xref="x",
            yref="y",
            xanchor="right",
            yanchor="top",
            text="Underperformed promise<br>(actual &lt; potential)",
            showarrow=False,
            align="left",
            opacity=0.9,
            bgcolor="rgba(230, 57, 70, 0.18)",
            bordercolor="rgba(230, 57, 70, 0.45)",
            borderwidth=1,
            font=dict(size=10, color="#FFB3C1"),
            xshift=-6,
            yshift=-4,
        )
    fig3.add_annotation(
        x=0.01,
        y=0.01,
        xanchor="left",
        yanchor="bottom",
        text="SELL zone:<br>Above line = weaker SELL call<br>(fell less than predicted)",
        **annotation_style,
    )

    fig3.update_layout(**PLOTLY_THEME, height=420, margin=dict(l=10, r=10, t=56, b=36),
                       xaxis=xaxis_cfg, yaxis=yaxis_cfg,
                       legend=dict(
                           title="Firm",
                           orientation="v",
                           yanchor="top",
                           y=1,
                           xanchor="left",
                           x=1.02,
                           traceorder="normal"
                       ))
    fig3.update_traces(marker=dict(opacity=0.88, line=dict(width=0.5, color="#0a0a0f")))
    st.plotly_chart(fig3, use_container_width=True)

    # Table
    display = df[[
        "organization", "stock_name", "analyst_recommendation",
        "recommend_date", "recommended_price", "target_price",
        "return_30d", "return_60d", "return_current",
        "potential_returns", "days_alive", "direction_correct"
    ]].copy()

    display["recommend_date"] = pd.to_datetime(display["recommend_date"]).dt.strftime("%d %b %Y")
    display["direction_correct"] = display["direction_correct"].map({1: "✓", 0: "✗"})

    display.columns = [
        "Firm", "Stock", "Call", "Date", "Entry ₹", "Target ₹",
        "30d %", "60d %", "Current %", "Upside %", "Days", "Direction"
    ]

    def style_calls(df):
        styles = pd.DataFrame("", index=df.index, columns=df.columns)
        for col in ["30d %", "60d %", "Current %"]:
            styles[col] = df[col].apply(
                lambda v: "color: #00d4aa" if pd.notna(v) and v >= 0
                else ("color: #ff5577" if pd.notna(v) else "color: #3a3a5e")
            )
        styles["Direction"] = df["Direction"].apply(
            lambda v: "color: #00d4aa" if v == "✓" else "color: #ff5577"
        )
        return styles

    st.dataframe(
        display.style
            .format({
                "Entry ₹":    lambda x: f"₹{x:,.2f}" if pd.notna(x) else "—",
                "Target ₹":   lambda x: f"₹{x:,.2f}" if pd.notna(x) else "—",
                "30d %":      lambda x: fmt_pct(x),
                "60d %":      lambda x: fmt_pct(x),
                "Current %":  lambda x: fmt_pct(x),
                "Upside %":   lambda x: fmt_pct(x),
                "Days":       lambda x: str(int(x)) if pd.notna(x) else "—",
            }, na_rep="—")
            .apply(style_calls, axis=None)
            .set_properties(**{"background-color": "#12121a", "border-color": "#2a2a3e"}),
        use_container_width=True,
        height=480
    )


# ══════════════════════════════════════════
# TAB 3 — TARGET ANALYSIS
# ══════════════════════════════════════════

with tab3:

    c1, c2 = st.columns(2)

    with c1:
        st.markdown('<div class="section-header">Target Upside vs Max Return Achieved</div>', unsafe_allow_html=True)

        fig4 = px.scatter(
            target_hit,
            x="target_upside_pct",
            y="max_return_achieved",
            color="target_hit",
            color_discrete_map={1: "#00d4aa", 0: "#ff5577"},
            hover_data=["stock_name", "organization", "recommend_date"],
            labels={
                "target_upside_pct":    "Target Upside %",
                "max_return_achieved":  "Max Return Achieved %",
                "target_hit":           "Target Hit"
            }
        )
        # Build a shared axis domain from both metrics and keep 1:1 scaling.
        x_vals = target_hit["target_upside_pct"].dropna()
        y_vals = target_hit["max_return_achieved"].dropna()
        axis_min, axis_max = -10.0, 40.0

        if not x_vals.empty or not y_vals.empty:
            combined_vals = pd.concat([x_vals, y_vals], ignore_index=True)
            if not combined_vals.empty:
                raw_min = float(combined_vals.min())
                raw_max = float(combined_vals.max())
                raw_span = max(raw_max - raw_min, 1.0)
                pad = max(raw_span * 0.08, 2.0)
                axis_min = raw_min - pad
                axis_max = raw_max + pad
                if axis_min == axis_max:
                    axis_min -= 1.0
                    axis_max += 1.0

        # Diagonal line: max_return == target_upside, drawn across the visible domain.
        fig4.add_shape(
            type="line",
            x0=axis_min,
            y0=axis_min,
            x1=axis_max,
            y1=axis_max,
            line=dict(color="#3a3a5e", dash="dot", width=1)
        )
        line_span = axis_max - axis_min
        line_label_x = min(axis_max - line_span * 0.06, axis_max - 0.5)
        line_label_x = max(line_label_x, axis_min + line_span * 0.6)
        fig4.add_annotation(
            x=line_label_x,
            y=line_label_x,
            text="max return = target upside",
            showarrow=False,
            textangle=35,
            font=dict(size=11, color="#8d93b8"),
            bgcolor="rgba(18, 18, 26, 0.7)"
        )
        fig4.add_annotation(
            x=0.02,
            y=0.99,
            xref="paper",
            yref="paper",
            text="Above line: Exceeded target",
            showarrow=False,
            align="left",
            xanchor="left",
            yanchor="top",
            font=dict(size=10, color="#c8cbe0"),
            bgcolor="rgba(18, 18, 26, 0.65)",
            bordercolor="#3a3a5e",
            borderwidth=1,
            borderpad=4
        )
        fig4.add_annotation(
            x=0.98,
            y=0.03,
            xref="paper",
            yref="paper",
            text="Below line: Missed target",
            showarrow=False,
            align="right",
            xanchor="right",
            yanchor="bottom",
            font=dict(size=10, color="#c8cbe0"),
            bgcolor="rgba(18, 18, 26, 0.65)",
            bordercolor="#3a3a5e",
            borderwidth=1,
            borderpad=4
        )
        fig4.update_layout(
            **PLOTLY_THEME,
            height=360,
            margin=dict(l=10, r=10, t=30, b=10),
            xaxis=dict(**AXIS_STYLE, range=[axis_min, axis_max]),
            yaxis=dict(
                **AXIS_STYLE,
                range=[axis_min, axis_max],
                scaleanchor="x",
                scaleratio=1,
            ),
            legend=dict(title="Hit", orientation="h", y=-0.15)
        )
        st.plotly_chart(fig4, use_container_width=True)

    with c2:
        st.markdown('<div class="section-header">Target Hit Rate by Firm</div>', unsafe_allow_html=True)

        th_firm = (
            target_hit.groupby("organization")
            .agg(total=("target_hit","count"), hits=("target_hit","sum"))
            .reset_index()
        )
        th_firm = th_firm[th_firm["total"] >= 2]
        th_firm["hit_rate"] = th_firm["hits"] / th_firm["total"] * 100
        th_firm = th_firm.sort_values("hit_rate", ascending=False)

        fig5 = go.Figure()
        fig5.add_trace(go.Bar(
            name="Hits",
            x=th_firm["organization"],
            y=th_firm["hits"],
            marker_color="#00d4aa"
        ))
        fig5.add_trace(go.Bar(
            name="Misses",
            x=th_firm["organization"],
            y=th_firm["total"] - th_firm["hits"],
            marker_color="#ff5577"
        ))
        fig5.update_layout(
            **PLOTLY_THEME,
            barmode="stack",
            height=360,
            margin=dict(l=10,r=10,t=10,b=80),
            xaxis=dict(**AXIS_STYLE, tickangle=-30),
            yaxis=dict(**AXIS_STYLE),
            legend=dict(orientation="h", y=-0.35)
        )
        st.plotly_chart(fig5, use_container_width=True)

    # Days to target distribution
    st.markdown('<div class="section-header">Days to Target (Hits Only)</div>', unsafe_allow_html=True)

    hits_only = target_hit[target_hit["target_hit"] == 1].dropna(subset=["days_to_target"])

    if len(hits_only) > 0:
        fig6 = px.histogram(
            hits_only, x="days_to_target", nbins=20,
            color="organization",
            labels={"days_to_target": "Days to Target", "count": "# Calls"},
            color_discrete_map=FIRM_COLOR_MAP,
            category_orders={"organization": ORGANIZATION_ORDER}
        )
        fig6.update_layout(**PLOTLY_THEME, height=300, margin=dict(l=10,r=10,t=10,b=10),
                           xaxis=dict(**AXIS_STYLE), yaxis=dict(**AXIS_STYLE), bargap=0.1)
        st.plotly_chart(fig6, use_container_width=True)
    else:
        st.markdown('<div style="color:#6b6b8a; padding:40px; text-align:center; font-size:13px;">No targets hit yet — check back as data matures</div>', unsafe_allow_html=True)

    # Full target table
    st.markdown('<div class="section-header">All Calls</div>', unsafe_allow_html=True)

    th_display = target_hit[[
        "organization","stock_name","analyst_recommendation",
        "recommend_date","recommended_price","target_price",
        "target_upside_pct","target_hit","days_to_target","max_return_achieved"
    ]].copy()
    th_display["recommend_date"] = pd.to_datetime(th_display["recommend_date"]).dt.strftime("%d %b %Y")
    th_display["target_hit"] = th_display["target_hit"].map({1: "✓ Hit", 0: "✗ Miss"})
    th_display.columns = ["Firm","Stock","Call","Date","Entry ₹","Target ₹",
                           "Upside %","Hit?","Days","Max Return %"]

    def style_target(df):
        styles = pd.DataFrame("", index=df.index, columns=df.columns)
        styles["Hit?"] = df["Hit?"].apply(
            lambda v: "color: #00d4aa" if "Hit" in str(v) else "color: #ff5577"
        )
        styles["Max Return %"] = df["Max Return %"].apply(
            lambda v: "color: #00d4aa" if pd.notna(v) and v >= 0 else "color: #ff5577"
        )
        return styles

    st.dataframe(
        th_display.style
            .format({
                "Entry ₹":      lambda x: f"₹{x:,.2f}" if pd.notna(x) else "—",
                "Target ₹":     lambda x: f"₹{x:,.2f}" if pd.notna(x) else "—",
                "Upside %":     lambda x: fmt_pct(x),
                "Max Return %": lambda x: fmt_pct(x),
                "Days":         lambda x: str(int(x)) if pd.notna(x) else "—",
            }, na_rep="—")
            .apply(style_target, axis=None)
            .set_properties(**{"background-color": "#12121a", "border-color": "#2a2a3e"}),
        use_container_width=True,
        height=420
    )


# ══════════════════════════════════════════
# TAB 4 — STOCK LOOKUP
# ══════════════════════════════════════════

with tab4:

    stocks = sorted(returns["stock_name"].dropna().unique().tolist())
    selected_stock = st.selectbox("Select Stock", stocks)

    stock_data = returns[returns["stock_name"] == selected_stock].copy()
    stock_targets = target_hit[target_hit["stock_name"] == selected_stock].copy()

    if len(stock_data) == 0:
        st.warning("No data found for this stock.")
    else:
        st.markdown(f'<div class="section-header">{selected_stock} — {len(stock_data)} Recommendation(s)</div>', unsafe_allow_html=True)

        m1, m2, m3, m4 = st.columns(4)

        with m1:
            latest = stock_data.iloc[0]
            st.markdown(f"""<div class="metric-card">
                <div class="metric-label">Latest Call</div>
                <div class="metric-value" style="font-size:20px">{latest['analyst_recommendation']}</div>
                <div style="color:#6b6b8a;font-size:11px;margin-top:4px">{latest['organization']}</div>
            </div>""", unsafe_allow_html=True)

        with m2:
            avg_ret = stock_data["return_current"].mean()
            cls = "positive" if avg_ret >= 0 else "negative"
            st.markdown(f"""<div class="metric-card">
                <div class="metric-label">Avg Current Return</div>
                <div class="metric-value {cls}">{fmt_pct(avg_ret)}</div>
            </div>""", unsafe_allow_html=True)

        with m3:
            avg_target = stock_data["target_price"].mean()
            st.markdown(f"""<div class="metric-card">
                <div class="metric-label">Avg Target Price</div>
                <div class="metric-value" style="font-size:20px">₹{avg_target:,.0f}</div>
            </div>""", unsafe_allow_html=True)

        with m4:
            n_firms = stock_data["organization"].nunique()
            st.markdown(f"""<div class="metric-card">
                <div class="metric-label">Firms Covering</div>
                <div class="metric-value">{n_firms}</div>
            </div>""", unsafe_allow_html=True)

        # All recs for this stock
        s_display = stock_data[[
            "organization","analyst_recommendation","recommend_date",
            "recommended_price","target_price","potential_returns",
            "return_30d","return_current","direction_correct"
        ]].copy()
        s_display["recommend_date"] = pd.to_datetime(s_display["recommend_date"]).dt.strftime("%d %b %Y")
        s_display["direction_correct"] = s_display["direction_correct"].map({1: "✓", 0: "✗"})
        s_display.columns = ["Firm","Call","Date","Entry ₹","Target ₹","Upside %","30d %","Current %","Direction"]

        def style_stock(df):
            styles = pd.DataFrame("", index=df.index, columns=df.columns)
            for col in ["30d %","Current %"]:
                styles[col] = df[col].apply(
                    lambda v: "color: #00d4aa" if pd.notna(v) and v >= 0
                    else ("color: #ff5577" if pd.notna(v) else "color: #3a3a5e")
                )
            styles["Direction"] = df["Direction"].apply(
                lambda v: "color: #00d4aa" if v == "✓" else "color: #ff5577"
            )
            return styles

        st.dataframe(
            s_display.style
                .format({
                    "Entry ₹":   lambda x: f"₹{x:,.2f}" if pd.notna(x) else "—",
                    "Target ₹":  lambda x: f"₹{x:,.2f}" if pd.notna(x) else "—",
                    "Upside %":  lambda x: fmt_pct(x),
                    "30d %":     lambda x: fmt_pct(x),
                    "Current %": lambda x: fmt_pct(x),
                }, na_rep="—")
                .apply(style_stock, axis=None)
                .set_properties(**{"background-color": "#12121a", "border-color": "#2a2a3e"}),
            use_container_width=True
        )


# ─────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────

st.markdown("""
<div style="margin-top:48px; padding-top:16px; border-top:1px solid #2a2a3e; 
            font-size:11px; color:#3a3a5e; letter-spacing:1px; text-align:center;">
    DATA REFRESHED DAILY · NSE EQUITY · FOR INFORMATIONAL USE ONLY
</div>
""", unsafe_allow_html=True)
