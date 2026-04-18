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

# Colorblind-safe core (Okabe-Ito style), extended with high-separation colors.
ORGANIZATION_PALETTE = [
    "#0072B2", "#E69F00", "#009E73", "#D55E00", "#CC79A7", "#56B4E9", "#F0E442", "#000000",
    "#EE7733", "#33BBEE", "#228833", "#AA3377", "#BBBBBB", "#4477AA", "#66CCEE", "#117733",
    "#CCBB44", "#EE6677", "#AA4499", "#44AA99",
]
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
    firm_color_map = {
        firm: ORGANIZATION_PALETTE[i % len(ORGANIZATION_PALETTE)]
        for i, firm in enumerate(firms)
    }
    return firms, firm_color_map

def build_overflow_symbol_map(plot_df, firms):
    symbol_map = {firm: DEFAULT_SYMBOL for firm in firms}
    if len(firms) > len(ORGANIZATION_PALETTE):
        overflow_firms = firms[len(ORGANIZATION_PALETTE):]
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
    plot_df["analyst_recommendation"] = (
        plot_df["analyst_recommendation"]
        .astype("string")
        .str.strip()
        .str.title()
    )
    tab2_firms, firm_color_map = build_firm_color_map(plot_df["organization"])
    symbol_map = build_overflow_symbol_map(plot_df, tab2_firms)

    # Scatter: potential vs actual return
    fig3 = px.scatter(
        plot_df,
        x="potential_returns",
        y="return_current",
        color="organization",
        color_discrete_map=firm_color_map,
        category_orders={"organization": tab2_firms},
        symbol="organization",
        symbol_map=symbol_map,
        hover_data=["stock_name", "recommend_date", "recommended_price", "target_price"],
        labels={
            "potential_returns": "Potential Return % (at recommendation)",
            "return_current":    "Actual Return % (current)",
            "organization":      "Firm",
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

    fig3.add_vline(x=0, line_dash="dash", line_color="#6c728f", line_width=1)
    fig3.add_vline(x=15, line_dash="dash", line_color="#6c728f", line_width=1)
    fig3.add_vrect(x0=x_lo, x1=0, fillcolor="rgba(255, 99, 132, 0.07)", line_width=0, layer="below")
    fig3.add_vrect(x0=0, x1=15, fillcolor="rgba(255, 206, 86, 0.08)", line_width=0, layer="below")
    fig3.add_vrect(x0=15, x1=x_hi, fillcolor="rgba(75, 192, 192, 0.08)", line_width=0, layer="below")
    annotation_y = y_hi - max((y_hi - y_lo) * 0.06, 1.0)
    fig3.add_annotation(x=(x_lo + 0) / 2, y=annotation_y, text="Sell (&lt;0%)", showarrow=False,
                        font=dict(size=11, color="#ff9bb0"))
    fig3.add_annotation(x=7.5, y=annotation_y, text="Hold (0–15%)", showarrow=False,
                        font=dict(size=11, color="#f6d37a"))
    fig3.add_annotation(x=(15 + x_hi) / 2, y=annotation_y, text="Buy (≥15%)", showarrow=False,
                        font=dict(size=11, color="#8ef0d5"))

    fig3.update_layout(**PLOTLY_THEME, height=420, margin=dict(l=10, r=10, t=40, b=10),
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
        # Diagonal line: if max_return == target_upside, target was just hit
        max_val = target_hit[["target_upside_pct", "max_return_achieved"]].max().max()
        fig4.add_shape(type="line", x0=0, y0=0, x1=max_val, y1=max_val,
                       line=dict(color="#3a3a5e", dash="dot", width=1))
        fig4.add_annotation(
            x=max_val * 0.58,
            y=max_val * 0.58,
            text="max return = target upside",
            showarrow=False,
            textangle=35,
            font=dict(size=11, color="#8d93b8"),
            bgcolor="rgba(18, 18, 26, 0.7)"
        )
        fig4.add_annotation(
            x=0.02,
            y=1.1,
            xref="paper",
            yref="paper",
            text="Above line: exceeded target upside | Below line: never reached target upside",
            showarrow=False,
            align="left",
            font=dict(size=11, color="#c8cbe0")
        )
        fig4.update_layout(**PLOTLY_THEME, height=360, margin=dict(l=10,r=10,t=30,b=10),
                           xaxis=dict(**AXIS_STYLE), yaxis=dict(**AXIS_STYLE),
                           legend=dict(title="Hit", orientation="h", y=-0.15))
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
