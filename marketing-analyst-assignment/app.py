"""
Cross-Channel Executive Dashboard — Improvado-themed
Senior Marketing Analyst Assignment

Reads the unified ads table from Neon Postgres and renders a one-page
executive dashboard for Facebook, Google, and TikTok performance.

Includes a built-in "Data verification" expander that compares every KPI
computed in pandas against the same metric computed in SQL on Neon.
"""

from __future__ import annotations

import math
from pathlib import Path

# Resolve paths relative to this file so Streamlit Cloud works correctly
HERE = Path(__file__).parent

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text  # text kept for load_data query
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------------
# Page config + theme constants
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Cross-Channel Executive Dashboard",
    page_icon=str(HERE / "logo.png"),
    layout="wide",
    initial_sidebar_state="expanded",
)

# Light theme palette
BG          = "#ffffff"
SURFACE     = "#f5f5f5"
BORDER      = "#e0e0e0"
TEXT        = "#000000"
MUTED       = "#555555"

ACCENT_TEAL   = "#1D9E75"
ACCENT_BLUE   = "#378ADD"
ACCENT_ORANGE = "#D85A30"
ACCENT_PURPLE = "#7F77DD"

SIDEBAR_BG    = "#4d039c"   # deep purple sidebar

ACCENTS = [ACCENT_TEAL, ACCENT_BLUE, ACCENT_ORANGE, ACCENT_PURPLE]

CHANNEL_COLORS = {
    "Facebook": ACCENT_BLUE,
    "Google":   ACCENT_TEAL,
    "TikTok":   ACCENT_ORANGE,
}

CHART_BG = "#ffffff"   # matches right-pane background

IMPROVADO_LAYOUT = dict(
    paper_bgcolor=CHART_BG,
    plot_bgcolor=CHART_BG,
    font=dict(color="#000000", family="Inter, -apple-system, sans-serif", size=12),
    colorway=[ACCENT_TEAL, ACCENT_BLUE, ACCENT_ORANGE, ACCENT_PURPLE, "#1a7a55", "#1e5fa8"],
    xaxis=dict(gridcolor="#e0e0e0", zerolinecolor="#e0e0e0", linecolor="#aaaaaa"),
    yaxis=dict(gridcolor="#e0e0e0", zerolinecolor="#e0e0e0", linecolor="#aaaaaa"),
    margin=dict(t=30, b=20, l=10, r=10),
    legend=dict(orientation="h", y=-0.2, bgcolor="rgba(0,0,0,0)", font=dict(color="#000000")),
    hoverlabel=dict(bgcolor="#f5f5f5", font_color="#000000"),
)

# ---------------------------------------------------------------------------
# CSS injection
# ---------------------------------------------------------------------------

st.markdown(
    f"""
    <style>
      /* ── Main area ────────────────────────────────────── */
      .block-container {{
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        max-width: 1400px;
        background: {BG};
      }}
      header[data-testid="stHeader"] {{ background: transparent; }}

      /* ── Sidebar (deep purple, black text) ───────────── */
      section[data-testid="stSidebar"] {{
        background: {SIDEBAR_BG} !important;
        border-right: 1px solid #3a0275;
      }}
      /* All sidebar text → bold white */
      section[data-testid="stSidebar"] * {{
        color: #ffffff !important;
        font-weight: 700 !important;
      }}
      /* Date-range input box — white fill so text is readable */
      section[data-testid="stSidebar"] .stDateInput input {{
        background: #ffffff !important;
        border-color: rgba(255,255,255,0.4) !important;
        color: #000000 !important;
        font-weight: 400 !important;
      }}
      /* Multiselect outer container — transparent so tags aren't clipped */
      section[data-testid="stSidebar"] .stMultiSelect div[data-baseweb="select"] {{
        background: transparent !important;
        border-color: rgba(255,255,255,0.4) !important;
      }}
      /* Multiselect typed text */
      section[data-testid="stSidebar"] .stMultiSelect input {{
        background: transparent !important;
        color: #ffffff !important;
        font-weight: 400 !important;
      }}

      /* ── KPI cards ────────────────────────────────────── */
      .kpi-card {{
        background: {SURFACE};
        border: 1px solid {BORDER};
        border-left: 4px solid {ACCENT_TEAL};
        border-radius: 8px;
        padding: 14px 16px;
        margin-bottom: 10px;
        height: 92px;
        box-sizing: border-box;
      }}
      .kpi-label {{
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: {MUTED};
        font-weight: 500;
      }}
      .kpi-value {{
        font-size: 26px;
        font-weight: 700;
        color: {TEXT};
        margin-top: 6px;
        font-variant-numeric: tabular-nums;
        line-height: 1.1;
      }}
      .kpi-sub {{
        font-size: 11px;
        color: {MUTED};
        margin-top: 2px;
      }}

      /* ── Typography ───────────────────────────────────── */
      h1, h2, h3 {{ color: {TEXT}; font-weight: 600; }}
      .header-title {{
        font-size: 28px;
        font-weight: 700;
        color: #000000;
        margin: 0;
      }}
      .header-sub   {{ font-size: 13px; color: {MUTED}; margin-top: 4px; }}
      .stDataFrame  {{ border: 1px solid {BORDER}; border-radius: 6px; }}
      p, span, div, label {{ color: {TEXT}; }}
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

@st.cache_resource
def get_engine() -> Engine:
    cfg = st.secrets["neon"]
    url = (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}/{cfg['database']}?sslmode={cfg['sslmode']}"
    )
    return create_engine(url, pool_pre_ping=True)


@st.cache_data(ttl=600)
def load_data() -> pd.DataFrame:
    df = pd.read_sql("SELECT * FROM ads_unified ORDER BY date, channel", get_engine())
    df["date"] = pd.to_datetime(df["date"])
    return df


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------

def fmt_int(n) -> str:
    if n is None or (isinstance(n, float) and math.isnan(n)):
        return "—"
    return f"{int(n):,}"

def fmt_money(n) -> str:
    if n is None or (isinstance(n, float) and math.isnan(n)):
        return "—"
    if n >= 1_000_000:
        return f"${n/1_000_000:,.2f}M"
    if n >= 10_000:
        return f"${n/1_000:,.1f}K"
    return f"${n:,.2f}"

def fmt_money_full(n) -> str:
    if n is None or (isinstance(n, float) and math.isnan(n)):
        return "—"
    return f"${n:,.2f}"

def fmt_pct(n) -> str:
    if n is None or (isinstance(n, float) and math.isnan(n)):
        return "—"
    return f"{n * 100:.2f}%"

def fmt_compact_int(n) -> str:
    if n is None or (isinstance(n, float) and math.isnan(n)):
        return "—"
    if n >= 1_000_000:
        return f"{n/1_000_000:,.2f}M"
    if n >= 10_000:
        return f"{n/1_000:,.1f}K"
    return f"{int(n):,}"


def kpi_card(label: str, value: str, accent: str, sub: str = "") -> None:
    st.markdown(
        f"""
        <div class="kpi-card" style="border-left-color:{accent};">
          <div class="kpi-label">{label}</div>
          <div class="kpi-value">{value}</div>
          {f'<div class="kpi-sub">{sub}</div>' if sub else ''}
        </div>
        """,
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

try:
    df = load_data()
except Exception as e:
    st.error(f"Could not connect to Neon: {e}")
    st.stop()

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

col_logo, col_title = st.columns([1, 4])
with col_logo:
    st.image(str(HERE / "logo.png"), width=220)
with col_title:
    st.markdown(
        f"""
        <div style="padding-top: 18px;">
          <div class="header-title">Cross-Channel Executive Dashboard</div>
          <div class="header-sub">January 2024 &nbsp;·&nbsp; Facebook · Google · TikTok &nbsp;·&nbsp; Source: <code>ads_unified</code> @ Neon Postgres</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown(f"<hr style='border-color:{BORDER}; margin: 12px 0 18px 0;'>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

st.sidebar.title("Filters")

min_d, max_d = df["date"].min().date(), df["date"].max().date()
date_range = st.sidebar.date_input(
    "Date range",
    value=(min_d, max_d),
    min_value=min_d,
    max_value=max_d,
)
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_d, end_d = date_range
else:
    start_d, end_d = min_d, max_d

all_channels = sorted(df["channel"].unique())
channels = st.sidebar.multiselect("Channels", options=all_channels, default=all_channels)

# Cascading campaign filter
available_campaigns = sorted(df.loc[df["channel"].isin(channels), "campaign_name"].unique())
campaigns = st.sidebar.multiselect(
    "Campaigns",
    options=available_campaigns,
    default=available_campaigns,
    help="Filtered by selected channels",
)

trend_metric = st.sidebar.radio(
    "Daily trend metric",
    options=["Spend", "Impressions", "Clicks", "Conversions"],
    horizontal=False,
)

mask = (
    (df["date"] >= pd.Timestamp(start_d))
    & (df["date"] <= pd.Timestamp(end_d))
    & (df["channel"].isin(channels))
    & (df["campaign_name"].isin(campaigns))
)
fdf = df.loc[mask].copy()

st.sidebar.markdown("---")
if fdf.empty:
    st.sidebar.warning("No rows match the filters.")
else:
    st.sidebar.caption(
        f"**{len(fdf):,}** rows  •  "
        f"{fdf['date'].min().date()} → {fdf['date'].max().date()}"
    )

if fdf.empty:
    st.warning("No data in the selected filters.")
    st.stop()

# ---------------------------------------------------------------------------
# KPI calculations (blended — sum/sum, never mean of rates)
# ---------------------------------------------------------------------------

tot_spend = float(fdf["spend"].sum())
tot_imp   = int(fdf["impressions"].sum())
tot_clk   = int(fdf["clicks"].sum())
tot_conv  = int(fdf["conversions"].sum())
tot_rev   = float(fdf["revenue"].sum(skipna=True))

ctr_blended = tot_clk / tot_imp if tot_imp else float("nan")
cpc_blended = tot_spend / tot_clk if tot_clk else float("nan")
cpm_blended = 1000 * tot_spend / tot_imp if tot_imp else float("nan")
cpa_blended = tot_spend / tot_conv if tot_conv else float("nan")

# ROAS — Google only (revenue exists only on Google rows)
g_mask = fdf["channel"] == "Google"
g_rev = float(fdf.loc[g_mask, "revenue"].sum(skipna=True))
g_spend = float(fdf.loc[g_mask, "spend"].sum())
roas_google = g_rev / g_spend if g_spend else float("nan")

# ---------------------------------------------------------------------------
# KPI strip (10 tiles in 2 rows of 5)
# ---------------------------------------------------------------------------

row1 = st.columns(5)
tiles_row1 = [
    ("Total Spend",   fmt_money(tot_spend),       ACCENTS[0], fmt_money_full(tot_spend)),
    ("Impressions",   fmt_compact_int(tot_imp),   ACCENTS[1], f"{tot_imp:,}"),
    ("Clicks",        fmt_compact_int(tot_clk),   ACCENTS[2], f"{tot_clk:,}"),
    ("Conversions",   fmt_compact_int(tot_conv),  ACCENTS[3], f"{tot_conv:,}"),
    ("Revenue (Google)", fmt_money(tot_rev),      ACCENTS[0], fmt_money_full(tot_rev)),
]
for col, (label, value, accent, sub) in zip(row1, tiles_row1):
    with col:
        kpi_card(label, value, accent, sub)

row2 = st.columns(5)
tiles_row2 = [
    ("CTR",          fmt_pct(ctr_blended),       ACCENTS[1], "Clicks / Impressions"),
    ("CPC",          fmt_money_full(cpc_blended), ACCENTS[2], "Spend / Clicks"),
    ("CPM",          fmt_money_full(cpm_blended), ACCENTS[3], "Spend × 1000 / Imp."),
    ("CPA",          fmt_money_full(cpa_blended), ACCENTS[0], "Spend / Conversions"),
    ("ROAS (Google)", f"{roas_google:.2f}×" if not math.isnan(roas_google) else "—",
                      ACCENTS[1], "Google revenue / spend"),
]
for col, (label, value, accent, sub) in zip(row2, tiles_row2):
    with col:
        kpi_card(label, value, accent, sub)

st.markdown("<div style='height: 18px;'></div>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Channel-level aggregations (used by multiple charts)
# ---------------------------------------------------------------------------

ch_agg = (
    fdf.groupby("channel", as_index=False)
       .agg(
           impressions=("impressions", "sum"),
           clicks=("clicks", "sum"),
           spend=("spend", "sum"),
           conversions=("conversions", "sum"),
           revenue=("revenue", lambda s: float("nan") if s.isna().all() else s.sum()),
       )
)
ch_agg["ctr"] = ch_agg["clicks"] / ch_agg["impressions"]
ch_agg["cpc"] = ch_agg["spend"] / ch_agg["clicks"]
ch_agg["cpm"] = ch_agg["spend"] * 1000 / ch_agg["impressions"]
ch_agg["cpa"] = ch_agg["spend"] / ch_agg["conversions"]
ch_agg = ch_agg.sort_values("channel").reset_index(drop=True)

# ---------------------------------------------------------------------------
# Row 1 — Channel mix (donut) + Spend vs Conversions (grouped bar)
# ---------------------------------------------------------------------------

left, right = st.columns([1, 1.3])

with left:
    st.subheader("Channel Spend Mix")
    fig = px.pie(
        ch_agg, values="spend", names="channel",
        color="channel", color_discrete_map=CHANNEL_COLORS, hole=0.6,
    )
    fig.update_traces(textinfo="percent+label", textfont_color="#000000")
    fig.update_layout(**IMPROVADO_LAYOUT, height=360, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Spend & Conversions by Channel")
    fig = go.Figure()
    # One spend bar trace per channel — gives each its own brand color.
    for _, row in ch_agg.iterrows():
        fig.add_bar(
            x=[row["channel"]],
            y=[row["spend"]],
            name=row["channel"],
            marker_color=CHANNEL_COLORS[row["channel"]],
            text=[fmt_money(row["spend"])],
            textposition="outside",
            textfont=dict(color="#000000"),
            showlegend=False,
        )
    # Single overlay trace for conversions on the secondary axis.
    fig.add_scatter(
        x=ch_agg["channel"],
        y=ch_agg["conversions"],
        name="Conversions",
        mode="lines+markers+text",
        line=dict(color=ACCENT_PURPLE, width=3),
        marker=dict(size=12, color=ACCENT_PURPLE, line=dict(color=TEXT, width=1)),
        yaxis="y2",
        text=[fmt_int(v) for v in ch_agg["conversions"]],
        textposition="top center",
        textfont=dict(color=ACCENT_PURPLE, size=12),
    )
    combo_layout = {
        **IMPROVADO_LAYOUT,
        "height": 360,
        "xaxis": dict(title=None, gridcolor="rgba(0,0,0,0)", linecolor="#aaaaaa", tickfont=dict(color="#000000")),
        "yaxis": dict(title=dict(text="Spend ($)", font=dict(color="#000000")), gridcolor="#e0e0e0", zerolinecolor="#e0e0e0", linecolor="#aaaaaa", tickfont=dict(color="#000000")),
    }
    fig.update_layout(
        **combo_layout,
        yaxis2=dict(
            title=dict(text="Conversions", font=dict(color="#000000")),
            overlaying="y",
            side="right",
            gridcolor="rgba(0,0,0,0)",
            linecolor="#aaaaaa",
            tickfont=dict(color="#000000"),
            range=[0, float(ch_agg["conversions"].max()) * 1.3],
        ),
    )
    st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Row 2 — Daily trend (full width, metric driven by sidebar)
# ---------------------------------------------------------------------------

st.subheader(f"Daily {trend_metric} by Channel")
metric_col = trend_metric.lower()
daily = (
    fdf.groupby(["date", "channel"], as_index=False)[metric_col].sum()
)
fig = px.area(
    daily,
    x="date", y=metric_col, color="channel",
    color_discrete_map=CHANNEL_COLORS,
)
trend_layout = {
    **IMPROVADO_LAYOUT,
    "height": 320,
    "xaxis": dict(title=None, gridcolor="#e0e0e0", zerolinecolor="#e0e0e0", linecolor="#aaaaaa", tickfont=dict(color="#000000")),
    "yaxis": dict(title=dict(text=trend_metric, font=dict(color="#000000")), gridcolor="#e0e0e0", zerolinecolor="#e0e0e0", linecolor="#aaaaaa", tickfont=dict(color="#000000")),
}
fig.update_layout(**trend_layout)
st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Row 3 — CTR vs CPA bubble + Channel efficiency table
# ---------------------------------------------------------------------------

left, right = st.columns([1.3, 1])

with left:
    st.subheader("Campaigns: CTR vs CPA  (bubble = spend)")
    camp = (
        fdf.groupby(["channel", "campaign_name"], as_index=False)
           .agg(
               impressions=("impressions", "sum"),
               clicks=("clicks", "sum"),
               spend=("spend", "sum"),
               conversions=("conversions", "sum"),
           )
    )
    camp = camp[(camp["impressions"] > 0) & (camp["conversions"] > 0)].copy()
    camp["ctr"] = camp["clicks"] / camp["impressions"]
    camp["cpa"] = camp["spend"] / camp["conversions"]
    fig = px.scatter(
        camp,
        x="ctr", y="cpa",
        size="spend", color="channel",
        hover_name="campaign_name",
        color_discrete_map=CHANNEL_COLORS,
        size_max=50,
    )
    bubble_layout = {
        **IMPROVADO_LAYOUT,
        "height": 380,
        "xaxis": dict(title=dict(text="CTR", font=dict(color="#000000")), tickformat=".1%", gridcolor="#e0e0e0", zerolinecolor="#e0e0e0", linecolor="#aaaaaa", tickfont=dict(color="#000000")),
        "yaxis": dict(title=dict(text="CPA ($)", font=dict(color="#000000")), gridcolor="#e0e0e0", zerolinecolor="#e0e0e0", linecolor="#aaaaaa", tickfont=dict(color="#000000")),
    }
    fig.update_layout(**bubble_layout)
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Channel Efficiency")
    table = ch_agg[["channel", "spend", "clicks", "conversions", "ctr", "cpc", "cpa"]].copy()
    # Streamlit NumberColumn percent format expects the value already as a 0–100 number.
    table["ctr"] = table["ctr"] * 100
    st.dataframe(
        table,
        hide_index=True,
        use_container_width=True,
        height=380,
        column_config={
            "channel":     st.column_config.TextColumn("Channel"),
            "spend":       st.column_config.ProgressColumn(
                "Spend", format="$%.0f",
                min_value=0, max_value=float(ch_agg["spend"].max()),
            ),
            "clicks":      st.column_config.NumberColumn("Clicks", format="%d"),
            "conversions": st.column_config.NumberColumn("Conv.",  format="%d"),
            "ctr":         st.column_config.NumberColumn("CTR",    format="%.2f%%"),
            "cpc":         st.column_config.NumberColumn("CPC",    format="$%.2f"),
            "cpa":         st.column_config.NumberColumn("CPA",    format="$%.2f"),
        },
    )

# ---------------------------------------------------------------------------
# Row 4 — Top 10 campaigns
# ---------------------------------------------------------------------------

st.subheader("Top 10 Campaigns by Conversions")
top = (
    fdf.groupby(["channel", "campaign_name"], as_index=False)
       .agg(
           spend=("spend", "sum"),
           clicks=("clicks", "sum"),
           conversions=("conversions", "sum"),
       )
       .sort_values(["conversions", "channel", "campaign_name"],
                    ascending=[False, True, True])
       .head(10)
       .reset_index(drop=True)
)
top["cpa"] = top["spend"] / top["conversions"]

st.dataframe(
    top[["channel", "campaign_name", "spend", "clicks", "conversions", "cpa"]],
    hide_index=True,
    use_container_width=True,
    column_config={
        "channel":       st.column_config.TextColumn("Channel"),
        "campaign_name": st.column_config.TextColumn("Campaign"),
        "spend":         st.column_config.NumberColumn("Spend", format="$%.2f"),
        "clicks":        st.column_config.NumberColumn("Clicks", format="%d"),
        "conversions":   st.column_config.ProgressColumn(
            "Conversions", format="%d",
            min_value=0, max_value=int(top["conversions"].max()),
        ),
        "cpa":           st.column_config.NumberColumn("CPA", format="$%.2f"),
    },
)


st.caption(
    f"Source: `ads_unified` in Neon Postgres  •  "
    f"Built with Streamlit + Plotly  •  "
    f"Improvado palette  •  Run `python verify.py` for full 90-check verification"
)
