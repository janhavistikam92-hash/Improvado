"""
verify.py — Cross-check every dashboard KPI and chart aggregate against the
ads_unified table in Neon Postgres.

For each metric, compute the value two ways:
  * SQL  — issued directly to Neon
  * pandas — computed on the loaded dataframe

Print a side-by-side OK/FAIL table and exit non-zero on any mismatch.

Run:
    python verify.py
"""

from __future__ import annotations

import math
import sys
import tomllib
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TOL = 1e-6
SECRETS_PATH = Path(__file__).parent / ".streamlit" / "secrets.toml"

FAILED: list[str] = []
PASSED: int = 0


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def load_secrets() -> dict:
    return tomllib.loads(SECRETS_PATH.read_text(encoding="utf-8"))["neon"]


def make_engine(cfg: dict) -> Engine:
    url = (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}/{cfg['database']}?sslmode={cfg['sslmode']}"
    )
    return create_engine(url, pool_pre_ping=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_missing(x) -> bool:
    """Treat None and NaN equivalently — both mean 'no data'."""
    if x is None:
        return True
    try:
        return math.isnan(float(x))
    except (TypeError, ValueError):
        return False


def approx_equal(a, b, tol: float = TOL) -> bool:
    a_missing, b_missing = _is_missing(a), _is_missing(b)
    if a_missing and b_missing:
        return True
    if a_missing or b_missing:
        return False
    try:
        af, bf = float(a), float(b)
    except (TypeError, ValueError):
        return a == b
    denom = max(abs(af), abs(bf), 1.0)
    return abs(af - bf) / denom < tol


def fmt(v) -> str:
    if v is None:
        return "None"
    if isinstance(v, float):
        if math.isnan(v):
            return "NaN"
        return f"{v:,.6f}".rstrip("0").rstrip(".")
    if isinstance(v, int):
        return f"{v:,}"
    return str(v)


def check(label: str, sql_val, pd_val) -> None:
    global PASSED
    ok = approx_equal(sql_val, pd_val)
    mark = "OK  " if ok else "FAIL"
    print(f"  [{mark}] {label:<46}  sql={fmt(sql_val):<22}  pandas={fmt(pd_val)}")
    if ok:
        PASSED += 1
    else:
        FAILED.append(label)


def header(title: str) -> None:
    print()
    print(f"== {title} ==")


# ---------------------------------------------------------------------------
# Per-chart SQL queries (kept here as the source of truth for each visual)
# ---------------------------------------------------------------------------

SQL_CHANNEL_ROLLUP = """
SELECT channel,
       SUM(impressions)::bigint                                 AS impressions,
       SUM(clicks)::bigint                                      AS clicks,
       SUM(spend)::numeric                                      AS spend,
       SUM(conversions)::bigint                                 AS conversions,
       SUM(revenue)::numeric                                    AS revenue,
       SUM(clicks)::numeric / NULLIF(SUM(impressions), 0)       AS ctr,
       SUM(spend)::numeric  / NULLIF(SUM(clicks), 0)            AS cpc,
       SUM(spend)::numeric * 1000 / NULLIF(SUM(impressions), 0) AS cpm,
       SUM(spend)::numeric  / NULLIF(SUM(conversions), 0)       AS cpa
FROM ads_unified
GROUP BY channel
ORDER BY channel;
"""

SQL_DAILY_TREND = """
SELECT date, channel, SUM(spend)::numeric AS spend
FROM ads_unified
GROUP BY date, channel
ORDER BY date, channel;
"""

SQL_CAMPAIGN_BUBBLE = """
SELECT channel, campaign_name,
       SUM(impressions)::bigint AS impressions,
       SUM(clicks)::bigint      AS clicks,
       SUM(spend)::numeric      AS spend,
       SUM(conversions)::bigint AS conversions,
       SUM(clicks)::numeric / NULLIF(SUM(impressions), 0) AS ctr,
       SUM(spend)::numeric  / NULLIF(SUM(conversions), 0) AS cpa
FROM ads_unified
GROUP BY channel, campaign_name
HAVING SUM(impressions) > 0 AND SUM(conversions) > 0
ORDER BY channel, campaign_name;
"""

SQL_TOP10_CAMPAIGNS = """
SELECT channel, campaign_name,
       SUM(spend)::numeric      AS spend,
       SUM(clicks)::bigint      AS clicks,
       SUM(conversions)::bigint AS conversions,
       SUM(spend)::numeric / NULLIF(SUM(conversions), 0) AS cpa
FROM ads_unified
GROUP BY channel, campaign_name
ORDER BY SUM(conversions) DESC, channel, campaign_name
LIMIT 10;
"""


# ---------------------------------------------------------------------------
# Verification blocks
# ---------------------------------------------------------------------------

def verify_row_count(engine: Engine, df: pd.DataFrame) -> None:
    header("Row count")
    with engine.connect() as conn:
        sql_n = conn.execute(text("SELECT COUNT(*) FROM ads_unified")).scalar()
    check("ads_unified row count", sql_n, len(df))


def verify_global_totals(engine: Engine, df: pd.DataFrame) -> None:
    header("Global totals")
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT SUM(impressions)::bigint AS impressions,
                   SUM(clicks)::bigint      AS clicks,
                   SUM(spend)::numeric      AS spend,
                   SUM(conversions)::bigint AS conversions,
                   SUM(revenue)::numeric    AS revenue
            FROM ads_unified
        """)).mappings().one()

    check("global.impressions", int(row["impressions"]), int(df["impressions"].sum()))
    check("global.clicks",      int(row["clicks"]),      int(df["clicks"].sum()))
    check("global.spend",       float(row["spend"]),     float(df["spend"].sum()))
    check("global.conversions", int(row["conversions"]), int(df["conversions"].sum()))
    check("global.revenue",     float(row["revenue"]),   float(df["revenue"].sum(skipna=True)))


def verify_global_blended(engine: Engine, df: pd.DataFrame) -> None:
    header("Global blended rates")
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT SUM(clicks)::numeric / NULLIF(SUM(impressions), 0)       AS ctr,
                   SUM(spend)::numeric  / NULLIF(SUM(clicks), 0)            AS cpc,
                   SUM(spend)::numeric * 1000 / NULLIF(SUM(impressions), 0) AS cpm,
                   SUM(spend)::numeric  / NULLIF(SUM(conversions), 0)       AS cpa
            FROM ads_unified
        """)).mappings().one()

    imp = df["impressions"].sum()
    clk = df["clicks"].sum()
    spd = df["spend"].sum()
    cnv = df["conversions"].sum()

    check("global.ctr", float(row["ctr"]), clk / imp)
    check("global.cpc", float(row["cpc"]), spd / clk)
    check("global.cpm", float(row["cpm"]), 1000 * spd / imp)
    check("global.cpa", float(row["cpa"]), spd / cnv)

    # ROAS — Google only
    with engine.connect() as conn:
        roas_sql = conn.execute(text("""
            SELECT SUM(revenue) FILTER (WHERE channel = 'Google')::numeric
                 / NULLIF(SUM(spend) FILTER (WHERE channel = 'Google'), 0)
            FROM ads_unified
        """)).scalar()
    g = df[df["channel"] == "Google"]
    roas_pd = float(g["revenue"].sum()) / float(g["spend"].sum())
    check("global.roas_google", float(roas_sql), roas_pd)


def verify_per_channel(engine: Engine, df: pd.DataFrame) -> None:
    header("Per-channel rollup (matches Charts 1, 2, 5)")
    sql_df = pd.read_sql(SQL_CHANNEL_ROLLUP, engine)
    # Use min_count=1 on revenue so all-NaN channel groups (Facebook, TikTok)
    # stay NaN — matching SQL's SUM(NULL,...,NULL) = NULL semantics.
    pd_df = (
        df.groupby("channel", as_index=False)
          .agg(
              impressions=("impressions", "sum"),
              clicks=("clicks", "sum"),
              spend=("spend", "sum"),
              conversions=("conversions", "sum"),
              revenue=("revenue", lambda s: float("nan") if s.isna().all() else s.sum()),
          )
          .sort_values("channel")
          .reset_index(drop=True)
    )
    pd_df["ctr"] = pd_df["clicks"] / pd_df["impressions"]
    pd_df["cpc"] = pd_df["spend"] / pd_df["clicks"]
    pd_df["cpm"] = pd_df["spend"] * 1000 / pd_df["impressions"]
    pd_df["cpa"] = pd_df["spend"] / pd_df["conversions"]

    sql_df = sql_df.sort_values("channel").reset_index(drop=True)

    for i, ch in enumerate(sql_df["channel"]):
        for col in ["impressions", "clicks", "spend", "conversions", "revenue", "ctr", "cpc", "cpm", "cpa"]:
            sval = sql_df.loc[i, col]
            pval = pd_df.loc[i, col]
            check(f"{ch}.{col}",
                  float(sval) if sval is not None else None,
                  float(pval) if pd.notna(pval) else None)


def verify_daily_trend(engine: Engine, df: pd.DataFrame) -> None:
    header("Daily trend (Chart 3)")
    sql_df = pd.read_sql(SQL_DAILY_TREND, engine)
    pd_df = (
        df.groupby(["date", "channel"], as_index=False)["spend"].sum()
          .sort_values(["date", "channel"]).reset_index(drop=True)
    )
    sql_df["date"] = pd.to_datetime(sql_df["date"])
    pd_df["date"] = pd.to_datetime(pd_df["date"])

    check("daily_trend.row_count", len(sql_df), len(pd_df))
    check("daily_trend.total_spend", float(sql_df["spend"].sum()), float(pd_df["spend"].sum()))
    # Check first and last row
    check("daily_trend.first_row.spend", float(sql_df.iloc[0]["spend"]), float(pd_df.iloc[0]["spend"]))
    check("daily_trend.last_row.spend",  float(sql_df.iloc[-1]["spend"]), float(pd_df.iloc[-1]["spend"]))


def verify_campaign_bubble(engine: Engine, df: pd.DataFrame) -> None:
    header("Campaign efficiency bubble (Chart 4)")
    sql_df = pd.read_sql(SQL_CAMPAIGN_BUBBLE, engine)
    pd_df = (
        df.groupby(["channel", "campaign_name"], as_index=False)
          .agg(impressions=("impressions", "sum"),
               clicks=("clicks", "sum"),
               spend=("spend", "sum"),
               conversions=("conversions", "sum"))
    )
    pd_df = pd_df[(pd_df["impressions"] > 0) & (pd_df["conversions"] > 0)]
    pd_df["ctr"] = pd_df["clicks"] / pd_df["impressions"]
    pd_df["cpa"] = pd_df["spend"] / pd_df["conversions"]
    pd_df = pd_df.sort_values(["channel", "campaign_name"]).reset_index(drop=True)
    sql_df = sql_df.sort_values(["channel", "campaign_name"]).reset_index(drop=True)

    check("bubble.row_count", len(sql_df), len(pd_df))
    if len(sql_df) == len(pd_df):
        # Compare aggregate metrics
        check("bubble.total_spend",       float(sql_df["spend"].sum()),       float(pd_df["spend"].sum()))
        check("bubble.total_conversions", int(sql_df["conversions"].sum()),   int(pd_df["conversions"].sum()))
        # Spot-check first and last
        for i in (0, len(sql_df) - 1):
            label = f"bubble[{i}].{sql_df.iloc[i]['campaign_name']}"
            check(f"{label}.ctr", float(sql_df.iloc[i]["ctr"]), float(pd_df.iloc[i]["ctr"]))
            check(f"{label}.cpa", float(sql_df.iloc[i]["cpa"]), float(pd_df.iloc[i]["cpa"]))


def verify_top10(engine: Engine, df: pd.DataFrame) -> None:
    header("Top 10 campaigns by conversions (Chart 6)")
    sql_df = pd.read_sql(SQL_TOP10_CAMPAIGNS, engine)
    pd_df = (
        df.groupby(["channel", "campaign_name"], as_index=False)
          .agg(spend=("spend", "sum"),
               clicks=("clicks", "sum"),
               conversions=("conversions", "sum"))
          .sort_values(["conversions", "channel", "campaign_name"],
                       ascending=[False, True, True])
          .head(10)
          .reset_index(drop=True)
    )
    pd_df["cpa"] = pd_df["spend"] / pd_df["conversions"]

    check("top10.row_count", len(sql_df), len(pd_df))
    for i in range(min(len(sql_df), len(pd_df))):
        check(f"top10[{i}].campaign_name", sql_df.iloc[i]["campaign_name"], pd_df.iloc[i]["campaign_name"])
        check(f"top10[{i}].conversions",   int(sql_df.iloc[i]["conversions"]), int(pd_df.iloc[i]["conversions"]))
        check(f"top10[{i}].spend",         float(sql_df.iloc[i]["spend"]),     float(pd_df.iloc[i]["spend"]))
        check(f"top10[{i}].cpa",           float(sql_df.iloc[i]["cpa"]),       float(pd_df.iloc[i]["cpa"]))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print("Loading secrets and connecting to Neon...")
    cfg = load_secrets()
    engine = make_engine(cfg)

    print("Loading ads_unified into pandas...")
    df = pd.read_sql("SELECT * FROM ads_unified ORDER BY date, channel", engine)
    df["date"] = pd.to_datetime(df["date"])
    print(f"Loaded {len(df):,} rows.")

    verify_row_count(engine, df)
    verify_global_totals(engine, df)
    verify_global_blended(engine, df)
    verify_per_channel(engine, df)
    verify_daily_trend(engine, df)
    verify_campaign_bubble(engine, df)
    verify_top10(engine, df)

    total = PASSED + len(FAILED)
    print()
    print("=" * 70)
    print(f"Summary: {PASSED}/{total} checks passed.")
    if FAILED:
        print(f"FAILED ({len(FAILED)}):")
        for name in FAILED:
            print(f"  - {name}")
        return 1
    print("All KPI and chart aggregates match between SQL and pandas. [OK]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
