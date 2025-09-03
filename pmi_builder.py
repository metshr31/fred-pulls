#!/usr/bin/env python3
"""
pmi_build.py

Fetch ISM Manufacturing PMI levels and produce a 2019=100 index using a fixed base (51.2).
Data sources:
  1) Manual 2016–2019 levels (optional but included here for full history)
  2) DBnomics: ISM/pmi series 'pm' (2020-05 → latest)
  3) ISM PMI landing page scrape to patch the newest month if DBnomics lags

Outputs a CSV with columns:
  Date, PMI_level, PMI_idx

Usage:
  python pmi_build.py
  python pmi_build.py --start 2016-01-01 --out outputs/my_pmi.csv

Deps:
  pip install pandas requests
"""

import argparse
import os
import re
import sys
from typing import Optional

import pandas as pd

try:
    import requests
except ImportError as e:
    raise SystemExit(
        "Missing dependency: requests. Install with `pip install requests`."
    ) from e


# ---------------------------
# CONFIG
# ---------------------------
PMI_BASE_2019 = 51.2            # explicit 2019 mean PMI level → index base
PMI_MANUAL_BEFORE_2020 = True   # include manual 2016–2019 PMI levels below
PMI_PRE2020_START = "2016-01-01"

# 2016–2019 PMI LEVELS (not index) in order Jan 2016 → Dec 2019
PMI_LEVELS_PRE2020 = [
    48.2, 49.5, 51.8, 50.8, 51.3, 53.2, 52.6, 49.4, 51.5, 51.9, 53.2, 54.7,
    56.0, 57.7, 57.2, 54.8, 54.9, 57.8, 56.3, 58.8, 60.8, 58.7, 58.2, 59.7,
    59.1, 60.8, 59.3, 57.3, 58.7, 60.2, 58.1, 61.3, 59.8, 57.7, 59.3, 54.1,
    56.6, 54.2, 55.3, 52.8, 52.1, 51.7, 51.2, 49.1, 47.8, 48.3, 48.1, 47.2
]


# ---------------------------
# HELPERS
# ---------------------------
def index_2019_from_levels(levels: pd.Series, base_2019: float) -> pd.Series:
    """Convert PMI levels to an index where the 2019 average equals 100 using a fixed base."""
    idx = levels.astype(float) / base_2019 * 100.0
    idx.name = "PMI_idx"
    return idx


def fetch_pmi_dbnomics(start: str = "2020-05-01") -> pd.Series:
    """
    Fetch ISM PMI levels from DBnomics (dataset ISM/pmi, series 'pm').
    Returns monthly levels with MS index.
    """
    url = "https://api.db.nomics.world/v22/series/ISM/pmi/pm?observations=1&format=json"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    series = data.get("series", {})
    docs = series.get("docs", [])
    if not docs:
        raise RuntimeError("DBnomics PMI: empty docs")

    doc = docs[0]
    periods = doc.get("period", [])
    values = doc.get("value", [])
    if not periods or not values:
        raise RuntimeError("DBnomics PMI: missing period/value")

    # 'YYYY-MM' → first of month
    idx = pd.to_datetime([p + "-01" if len(p) == 7 else p for p in periods])
    s = pd.Series(values, index=idx, name="PMI_level").astype(float)
    s = s.resample("MS").last().sort_index()
    s = s[s.index >= pd.to_datetime(start)]
    return s


def fetch_pmi_latest_from_ism() -> pd.Series:
    """
    Scrape ISM's PMI landing page for the latest headline PMI level.
    Returns a single-value Series (level) indexed to the start of the reference month.
    """
    base = "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/"
    html = requests.get(base, timeout=20).text

    # Look for "Manufacturing PMI® at 48.7%" (® optional)
    m = re.search(r"Manufacturing PMI(?:®)?\s*at\s*([0-9]+(?:\.[0-9])?)\%?", html, flags=re.I)
    if not m:
        raise RuntimeError("ISM scrape: PMI headline not found")

    level = float(m.group(1))
    # Assume reference month is the previous month start (ISM releases T+1 business day)
    today = pd.Timestamp.today().normalize()
    ref_month = (today - pd.offsets.MonthBegin(1))
    return pd.Series([level], index=[ref_month], name="PMI_level")


def build_pmi_levels(start: str = "2016-01-01") -> pd.Series:
    """Compose PMI levels from manual pre-2020, DBnomics history, and ISM scrape for the latest month."""
    pieces = []

    if PMI_MANUAL_BEFORE_2020:
        pre = pd.Series(
            PMI_LEVELS_PRE2020,
            index=pd.date_range(PMI_PRE2020_START, periods=len(PMI_LEVELS_PRE2020), freq="MS"),
            name="PMI_level"
        )
        pieces.append(pre)

    # DBnomics (2020-05 → latest)
    try:
        dbn = fetch_pmi_dbnomics(start="2020-05-01")
        pieces.append(dbn)
    except Exception as e:
        print(f"[warn] DBnomics PMI fetch failed: {e}", file=sys.stderr)

    if not pieces:
        raise RuntimeError("No PMI source available (manual disabled and DBnomics failed).")

    levels = pd.concat(pieces).sort_index().resample("MS").last()

    # Patch freshest month via ISM scrape (if missing)
    try:
        latest = fetch_pmi_latest_from_ism()
        for dt, val in latest.items():
            if dt not in levels.index or pd.isna(levels.loc[dt]):
                levels.loc[dt] = val
        levels = levels.sort_index()
    except Exception as e:
        print(f"[info] ISM scrape skipped: {e}", file=sys.stderr)

    # Respect user start date
    levels = levels[levels.index >= pd.to_datetime(start)]
    levels.name = "PMI_level"
    return levels


def build_dataframe(start: str) -> pd.DataFrame:
    """Return a DataFrame with Date, PMI_level, PMI_idx (2019=100)."""
    levels = build_pmi_levels(start=start)
    idx = index_2019_from_levels(levels, PMI_BASE_2019)
    df = pd.DataFrame({"Date": levels.index, "PMI_level": levels.values, "PMI_idx": idx.values})
    return df


# ---------------------------
# MAIN
# ---------------------------
def main():
    ap = argparse.ArgumentParser(description="Build PMI levels and 2019-indexed series (base=51.2).")
    ap.add_argument("--start", default="2016-01-01", help="Start date (YYYY-MM-DD). Default: 2016-01-01")
    ap.add_argument("--out", default="outputs/pmi_levels_and_index.csv", help="Output CSV path.")
    args = ap.parse_args()

    # Ensure output folder exists
    out_dir = os.path.dirname(args.out) or "."
    os.makedirs(out_dir, exist_ok=True)

    df = build_dataframe(start=args.start)
    df.to_csv(args.out, index=False)

    # Console summary
    print(f"✅ Built PMI dataset: {df['Date'].min().date()} → {df['Date'].max().date()}")
    # Show last row for quick sanity
    last = df.iloc[-1]
    print(f"   Last month: level={last['PMI_level']:.1f}, PMI_idx(2019=100)={last['PMI_idx']:.3f}")
    print(f"📄 Saved: {args.out}")


if __name__ == "__main__":
    main()
