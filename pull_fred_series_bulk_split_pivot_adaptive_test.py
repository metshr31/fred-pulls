# pull_fred_series_bulk_split_pivot_adaptive_test.py
import os
import pandas as pd
from fredapi import Fred

START_DATE = "2016-01-01"
BASE_YEAR  = 2019
OUTPUT_XLSX = "fred_series_2019base.xlsx"

FRED_API_KEY = os.environ.get("FRED_API_KEY")
if not FRED_API_KEY:
    raise RuntimeError("FRED_API_KEY env var not set")

fred = Fred(api_key=FRED_API_KEY)

SERIES = {
    "IPMANSICS": "IP: Manufacturing (Total, SA)",
    "TRUCKD11":  "ATA Truck Tonnage (SA)",
}

def fetch_rebase(code: str) -> pd.DataFrame:
    s = fred.get_series(code, observation_start=START_DATE)
    if s is None or len(s) == 0:
        return pd.DataFrame(columns=["date","value","index_2019=100","series_id","series_label"])
    df = s.to_frame("value").reset_index().rename(columns={"index": "date"})
    df["date"] = pd.to_datetime(df["date"])
    base = df[df["date"].dt.year == BASE_YEAR]["value"].mean()
    if pd.isna(base) or base == 0:
        base = df["value"].head(12).mean()
        if pd.isna(base) or base == 0:
            base = 1.0
    df["index_2019=100"] = (df["value"] / base) * 100.0
    return df

frames = []
for code, label in SERIES.items():
    df = fetch_rebase(code)
    if not df.empty:
        df["series_id"] = code
        df["series_label"] = label
        frames.append(df)

if not frames:
    raise RuntimeError("No data pulled from FRED in test script.")

long_df = pd.concat(frames, ignore_index=True).sort_values(["series_id","date"])
wide_idx = long_df.pivot_table(index="date", columns="series_id", values="index_2019=100", aggfunc="last").sort_index()

with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as xw:
    long_df.to_excel(xw, sheet_name="Series_Long", index=False)
    wide_idx.to_excel(xw, sheet_name="Wide_Index2019")

print(f"✅ Test Excel written: {OUTPUT_XLSX}")
