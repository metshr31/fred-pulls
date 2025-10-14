# pull_fred_series_bulk_split_pivot_adaptive_test.py
# Minimal smoke test for FRED bulk pull plumbing.
# - Small list of known-good monthly series
# - 2019=100 rebasing
# - Wide pivot
# - Hard-skip bad IDs; short retry/backoff for rate limits
# - Writes a single Excel file (default: fred_series_2019base.xlsx)

import os, time, random, datetime as dt
import pandas as pd
from fredapi import Fred

# ------------------ CONFIG ------------------
START_DATE  = "2016-01-01"
BASE_YEAR   = 2019
OUT_NAME    = "fred_series_2019base.xlsx"  # your workflow moves it to outputs/; that's fine

# Keep this list TINY for a fast, reliable CI test
TEST_SERIES = {
    "RSNSR":              "Retail & Food Services (NSA)",        # monthly, long history
    "IPG333N":            "IP: Machinery (NSA)",                 # monthly
    "TRUCKD11":           "ATA Truck Tonnage (SA)",              # monthly
    "FRGSHPUSM649NCIS":   "Cass Freight Shipments (NSA)",        # monthly
    "FRGEXPUSM649NCIS":   "Cass Freight Expenditures (NSA)",     # monthly
}

# ------------------ ENV / FRED ------------------
FRED_API_KEY = os.environ.get("FRED_API_KEY")
if not FRED_API_KEY:
    raise RuntimeError("FRED_API_KEY env var not set (define it in GitHub Secrets or your shell).")
fred = Fred(api_key=FRED_API_KEY)

# ------------------ Pacing / Retry ------------------
MIN_PAUSE = 0.35
MAX_PAUSE = 2.0
STEP_UP   = 1.4
STEP_DOWN = 0.9
SUCCESS_STREAK = 12
BASE_BACKOFF = 1.6
MAX_TRIES = 5

class Pacer:
    def __init__(self):
        self.pause = MIN_PAUSE
        self.ok = 0
    def sleep(self):
        time.sleep(self.pause + random.uniform(0, 0.08))
    def on_success(self):
        self.ok += 1
        if self.ok >= SUCCESS_STREAK:
            self.pause = max(MIN_PAUSE, self.pause * STEP_DOWN)
            self.ok = 0
    def on_rl(self):
        self.ok = 0
        self.pause = min(MAX_PAUSE, self.pause * STEP_UP)

pacer = Pacer()

def retry_call(func, *args, **kwargs):
    """Retry with short backoff; hard-skip permanent 4xx/bad-id errors."""
    last = None
    for attempt in range(1, MAX_TRIES + 1):
        try:
            out = func(*args, **kwargs)
            pacer.on_success()
            return out
        except Exception as e:
            msg = str(e).lower()
            # hard, non-retryable:
            if ("does not exist" in msg) or ("bad request" in msg) or ("404" in msg):
                raise
            # rate limit cases:
            if ("too many requests" in msg) or ("429" in msg) or ("rate limit" in msg):
                pacer.on_rl()
                wait = BASE_BACKOFF * (2 ** (attempt - 1))
                print(f"⚠️ rate limit; backoff {wait:.1f}s (try {attempt}/{MAX_TRIES})")
                time.sleep(wait)
                continue
            # transient:
            last = e
            if attempt < MAX_TRIES:
                wait = BASE_BACKOFF * (attempt ** 1.1)
                print(f"⚠️ {e} — retrying in {wait:.1f}s (try {attempt}/{MAX_TRIES})")
                time.sleep(wait)
            else:
                break
    raise last

# ------------------ Helpers ------------------
def rebase_2019(df: pd.DataFrame, value_col: str, base_year: int) -> pd.Series:
    df = df.copy()
    df["year"] = df["date"].dt.year
    base = df.loc[df["year"] == base_year, value_col].mean()
    if pd.isna(base) or base == 0:
        return pd.Series(pd.NA, index=df.index, dtype="float64", name="index_2019=100")
    out = (df[value_col] / base) * 100.0
    out.name = "index_2019=100"
    return out

# ------------------ Run ------------------
def main():
    t0 = time.time()
    print("Running test script with SAVE_PATH=outputs")

    records = []
    failed  = []

    for sid, label in TEST_SERIES.items():
        try:
            pacer.sleep()
            s = retry_call(fred.get_series, sid, observation_start=START_DATE)
            df = s.to_frame("value").reset_index().rename(columns={"index": "date"})
            if df.empty:
                failed.append({"FRED_Code": sid, "Reason": "Empty series"})
                continue
            df["date"] = pd.to_datetime(df["date"])
            df["index_2019=100"] = rebase_2019(df, "value", BASE_YEAR)
            df["series_id"] = sid
            df["series_label"] = label
            records.append(df)
        except Exception as e:
            failed.append({"FRED_Code": sid, "Reason": str(e)})

    if records:
        long_df = pd.concat(records, ignore_index=True).sort_values(["series_id","date"])
        wide_df = long_df.pivot_table(index="date", columns="series_id",
                                      values="index_2019=100", aggfunc="last").sort_index()
    else:
        long_df = pd.DataFrame(columns=["date","value","index_2019=100","series_id","series_label"])
        wide_df = pd.DataFrame()

    failed_df = pd.DataFrame(failed)

    # Write a single Excel file (your workflow collects it)
    out_path = OUT_NAME
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as xw:
        long_df.to_excel(xw, sheet_name="Series_Long", index=False)
        wide_df.to_excel(xw, sheet_name="Wide_Index2019")
        if not failed_df.empty:
            failed_df.to_excel(xw, sheet_name="Failed", index=False)

    print(f"✅ Test Excel written: {out_path}")
    print("After run, tree:")
    for p in [p for p in os.listdir(".") if p.endswith(".xlsx")]:
        print(" -", p)
    print(f"⏱️ Elapsed: {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
