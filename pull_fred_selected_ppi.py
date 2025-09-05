# pull_fred_selected_ppi.py
import os, time, re, random, datetime
import pandas as pd
from fredapi import Fred

# ------------- CONFIG -------------
START_DATE  = "2016-01-01"
BASE_YEAR   = 2019
OUTPUT_XLSX = "fred_selected_ppi_2019base.xlsx"

# FRED key from env (secrets in GitHub Actions)
FRED_API_KEY = os.environ.get("FRED_API_KEY")
if not FRED_API_KEY:
    raise RuntimeError("FRED_API_KEY env var not set.")

# =======================
# SERIES (grouped & commented, auto-cleaned below)
# =======================
SERIES_IDS_BLOCK = """
# =========================
# CORE FREIGHT SERVICE PPIs
# =========================
PCU4841214841212     # Truckload (TL) line-haul only
PCU4841224841221     # LTL line-haul only
PCU482111482111412   # Rail intermodal line-haul
PCU4931249312        # Warehousing & storage (general)

# =========================
# VAN / RETAIL / WHOLESALE
# =========================
PCU423423            # Merchant wholesalers, durable goods
PCU424424            # Merchant wholesalers, nondurable goods
PCU454110454110      # Electronic shopping & mail-order (e-commerce)
PCUARETTRARETTR      # Retail trade (aggregate)
PCU452910452910      # Warehouse clubs & supercenters
PCU445110445110      # Supermarkets & grocery stores

# ==============
# PACKAGING PPIs
# ==============
PCU322211322211      # Corrugated boxes (industry)
PCU322212322212      # Folding paperboard boxes (industry)
PCU326160326160      # Plastic bottles (industry)
WPU09150301          # Corrugated containers (commodity)
WPU072A              # Plastic packaging products (commodity)
WPU066               # Plastics & resins (commodity)

# ===================
# REEFER / COLD CHAIN
# ===================
PCU311311            # Food manufacturing (aggregate)
PCU3116131161        # Animal slaughtering & meat processing
PCU493120493120      # Refrigerated warehousing & storage
PCU3115              # Dairy product manufacturing
PCU3114              # Fruit & vegetable preserving
PCU3119              # Other food manufacturing

# ============================
# INTERMODAL / INDUSTRIAL PPIs
# ============================
PCU325325            # Chemical manufacturing (industry)
WPU061               # Industrial chemicals (commodity)
WPU066               # Plastics & resins (commodity)  # dup safe
PCU332332            # Fabricated metal product mfg
PCU333333            # Machinery manufacturing
WPU101               # Iron & steel products (commodity)
PCU327320327320      # Ready-mix concrete
PCU331110331110      # Iron & steel mills (industry)
PCU327310327310      # Cement manufacturing
PCUOMFGOMFG          # Total manufacturing (industry)

# ======================
# ENERGY / INPUTS (PPI)
# ======================
WPU057303            # Diesel fuel (commodity)
WPU081               # Lumber & wood products (commodity)
""".strip()

def clean_series_ids(block: str) -> list[str]:
    ids = []
    for ln in block.splitlines():
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        # Take the first token as the series ID
        sid = ln.split()[0].upper()
        ids.append(sid)
    # Deduplicate but keep order
    seen = set()
    uniq = []
    for sid in ids:
        if sid not in seen:
            seen.add(sid)
            uniq.append(sid)
    return uniq

SERIES_IDS = clean_series_ids(SERIES_IDS_BLOCK)

# =======================
# Adaptive pacing / retries (friendly to FRED)
# =======================
MIN_PAUSE = 0.50
MAX_PAUSE = 2.50
STEP_UP_MULT = 1.5
STEP_DOWN_MULT = 0.90
SUCCESS_STREAK = 25
BASE_BACKOFF = 2.0
MAX_RETRIES_PER_CALL = 6
COOLDOWN_EVERY_N_CALLS = 120
COOLDOWN_SECONDS = 12

class AdaptivePacer:
    def __init__(self, pause=MIN_PAUSE):
        self.pause = pause
        self.successes = 0
        self.calls = 0
    def sleep(self):
        time.sleep(self.pause + random.uniform(0, 0.12))
        self.calls += 1
        if COOLDOWN_EVERY_N_CALLS and self.calls % COOLDOWN_EVERY_N_CALLS == 0:
            print(f"🛑 Cooldown: sleeping {COOLDOWN_SECONDS}s after {self.calls} calls...")
            time.sleep(COOLDOWN_SECONDS)
    def on_success(self):
        self.successes += 1
        if self.successes >= SUCCESS_STREAK:
            old = self.pause
            self.pause = max(MIN_PAUSE, self.pause * STEP_DOWN_MULT)
            if self.pause < old:
                print(f"↘️ Easing pace: {old:.2f}s → {self.pause:.2f}s")
            self.successes = 0
    def on_rate_limit(self):
        self.successes = 0
        old = self.pause
        self.pause = min(MAX_PAUSE, self.pause * STEP_UP_MULT)
        print(f"⏳ Rate-limit: pacing {old:.2f}s → {self.pause:.2f}s")

pacer = AdaptivePacer()

def retry_call(func, *args, **kwargs):
    last_err = None
    for attempt in range(1, MAX_RETRIES_PER_CALL + 1):
        try:
            out = func(*args, **kwargs)
            pacer.on_success()
            return out
        except Exception as e:
            last_err = e
            msg = str(e).lower()
            if "too many requests" in msg or "429" in msg or "rate limit" in msg:
                pacer.on_rate_limit()
                wait = BASE_BACKOFF * (2 ** (attempt - 1)) + random.uniform(0, 1.0)
                print(f"⚠️ 429/rate limit. Backoff {wait:.1f}s (attempt {attempt}/{MAX_RETRIES_PER_CALL})")
                time.sleep(wait)
            else:
                if attempt < MAX_RETRIES_PER_CALL:
                    wait = BASE_BACKOFF * (attempt ** 1.3) + random.uniform(0, 0.6)
                    print(f"⚠️ Error: {e}. Retry in {wait:.1f}s (attempt {attempt}/{MAX_RETRIES_PER_CALL})")
                    time.sleep(wait)
                else:
                    break
    raise last_err

fred = Fred(api_key=FRED_API_KEY)

def series_family(sid: str) -> str:
    m = re.match(r"^[A-Z]+", sid)
    return m.group(0) if m else sid

# ------------- METADATA -------------
meta_rows = []
for sid in SERIES_IDS:
    try:
        info = retry_call(fred.get_series_info, sid)
        meta_rows.append({
            "FRED_Code": sid,
            "Title": getattr(info, "title", "") or sid,
            "Frequency": getattr(info, "frequency", ""),
            "Units": getattr(info, "units", ""),
            "Seasonal_Adjustment": getattr(info, "seasonal_adjustment", ""),
            "Last_Updated": getattr(info, "last_updated", ""),
            "Observation_Start": getattr(info, "observation_start", ""),
            "Observation_End": getattr(info, "observation_end", ""),
            "Popularity": getattr(info, "popularity", ""),
            "Notes": getattr(info, "notes", ""),
        })
    except Exception as e:
        meta_rows.append({"FRED_Code": sid, "Title": sid, "Notes": f"(metadata error: {e})"})
    finally:
        pacer.sleep()

meta_df = pd.DataFrame(meta_rows).drop_duplicates(subset=["FRED_Code"]).sort_values("FRED_Code")

# ------------- DATA PULL -------------
records, latest_rows, failed = [], [], []
for i, sid in enumerate(SERIES_IDS, start=1):
    try:
        s = retry_call(fred.get_series, sid, observation_start=START_DATE)
        df = s.to_frame("value").reset_index().rename(columns={"index": "date"})
        if df.empty:
            failed.append({"FRED_Code": sid, "Reason": "Empty series"})
        else:
            base = df[df["date"].dt.year == BASE_YEAR]["value"].mean()
            df["index_2019=100"] = (df["value"] / base) * 100 if pd.notna(base) and base != 0 else pd.NA
            df["series_id"] = sid
            # join title from metadata if we have it
            title = meta_df.loc[meta_df["FRED_Code"] == sid, "Title"]
            df["series_label"] = title.iloc[0] if not title.empty else sid
            df["family"] = series_family(sid)
            records.append(df)
            latest_rows.append({"FRED_Code": sid, "Latest Available": df["date"].max()})
        if i % 10 == 0:
            print(f"...pulled {i}/{len(SERIES_IDS)} series")
    except Exception as e:
        failed.append({"FRED_Code": sid, "Reason": str(e)})
    finally:
        pacer.sleep()

long_df = (
    pd.concat(records, ignore_index=True).sort_values(["series_id", "date"])
    if records else
    pd.DataFrame(columns=["date","value","index_2019=100","series_id","series_label","family"])
)
latest_df = pd.DataFrame(latest_rows).sort_values("Latest Available", ascending=False)
failed_df = pd.DataFrame(failed).sort_values("FRED_Code") if failed else pd.DataFrame(columns=["FRED_Code","Reason"])

# Wide pivots
wide_idx  = long_df.pivot_table(index="date", columns="series_id", values="index_2019=100", aggfunc="last").sort_index()
wide_raw  = long_df.pivot_table(index="date", columns="series_id", values="value", aggfunc="last").sort_index()

# ------------- WRITE EXCEL -------------
with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as xw:
    long_df.to_excel(xw, sheet_name="Series_Long", index=False)         # raw + index in long form
    wide_raw.to_excel(xw, sheet_name="Wide_Raw")                         # raw values
    wide_idx.to_excel(xw, sheet_name="Wide_Index2019")                   # 2019=100
    latest_df.to_excel(xw, sheet_name="Latest_Dates", index=False)
    meta_df.to_excel(xw, sheet_name="Metadata", index=False)
    failed_df.to_excel(xw, sheet_name="Failed", index=False)
    # Optional: family pages
    for fam, fam_df in long_df.groupby("family"):
        fam_df.sort_values(["series_id","date"]).to_excel(xw, sheet_name=fam[:31], index=False)

print(f"✅ Saved {OUTPUT_XLSX} with {long_df['series_id'].nunique()} series.")
if not failed_df.empty:
    print(f"⚠️ {len(failed_df)} series failed (see 'Failed' sheet).")
