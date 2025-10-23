# pull_fred_series_bulk_split_pivot_adaptive.py
import os, re, time, random
import datetime as dt
from datetime import timezone # FIX: Import timezone
from pathlib import Path
import pandas as pd
import numpy as np 
from fredapi import Fred
from statsmodels.tsa.holtwinters import ExponentialSmoothing 
from dateutil.relativedelta import relativedelta 
import warnings 

# ------------------ TIMER START ------------------
warnings.filterwarnings("ignore") 
t0 = time.time()

# ------------------ CONFIG ------------------
START_DATE = "2016-01-01"
BASE_YEAR = 2019
OUTPUT_XLSX = "fred_series_2019base_with_forecasts.xlsx" 
FORECAST_HORIZON = 12 
MC_SIMS = 5000 # Number of simulations for p05/p95 bands

# ---------- Pull controls (env-configurable) ----------
PULL_MODE = os.environ.get("PULL_MODE", "FULL").upper() 
MAX_SERIES = int(os.environ.get("MAX_SERIES", "0")) 
SERIES_ALLOWLIST = [
    s.strip().upper() for s in os.environ.get("SERIES_ALLOWLIST", "").split(",")
    if s.strip() 
]

# Simple CSV cache (skip re-pull if fresher than TTL days)
CACHE_DIR = os.environ.get("CACHE_DIR", "outputs/fred_cache")
CACHE_TTL_DAYS = int(os.environ.get("CACHE_TTL_DAYS", "7"))
Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)

# ------------------ ENV / FRED ------------------
FRED_API_KEY = os.environ.get("FRED_API_KEY")
if not FRED_API_KEY:
    raise RuntimeError("FRED_API_KEY env var not set (define it in GitHub Secrets or your shell).")
fred = Fred(api_key=FRED_API_KEY)

# ------------------ Adaptive Pacing ------------------
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
        jitter = random.uniform(0, 0.12)
        time.sleep(self.pause + jitter)
        self.calls += 1
        if COOLDOWN_EVERY_N_CALLS and self.calls % COOLDOWN_EVERY_N_CALLS == 0:
            print(f"!! Cooldown: sleeping {COOLDOWN_SECONDS}s after {self.calls} calls...")
            time.sleep(COOLDOWN_SECONDS)

    def on_success(self):
        self.successes += 1
        if self.successes >= SUCCESS_STREAK:
            old = self.pause
            self.pause = max(MIN_PAUSE, self.pause * STEP_DOWN_MULT)
            if self.pause < old:
                print(f">> Easing pace: {old:.2f}s → {self.pause:.2f}s")
            self.successes = 0

    def on_rate_limit(self):
        self.successes = 0
        old = self.pause
        self.pause = min(MAX_PAUSE, self.pause * STEP_UP_MULT)
        print(f"!! Rate-limit: pacing {old:.2f}s → {self.pause:.2f}s")

pacer = AdaptivePacer()

def polite_pause():
    pacer.sleep()

# ------------------ INPUT (curated labels) ------------------
SERIES = {
    # --- Industrial Production (IP) ---
    "IPMANSICS": "IP: Manufacturing (Total)",
    "IPMAN": "IP: Manufacturing (Aggregate)",
    "IPB50001N": "IP: Non-Energy Business Supplies",
    "IPG316N": "IP: Leather & Allied Products",
    "IPG311S": "IP: Food Manufacturing",
    "IPG3113S": "IP: Sugar & Confectionery",
    "IPG311A2S": "IP: Food (excl. Beverages/Tobacco)",
    "IPG312S": "IP: Beverage & Tobacco Products",
    "IPG3112N": "IP: Grain & Oilseed Milling (NAICS 3112)",
    "IPG315N": "IP: Apparel Manufacturing",
    "IPG322S": "IP: Paper Manufacturing",
    "IPG323S": "IP: Printing & Related Support",
    "IPG324S": "IP: Petroleum & Coal Products",
    "IPG325S": "IP: Chemicals",
    "IPG326S": "IP: Plastics & Rubber Products",
    "IPG327S": "IP: Nonmetallic Mineral Products",
    "IPG3273S": "IP: Cement & Concrete Products",
    "IPG333S": "IP: Machinery Manufacturing",
    "IPG334S": "IP: Computer & Electronic Products",
    "IPG335S": "IP: Electrical Equipment, Appliances",
    "IPG3361T3S":"IP: Motor Vehicles & Parts (3361–3363)",
    "IPG3363S": "IP: Motor Vehicle Parts",
    "IPG337N": "IP: Furniture & Related",
    "IPG339N": "IP: Miscellaneous Manufacturing",
    "IPG332S": "IP: Fabricated Metal Products",
    "IPG321S": "IP: Wood Products",
    "IPN3311A2RS":"IP: Primary Metal Industries (Real)",
    "IPG3327S": "IP: Screws Nuts Bolts",
    "IPN3328S": "IP: Coating and Engraving",
    "IPN213111S":"Drilling Oil and Gas Wells",
    "IPG313S": "IP: Textile Mills",
    "IPG314S": "IP: Textile Product Mills",

    # --- Producer Prices / Freight & Costs ---
    "WPU0221": "PPI: Gasoline (Commodity)",
    "PCU325325": "PPI Industry: Chemical Mfg",
    "PCU325412325412": "PPI Industry: Pharma Prep Mfg",
    "PCU325620325620": "PPI Industry: Toilet Prep Mfg",
    "PCU484121484121": "PPI Industry: Trucking, Long-Distance TL",
    "PCU4841224841221":"PPI Industry: Trucking, Long-Distance LTL",
    "PCU482111482111412":"PPI: Long-Distance Intermodal",
    "WPU057303": "PPI: No. 2 Diesel Fuel",
    "PCU336120336120": "PPI: Heavy Duty Truck Mfg",
    "WPU141302": "PPI: Motor Vehicle Parts",
    "WPU02": "PPI: Processed Foods & Feeds",

    # --- Retail / Wholesale / Inventories (Monthly only) ---
    "RRSFS": "Advance Real Retail & Food Services (CPI-Adj)",
    "RSNSR": "Retail & Food Services (NSA)",
    "MRTSSM454US": "Retail Sales: Nonstore (SA, Monthly)",
    "RETAILIRSA": "Retail Inventories/Sales Ratio (SA)",
    "WHLSLRIRSA": "Wholesale Inventories/Sales Ratio (SA)",
    "BUSINV": "Total Business Inventories",
    "ISRATIO": "Business Inventories-to-Sales Ratio",
    "WHLSLRSMSA": "Merchant Wholesalers Sales: Total (SA, Monthly)",
    "RSFSXMV": "Retail Sales: Furn/Elect/Appliances (SA, Monthly)",
    "TLPRVCONS": "Private Construction Spending (SA, Monthly)",
    "R423IRM163SCEN":"Inventories/Sales: Wholesalers, Durable (SA)",
    "RETAILIMSA": "Retailers: Inventories (SA, Monthly)",

    # --- Orders / Housing / Sentiment ---
    "DGORDER": "Durable Goods Orders (NSA)",
    "AMTMNO": "Manufacturers' New Orders: Total",
    "NEWORDER":"New Orders: Core Capex ex Air",
    "PERMIT1": "Building Permits: 1-Unit",
    "PERMIT5": "Building Permits: 5+ Units",
    "HOUST": "Housing Starts: Total Units",
    "UMCSENT": "Michigan: Consumer Sentiment",

    # --- Capacity Utilization ---
    "CUMFNS": "Capacity Utilization: Manufacturing",
    "CAPUTLG3311A2S": "Capacity: Primary Metal Industries",
    "CAPUTLG311S": "Capacity: Food Manufacturing",
    "CAPUTLG312S": "Capacity: Beverage & Tobacco",
    "CAPUTLG325S": "Capacity: Chemicals",
    "CAPUTLG326S": "Capacity: Plastics & Rubber",

    # --- Vehicles / Assemblies ---
    "MVAAUTLTTS": "Motor Vehicle Assemblies: Autos & Light Trucks",
    "HTRUCKSSAAR":"Retail Sales: Heavy Weight Trucks (SAAR)",

    # --- Freight / Transport ---
    "TRUCKD11": "ATA Truck Tonnage (SA)",
    "FRGSHPUSM649NCIS": "Cass Freight Shipments (NSA)",
    "FRGEXPUSM649NCIS": "Cass Freight Expenditures (NSA)",

    # --- Labor / Wages ---
    "CES4300000003": "Avg Hourly Earnings: Transportation & Warehousing",
    "LNU04032231": "Unemployment Rate: Construction",

    # --- Leads ---
    "CFNAI": "Chicago Fed National Activity Index (Monthly)",
    "CFNAIMA3": "CFNAI: 3-mo MA",

    # --- Imports / PCE ---
    "IMP0004": "U.S. Imports of Goods (SA, Monthly)",
    "DGDSRX1": "Real PCE: Goods",
}

# Long list (kept same as your working file)
SERIES_IDS_RAW = """
PCU484121484121
PCU4841224841221
PCU482111482111412
WPU057303
PCU336120336120
WPU141302
WPU02
BUSINV
ISRATIO
WHLSLRSMSA
RSFSXMV
RETAILIMSA
R423IRM163SCEN
RETAILIRSA
MRTSSM454USS
RSAFS
RSNSR
IMP0004
DGDSRX1
AMTMNO
NEWORDER
DGORDER
PERMIT1
PERMIT5
HOUST
UMCSENT
IPMANSICS
IPMAN
IPB50001N
IPG316N
IPG311S
IPG3113S
IPG311A2S
IPG312S
IPG3112N
IPG315N
IPG322S
IPG323S
IPG324S
IPG325S
IPG326S
IPG327S
IPG3273S
IPG333S
IPG334S
IPG335S
IPG3361T3S
IPG3363S
IPG337N
IPG339N
IPG332S
IPG321S
IPN3311A2RS
IPN213111S
IPG3327S
IPN3328S
IPG313S
IPG314S
CUMFNS
CAPUTLG3311A2S
CAPUTLG311S
CAPUTLG312S
CAPUTLG325S
CAPUTLG326S
MVAAUTLTTS
HTRUCKSSAAR
TRUCKD11
FRGSHPUSM649NCIS
FRGEXPUSM649NCIS
CES4300000003
LNU04032231
CFNAI
CFNAIMA3
""".strip()

# ------------------ HELPERS ------------------
def clean_ids(multiline_text: str):
    lines = [ln.strip().upper() for ln in multiline_text.splitlines()
             if ln.strip() and not ln.strip().startswith("#")]
    unique, seen = [], set()
    for s in lines:
        if s not in seen:
            seen.add(s)
            unique.append(s)
    dup_count = len(lines) - len(unique)
    if dup_count:
        print(f"-> Removed {dup_count} duplicate IDs before pulling.")
    return unique

def retry_call(func, *args, **kwargs):
    """
    Retries with exponential backoff + jitter.
    - Hard-fail immediately on 'does not exist' (bad ID).
    - Backoff on 429/rate-limit.
    """
    last_err = None
    for attempt in range(1, MAX_RETRIES_PER_CALL + 1):
        try:
            result = func(*args, **kwargs)
            pacer.on_success()
            return result
        except Exception as e:
            last_err = e
            msg = str(e).lower()

            # --- PATCH: Hard-fail only on "does not exist" ---
            if "does not exist" in msg:
                break

            if "too many requests" in msg or "429" in msg or "rate limit" in msg:
                pacer.on_rate_limit()
                wait = BASE_BACKOFF * (2 ** (attempt - 1)) + random.uniform(0, 1.0)
                print(f"!! Rate limit hit. Backing off {wait:.1f}s (attempt {attempt}/{MAX_RETRIES_PER_CALL})...")
                time.sleep(wait)
            else:
                if attempt < MAX_RETRIES_PER_CALL:
                    wait = BASE_BACKOFF * (attempt ** 1.3) + random.uniform(0, 0.6)
                    print(f"!! Error: {e}. Retrying in {wait:.1f}s (attempt {attempt}/{MAX_RETRIES_PER_CALL})...")
                    time.sleep(wait)
                else:
                    break
    raise last_err

def get_series_info_safe(sid: str):
    try:
        info = retry_call(fred.get_series_info, sid)
        return {
            "FRED_Code": sid,
            "Title": getattr(info, "title", "") or sid,
            "Frequency": getattr(info, "frequency", ""),
            "Units": getattr(info, "units", ""),
            "Seasonal_Adjustment": getattr(info, "seasonal_adjustment", ""),
            "Last_Updated": getattr(info, "last_updated", ""),
            "Notes": getattr(info, "notes", ""),
            "Observation_Start": getattr(info, "observation_start", ""),
            "Observation_End": getattr(info, "observation_end", ""),
            "Popularity": getattr(info, "popularity", ""),
        }
    except Exception as e:
        return {"FRED_Code": sid, "Title": sid, "Notes": f"(metadata error: {e})"}

def series_family(sid: str) -> str:
    m = re.match(r"^[A-Z]+", sid)
    return m.group(0) if m else sid

def _cache_path(sid: str) -> Path:
    return Path(CACHE_DIR) / f"{sid}.csv"

def _load_cache_if_fresh(sid: str):
    p = _cache_path(sid)
    if not p.exists():
        return None
    
    now_utc = dt.datetime.now(timezone.utc)
    file_time_utc = dt.datetime.fromtimestamp(p.stat().st_mtime, timezone.utc)
    age_days = (now_utc - file_time_utc).days
    
    if age_days <= CACHE_TTL_DAYS:
        try:
            df = pd.read_csv(p, parse_dates=["date"])
            df = df.set_index("date")["value"]
            return df
        except Exception:
            return None
    return None

def _save_cache(sid: str, s: pd.Series):
    try:
        df = s.to_frame("value").reset_index().rename(columns={"index": "date"})
        df.to_csv(_cache_path(sid), index=False)
    except Exception:
        pass

# ------------------ FORECASTING HELPER (ENHANCED) ------------------
def get_ets_forecast(s: pd.Series, horizon: int, mc_sims: int):
    """
    Generates a robust ETS forecast with Monte Carlo prediction intervals.
    
    Returns:
        - pd.Series: The point forecast (for the summary sheet).
        - pd.DataFrame: A full table with history, forecast, and p05-p95 bands.
    """
    s = s.dropna()
    
    # --- PATCH: Add guard clause for empty series ---
    if s.empty:
        future_idx = pd.date_range(
            pd.Timestamp.today().to_period("M").to_timestamp(how="start") + relativedelta(months=1),
            periods=horizon, freq="MS"
        )
        fc_table = pd.DataFrame(index=future_idx)
        fc_table["point_forecast"] = np.nan
        for q in ["p05","p50","p95"]:
            fc_table[q] = np.nan
        return fc_table["point_forecast"], fc_table
    # --- END PATCH ---
    
    # Create a future index for the forecast
    future_idx = pd.date_range(
        s.index[-1] + relativedelta(months=1), 
        periods=horizon, 
        freq="MS"
    )
    
    # --- Create a base table to fill ---
    full_idx = s.index.union(future_idx)
    fc_table = pd.DataFrame(index=full_idx)
    fc_table["actual"] = s.reindex(full_idx)
    
    # --- Fallback 1: Not enough data, just carry forward ---
    if len(s) < 24:
        last_val = s.iloc[-1] if not s.empty else np.nan
        fc_table["point_forecast"] = np.nan
        fc_table.loc[future_idx, "point_forecast"] = last_val
        # Add bands as just the last_val
        for q in ["p05", "p50", "p95"]:
            fc_table[q] = np.nan
            fc_table.loc[future_idx, q] = last_val
        
        point_forecast = fc_table.loc[future_idx, "point_forecast"]
        return point_forecast, fc_table

    # --- Main Model: Try ETS ---
    try:
        model = ExponentialSmoothing(
            s, 
            trend="add", 
            seasonal="add", 
            seasonal_periods=12
        ).fit()
        
        fc_table["fitted"] = model.fittedvalues
        point_forecast = model.forecast(horizon)
        fc_table.loc[future_idx, "point_forecast"] = point_forecast

        # --- Robust simulation handling ---
        sims = model.simulate(
            horizon,
            repetitions=mc_sims,
            error="add",
            random_state=42
        )
        sims = np.asarray(sims)

        # Normalize to (repetitions, horizon)
        if sims.ndim == 1:
            sims = sims.reshape(1, -1)
        elif sims.shape[0] == horizon and sims.shape[1] == mc_sims:
            sims = sims.T

        sims_df = pd.DataFrame(sims, columns=future_idx)
        quantiles = sims_df.quantile([0.05, 0.50, 0.95]).T
        quantiles.columns = ["p05", "p50", "p95"]
        quantiles.index = future_idx

        fc_table = fc_table.join(quantiles)
        
        return point_forecast, fc_table

    except Exception:
        # --- Fallback 2: Carry-forward (if ETS fails) ---
        last_val = s.iloc[-1]
        fc_table["point_forecast"] = np.nan
        fc_table.loc[future_idx, "point_forecast"] = last_val
        for q in ["p05", "p50", "p95"]:
            fc_table[q] = np.nan
            fc_table.loc[future_idx, q] = last_val
            
        point_forecast = fc_table.loc[future_idx, "point_forecast"]
        return point_forecast, fc_table
# ------------------ END HELPER ------------------


# ------------------ BUILD FINAL MAP ------------------
ids_all = clean_ids(SERIES_IDS_RAW)
final_map = dict(SERIES) # curated labels first
meta_rows = []

call_counter = 0
for sid in ids_all:
    info = get_series_info_safe(sid)
    meta_rows.append(info)
    if sid not in final_map:
        final_map[sid] = info["Title"] or sid
    call_counter += 1
    polite_pause()
    if COOLDOWN_EVERY_N_CALLS and call_counter % COOLDOWN_EVERY_N_CALLS == 0:
        print(f"!! Cooldown: sleeping {COOLDOWN_SECONDS}s after {call_counter} metadata calls...")
        time.sleep(COOLDOWN_SECONDS)

# ------- Select IDs to pull based on mode -------
all_items = list(final_map.items()) # [("SID","Label"), ...]

if SERIES_ALLOWLIST:
    allow = set(SERIES_ALLOWLIST)
    all_items = [(sid, lab) for sid, lab in all_items if sid in allow]

if PULL_MODE == "TEST":
    all_items = all_items[:25] # small smoke-run
else:
    pass # FULL → keep the entire list

if MAX_SERIES and MAX_SERIES > 0:
    all_items = all_items[:MAX_SERIES]

print(f">> Pull mode: {PULL_MODE} | series selected: {len(all_items)}")

# ------------------ PULL DATA ------------------
records, latest_rows, failed = [], [], []
SUMMARY_ROWS = [] 
ALL_FORECAST_TABLES = [] # This will be used to build one big sheet
for i, (sid, label) in enumerate(all_items, start=1):
    try:
        s = _load_cache_if_fresh(sid)
        if s is None:
            raw = retry_call(fred.get_series, sid, observation_start=START_DATE)
            s = pd.Series(raw.values, index=pd.to_datetime(raw.index), name="value")
            _save_cache(sid, s)

        df = s.to_frame("value").reset_index().rename(columns={"index": "date"})
        if df.empty:
            failed.append({"FRED_Code": sid, "Reason": "Empty series"})
        else:
            base = df[df["date"].dt.year == BASE_YEAR]["value"].mean()
            df["index_2019=100"] = (df["value"] / base) * 100 if pd.notna(base) and base != 0 else pd.NA
            df["series_id"] = sid
            df["series_label"] = label
            df["family"] = series_family(sid)
            records.append(df)
            latest_rows.append({"FRED_Code": sid, "Label": label, "Latest Available": df["date"].max()})
            
            # -----------------------------------------------------------------
            # --- FORECASTING & SUMMARY BLOCK ---
            # -----------------------------------------------------------------
            try:
                s_to_forecast = df.set_index("date")["index_2019=100"].dropna()
                
                # Align to month-start
                s_to_forecast.index = s_to_forecast.index.to_period("M").to_timestamp(how="start")
                s_to_forecast = s_to_forecast.asfreq("MS") 

                if not s_to_forecast.empty and len(s_to_forecast) > 1:
                    point_forecast_series, full_forecast_table = get_ets_forecast(
                        s_to_forecast, FORECAST_HORIZON, MC_SIMS
                    )
                    
                    # Add ID cols and append to single list
                    full_forecast_table['series_id'] = sid
                    full_forecast_table['series_label'] = label
                    ALL_FORECAST_TABLES.append(full_forecast_table)

                    # Build the Summary Row
                    summary_row = {
                        "Series Name": label,
                        "FRED ID": sid,
                        "Latest Actual Date": s_to_forecast.index[-1].strftime('%Y-%m-%d'),
                        "Latest Actual Value": s_to_forecast.iloc[-1],
                    }
                    
                    for j in range(FORECAST_HORIZON):
                        col_name = f"{j+1}-Month Forecast (T+{j+1})"
                        try:
                            summary_row[col_name] = point_forecast_series.iloc[j]
                        except IndexError:
                            summary_row[col_name] = np.nan
                            
                    SUMMARY_ROWS.append(summary_row)
                
            except Exception as fc_e:
                print(f"!! Forecast failed for {sid}: {fc_e}")
            # --- END NEW BLOCK ---
            
            if i % 25 == 0:
                print(f"...pulled {i} series")
    except Exception as e:
        failed.append({"FRED_Code": sid, "Reason": str(e)})
    finally:
        call_counter += 1
        polite_pause()
        if COOLDOWN_EVERY_N_CALLS and call_counter % COOLDOWN_EVERY_N_CALLS == 0:
            print(f"!! Cooldown: sleeping {COOLDOWN_SECONDS}s after {call_counter} total calls...")
            time.sleep(COOLDOWN_SECONDS)

# ------------------ ASSEMBLE TABLES ------------------
if records:
    long_df = pd.concat(records, ignore_index=True).sort_values(["series_id", "date"])
else:
    long_df = pd.DataFrame(columns=["date","value","index_2019=100","series_id","series_label","family"])

latest_df = pd.DataFrame(latest_rows).sort_values("Latest Available", ascending=False)
failed_df = pd.DataFrame(failed).sort_values("FRED_Code") if failed else pd.DataFrame(columns=["FRED_Code","Reason"])
meta_df = pd.DataFrame(meta_rows).drop_duplicates(subset=["FRED_Code"]).sort_values("FRED_Code")

# Wide pivot (index_2019=100)
wide_idx = long_df.pivot_table(index="date", columns="series_id", values="index_2019=100", aggfunc="last").sort_index()

# ------------------ WRITE EXCEL ------------------
print(f"Writing data to {OUTPUT_XLSX}...")
with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as xw:
    
    # --- 1. Summary Forecast Sheet ---
    if SUMMARY_ROWS:
        summary_df = pd.DataFrame(SUMMARY_ROWS)
        tn_cols_ordered = [f"{i+1}-Month Forecast (T+{i+1})" for i in range(FORECAST_HORIZON)]
        fixed_cols = ["Series Name", "FRED ID", "Latest Actual Date", "Latest Actual Value"]
        final_cols = fixed_cols + tn_cols_ordered
        for c in final_cols:
            if c not in summary_df.columns:
                summary_df[c] = np.nan
        summary_df[final_cols].to_excel(xw, sheet_name="Summary_Forecasts", index=False)
    
    # --- 2. Consolidate All Forecasts ---
    if ALL_FORECAST_TABLES:
        all_fc_df = pd.concat(ALL_FORECAST_TABLES, ignore_index=False)
        all_fc_df = all_fc_df.reset_index().rename(columns={"index": "date"})
        cols_order = ["series_id", "series_label", "date", "actual", "fitted", "point_forecast", "p05", "p50", "p95"]
        present = [c for c in cols_order if c in all_fc_df.columns]
        all_fc_df[present].to_excel(xw, sheet_name="All_Forecast_Data", index=False)

    # --- 3. Core Data Sheets ---
    long_df.to_excel(xw, sheet_name="Series_Long", index=False)
    wide_idx.to_excel(xw, sheet_name="Wide_Index2019")
    latest_df.to_excel(xw, sheet_name="Latest_Dates", index=False)
    meta_df.to_excel(xw, sheet_name="Metadata", index=False) 
    failed_df.to_excel(xw, sheet_name="Failed", index=False)
    
    # --- 4. Family split (REMOVED) ---
    # for fam, fam_df in long_df.groupby("family"):
    #     fam_df.sort_values(["series_id","date"]).to_excel(xw, sheet_name=fam[:31], index=False)

# ------------------ TIMER END ------------------
elapsed = time.time() - t0
print(f"OK: Attempted {len(all_items)} series; saved {long_df['series_id'].nunique()} to {OUTPUT_XLSX}.")
if not failed_df.empty:
    print(f"!! {len(failed_df)} series failed (see 'Failed' sheet).")
if not SUMMARY_ROWS:
    print(f"!! No forecasts were generated.")
else:
    print(f"OK: Saved {len(SUMMARY_ROWS)} forecasts to 'Summary_Forecasts' and 'All_Forecast_Data'.")
print(f"-> Total runtime: {elapsed:.1f} seconds ({elapsed/60:.2f} minutes)")

