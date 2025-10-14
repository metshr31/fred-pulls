# pull_fred_series_bulk_split_pivot_adaptive.py
import os, time, re, datetime, random
import numpy as np
import pandas as pd
from fredapi import Fred
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.arima.model import ARIMA

# ------------------ TIMER START ------------------
t0 = time.time()

# ------------------ CONFIG ------------------
START_DATE          = "2016-01-01"
BASE_YEAR           = 2019
OUTPUT_XLSX         = "fred_series_2019base.xlsx"

# Prather-style forecast config
FORECAST_HORIZON    = 12         # months ahead
MC_SIMS             = 1000       # Monte Carlo paths per series (kept modest for speed)
BOOT_BLOCK_LEN      = 6          # block length for bootstrap (keeps some autocorr)
SMOOTH_TAIL         = True       # apply light HW smoothing to the point path

# ------------------ FRED API KEY (ENV ONLY) ------------------
FRED_API_KEY = os.environ.get("FRED_API_KEY")
if not FRED_API_KEY:
    raise RuntimeError("FRED_API_KEY env var not set (define it in GitHub Secrets or your shell).")

# ------------------ Adaptive Pacing ------------------
MIN_PAUSE = 0.50       # fastest allowed per-call delay
MAX_PAUSE = 2.50       # slowest allowed per-call delay
STEP_UP_MULT = 1.5     # when 429s happen, multiply pause by this
STEP_DOWN_MULT = 0.90  # after a success streak, shrink pause
SUCCESS_STREAK = 25    # successes before stepping down
BASE_BACKOFF = 2.0     # base for exponential retry backoff
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

def polite_pause():
    pacer.sleep()

# ------------------ FRED CLIENT ------------------
fred = Fred(api_key=FRED_API_KEY)

# ------------------ INPUT ------------------
SERIES = {
    # --- Industrial Production (IP) ---
    "IPMANSICS": "IP: Manufacturing (Total)",
    "IPMAN":     "IP: Manufacturing (Aggregate)",
    "IPB50001N": "IP: Non-Energy Business Supplies",
    "IPG316N":   "IP: Leather & Allied Products",
    "IPG311S":   "IP: Food Manufacturing",
    "IPG3113S":  "IP: Sugar & Confectionery",
    "IPG311A2S": "IP: Food (excl. Beverages/Tobacco)",
    "IPG312S":   "IP: Beverage & Tobacco Products",
    "IPG3112N":  "IP: Grain & Oilseed Milling (NAICS 3112)",
    "IPG315N":   "IP: Apparel Manufacturing",
    "IPG322S":   "IP: Paper Manufacturing",
    "IPG323S":   "IP: Printing & Related Support",
    "IPG324S":   "IP: Petroleum & Coal Products",
    "IPG325S":   "IP: Chemicals",
    "IPG326S":   "IP: Plastics & Rubber Products",
    "IPG327S":   "IP: Nonmetallic Mineral Products",
    "IPG3273S":  "IP: Cement & Concrete Products",
    "IPG333S":   "IP: Machinery Manufacturing",
    "IPG334S":   "IP: Computer & Electronic Products",
    "IPG335S":   "IP: Electrical Equipment, Appliances",
    "IPG3361T3S":"IP: Motor Vehicles & Parts (3361–3363)",
    "IPG3363S":  "IP: Motor Vehicle Parts",
    "IPG337N":   "IP: Furniture & Related",
    "IPG339N":   "IP: Miscellaneous Manufacturing",
    "IPG332S":   "IP: Fabricated Metal Products",
    "IPG321S":   "IP: Wood Products",
    "IPN3311A2RS":"IP: Primary Metal Industries (Real)",
    "IPG3327S":  "IP: Screws Nuts Bolts",
    "IPN3328S":  "IP: Coating and Engraving",
    "IPN213111S":"Drilling Oil and Gas Wells", 
    "IPG313S":   "IP: Textile Mills",
    "IPG314S":   "IP: Textile Product Mills",

    # --- Producer Prices (PPI) / Freight & Costs ---
    "WPU0221":         "PPI: Gasoline (Commodity)",
    "PCU325325":       "PPI Industry: Chemical Mfg",
    "PCU325412325412": "PPI Industry: Pharma Prep Mfg",
    "PCU325620325620": "PPI Industry: Toilet Prep Mfg",
    "PCU484121484121": "PPI Industry: General Freight Trucking, Long-Distance TL",
    "PCU4841224841221":"PPI Industry: General Freight Trucking, Long-Distance LTL",
    "PCU482111482111412":"PPI Industry: General Freight Trucking, Long-Distance Intermodal",
    "WPU057303":       "PPI Commodity: No. 2 Diesel Fuel",
    "PCU336120336120": "PPI Industry: Heavy Duty Truck Manufacturing",
    "WPU141302":       "PPI Commodity: Motor Vehicle Parts",
    "WPU02":           "PPI Commodity: Processed Foods & Feeds",

    # --- Retail / Wholesale / Inventories (Monthly only) ---
    "RRSFS":        "Advance Real Retail and Food Services Sales CPI Adjusted",
    "RSNSR":        "Retail & Food Services (NSA)",
    "MRTSSM4541US": "Retail Sales: Nonstore Retailers (SA, Monthly) — e-commerce proxy",
    "RETAILIRSA":   "Retail Inventories/Sales Ratio (SA)",
    "WHLSLRIRSA":   "Wholesale Inventories/Sales Ratio (SA)",
    "BUSINV":       "Total Business Inventories",
    "ISRATIO":      "Total Business Inventories-to-Sales Ratio",
    "WHLSLRSMSA":   "Merchant Wholesalers Sales: Total (SA, Monthly)",
    "RSFSXMV":      "Retail Sales: Furniture/Electronics/Appliances (SA, Monthly)",
    "TLPRVCONS":    "Total Private Construction Spending (SA, Monthly)",
    "R423IRM163SCEN":"Inventories/Sales Ratio: Wholesalers, Durable (SA, Monthly)",
    "RETAILIMSA":   "Retailers: Inventories (SA, Monthly)",

    # --- Orders / Housing / Sentiment ---
    "DGORDER": "Durable Goods Orders (NSA)",
    "AMTMNO":  "Manufacturers' New Orders: Total Manufacturing",
    "NEWORDER":"New Orders: Nondefense Capital Goods ex. Aircraft",
    "PERMIT1": "Building Permits: 1-Unit Structures",
    "PERMIT5": "Building Permits: 5+ Unit Structures",
    "HOUST":   "Housing Starts: Total Units",
    "UMCSENT": "Univ. of Michigan: Consumer Sentiment",

    # --- Capacity Utilization ---
    "CUMFNS":         "Capacity Utilization: Manufacturing",
    "CAPUTLG3311A2S": "Capacity Utilization: Primary Metal Industries",
    "CAPUTLG311S":    "Capacity Utilization: Food Manufacturing",
    "CAPUTLG312S":    "Capacity Utilization: Beverage & Tobacco",
    "CAPUTLG325S":    "Capacity Utilization: Chemicals",
    "CAPUTLG326S":    "Capacity Utilization: Plastics & Rubber",

    # --- Vehicles / Assemblies ---
    "MVAAUTLTTS": "Motor Vehicle Assemblies: Autos & Light Trucks",
    "HTRUCKSSAAR":"Motor Vehicle Retail Sales: Heavy Weight Trucks (SAAR, Monthly)",

    # --- Freight / Transport ---
    "TRUCKD11":         "ATA Truck Tonnage (SA)",
    "FRGSHPUSM649NCIS": "Cass Freight Shipments (NSA via FRED)",
    "FRGEXPUSM649NCIS": "Cass Freight Expenditures (NSA via FRED)",

    # --- Labor / Wages ---
    "CES4300000003": "Avg Hourly Earnings: Transportation & Warehousing",
    "LNU04032231":   "Unemployment Rate: Construction (CPS, Monthly)",

    # --- Leading Indicators ---
    "CFNAI":     "Chicago Fed National Activity Index (Monthly)",
    "CFNAIMA3":  "Chicago Fed National Activity Index: 3-mo MA",

    # --- Imports / PCE ---
    "IMP0004": "U.S. Imports of Goods (Customs Basis, SA, Monthly)",
    "DGDSRX1": "Real PCE: Goods",
}

# ... (SERIES_IDS_RAW unchanged; keep your long list) ...
SERIES_IDS_RAW = """<-- keep your long SERIES_IDS_RAW block exactly as in your file -->""".strip()

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
        print(f"ℹ️ Removed {dup_count} duplicate IDs before pulling.")
    return unique

def retry_call(func, *args, **kwargs):
    """
    Retries with exponential backoff + jitter.
    Integrates with AdaptivePacer:
      - on success: pacer.on_success()
      - on 429/rate-limit: pacer.on_rate_limit() and exponential wait
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
            if "too many requests" in msg or "429" in msg or "rate limit" in msg:
                pacer.on_rate_limit()
                wait = BASE_BACKOFF * (2 ** (attempt - 1)) + random.uniform(0, 1.0)
                print(f"⚠️ Rate limit hit. Backing off {wait:.1f}s (attempt {attempt}/{MAX_RETRIES_PER_CALL})...")
                time.sleep(wait)
            else:
                if attempt < MAX_RETRIES_PER_CALL:
                    wait = BASE_BACKOFF * (attempt ** 1.3) + random.uniform(0, 0.6)
                    print(f"⚠️ Error: {e}. Retrying in {wait:.1f}s (attempt {attempt}/{MAX_RETRIES_PER_CALL})...")
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

# ---- Prather-style forecaster helpers ----
def holt_winters_tail(y_fc: pd.Series) -> pd.Series:
    try:
        hw = ExponentialSmoothing(y_fc, trend="add", seasonal=None).fit(optimized=True)
        return hw.fittedvalues.reindex(y_fc.index)
    except Exception:
        return y_fc

def _robust_residual_pool(s: pd.Series, resid: np.ndarray) -> np.ndarray:
    """
    Ensure a non-degenerate residual pool:
    1) use model residuals if long/volatile,
    2) else add MoM and YoY deltas,
    3) else synthesize noise from recent MoM std with a small floor.
    """
    def _clean(a):
        a = np.asarray(a, dtype=float)
        return a[~np.isnan(a)]
    pool = _clean(resid)

    if (pool.size < 6) or (np.nanstd(pool) < 1e-6):
        mom = _clean(s.diff().values)
        yoy = _clean(s.diff(12).values)
        if mom.size + yoy.size:
            pool = _clean(np.concatenate([pool, mom, yoy]))

    if (pool.size < 6) or (np.nanstd(pool) < 1e-6):
        recent_mom = _clean(s.diff().tail(12).values)
        sigma = float(np.nanstd(recent_mom)) if recent_mom.size else 0.0
        if not np.isfinite(sigma) or sigma < 0.25:
            sigma = 0.5  # absolute floor on 2019=100 scale
        rng = np.random.default_rng(42)
        pool = rng.normal(0.0, sigma, size=240)

    return pool

def fit_resid_and_forecast(s: pd.Series, H: int):
    """
    SARIMA(1,1,1)(0,1,1)[12] → ETS(A,None) → seasonal-naive → carry-forward
    Returns: point_forecast (pd.Series), residual_pool (np.ndarray)
    """
    s = s.dropna()
    if s.empty:
        idx = pd.date_range(pd.Timestamp.today().to_period("M").to_timestamp(how="start") + pd.offsets.MonthBegin(1),
                            periods=H, freq="MS")
        return pd.Series([np.nan]*H, index=idx), np.array([0.0])

    idx = pd.date_range(s.index[-1] + pd.offsets.MonthBegin(1), periods=H, freq="MS")

    if len(s) < 18:
        point = pd.Series([s.iloc[-1]]*H, index=idx)
        resid = np.diff(s.values) if len(s) > 1 else np.array([0.0])
        return point, _robust_residual_pool(s, resid)

    # Try SARIMA
    try:
        mdl = ARIMA(s, order=(1,1,1), seasonal_order=(0,1,1,12)).fit()
        point = pd.Series(mdl.forecast(H).values, index=idx)
        resid = (s - mdl.fittedvalues).dropna().values
        return point, _robust_residual_pool(s, resid)
    except Exception:
        pass

    # Try ETS (trend only)
    try:
        ets = ExponentialSmoothing(s, trend="add", seasonal=None).fit(optimized=True)
        point = pd.Series(ets.forecast(H).values, index=idx)
        resid = (s - ets.fittedvalues).dropna().values
        return point, _robust_residual_pool(s, resid)
    except Exception:
        pass

    # Seasonal naive if possible
    if len(s) >= 24:
        try:
            vals = [s.iloc[-12 + (i % 12)] for i in range(H)]
            point = pd.Series(vals, index=idx)
            resid = (s - s.shift(12)).dropna().values
            return point, _robust_residual_pool(s, resid)
        except Exception:
            pass

    # Final fallback: carry-forward
    point = pd.Series([s.iloc[-1]]*H, index=idx)
    resid = np.diff(s.values) if len(s) > 1 else np.array([0.0])
    return point, _robust_residual_pool(s, resid)

def mc_fan(point_fc: pd.Series, resid: np.ndarray, B=MC_SIMS, block_len=BOOT_BLOCK_LEN) -> pd.DataFrame:
    """Block-bootstrap Monte Carlo fan with robustness guards."""
    H = len(point_fc)
    rng = np.random.default_rng(42)
    resid = np.asarray(resid, dtype=float)
    if resid.size < 3:
        resid = np.concatenate([resid, np.zeros(3 - resid.size)])
    if np.nanstd(resid) < 1e-8:
        resid = rng.normal(0.0, 0.5, size=240)  # last-ditch floor

    paths = np.empty((B, H))
    for b in range(B):
        noise = []
        while len(noise) < H:
            start = rng.integers(0, max(1, resid.size - max(1, block_len) + 1))
            noise.extend(resid[start:start + max(1, block_len)])
        paths[b, :] = point_fc.values + np.array(noise[:H])

    fan = pd.DataFrame(paths.T, index=point_fc.index)
    q = fan.quantile([0.05, 0.10, 0.50, 0.90, 0.95], axis=1).T
    q.columns = ["p05", "p10", "p50", "p90", "p95"]
    return q

# ------------------ BUILD FINAL MAP ------------------
ids_all = clean_ids(SERIES_IDS_RAW)
final_map = dict(SERIES)  # curated labels first
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
        print(f"🛑 Cooldown: sleeping {COOLDOWN_SECONDS}s after {call_counter} metadata calls...")
        time.sleep(COOLDOWN_SECONDS)

# ------------------ PULL DATA ------------------
records, latest_rows, failed = [], [], []
for i, (sid, label) in enumerate(final_map.items(), start=1):
    try:
        s = retry_call(fred.get_series, sid, observation_start=START_DATE)
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
        if i % 25 == 0:
            print(f"...pulled {i} series")
    except Exception as e:
        failed.append({"FRED_Code": sid, "Reason": str(e)})
    finally:
        call_counter += 1
        polite_pause()
        if COOLDOWN_EVERY_N_CALLS and call_counter % COOLDOWN_EVERY_N_CALLS == 0:
            print(f"🛑 Cooldown: sleeping {COOLDOWN_SECONDS}s after {call_counter} total calls...")
            time.sleep(COOLDOWN_SECONDS)

# ------------------ ASSEMBLE TABLES ------------------
if records:
    long_df = pd.concat(records, ignore_index=True).sort_values(["series_id", "date"])
else:
    long_df = pd.DataFrame(columns=["date","value","index_2019=100","series_id","series_label","family"])

latest_df = pd.DataFrame(latest_rows).sort_values("Latest Available", ascending=False)
failed_df = pd.DataFrame(failed).sort_values("FRED_Code") if failed else pd.DataFrame(columns=["FRED_Code","Reason"])
meta_df   = pd.DataFrame(meta_rows).drop_duplicates(subset=["FRED_Code"]).sort_values("FRED_Code")

# Wide pivot (index_2019=100)
wide_idx = long_df.pivot_table(index="date", columns="series_id", values="index_2019=100", aggfunc="last").sort_index()

# ------------------ PRATHER-STYLE FORECAST (per series) ------------------
prather_point_rows = []   # list of dict rows: date, series_id, series_label, point, point_smooth
prather_fan_rows   = []   # list of dict rows: date, series_id, series_label, p05..p95
fan_debug_rows     = []   # width stats, resid std, etc.

if not long_df.empty:
    for sid, g in long_df.groupby("series_id"):
        label = g["series_label"].iloc[0]
        s = g.set_index("date")["index_2019=100"].astype(float).sort_index()

        # Skip if no usable data
        if s.dropna().empty:
            continue

        # Fit + robust residual pool, forecast H
        point_fc, resid_pool = fit_resid_and_forecast(s, FORECAST_HORIZON)
        if SMOOTH_TAIL:
            point_s = holt_winters_tail(point_fc)
        else:
            point_s = point_fc.copy()

        # Monte Carlo fan
        fan_q = mc_fan(point_s, resid_pool, B=MC_SIMS, block_len=BOOT_BLOCK_LEN)

        # Collect rows
        for dt_idx in point_fc.index:
            prather_point_rows.append({
                "date": dt_idx, "series_id": sid, "series_label": label,
                "point": float(point_fc.loc[dt_idx]) if pd.notna(point_fc.loc[dt_idx]) else np.nan,
                "point_smooth": float(point_s.loc[dt_idx]) if pd.notna(point_s.loc[dt_idx]) else np.nan
            })
        for dt_idx in fan_q.index:
            row = {"date": dt_idx, "series_id": sid, "series_label": label}
            for col in ["p05","p10","p50","p90","p95"]:
                row[col] = float(fan_q.loc[dt_idx, col]) if pd.notna(fan_q.loc[dt_idx, col]) else np.nan
            prather_fan_rows.append(row)

        # Debug widths / residual stats
        band_w = (fan_q["p90"] - fan_q["p10"]).mean()
        fan_debug_rows.append({
            "series_id": sid,
            "series_label": label,
            "resid_std": float(np.nanstd(resid_pool)),
            "mean_bandwidth_p90_p10": float(band_w),
            "n_resid_pool": int(len(resid_pool)),
            "n_hist": int(s.dropna().shape[0])
        })

# Build DataFrames
prather_point_all = pd.DataFrame(prather_point_rows).sort_values(["series_id","date"]) if prather_point_rows else pd.DataFrame(columns=["date","series_id","series_label","point","point_smooth"])
prather_fan_all   = pd.DataFrame(prather_fan_rows).sort_values(["series_id","date"]) if prather_fan_rows else pd.DataFrame(columns=["date","series_id","series_label","p05","p10","p50","p90","p95"])
prather_debug_all = pd.DataFrame(fan_debug_rows).sort_values(["series_id"]) if fan_debug_rows else pd.DataFrame(columns=["series_id","series_label","resid_std","mean_bandwidth_p90_p10","n_resid_pool","n_hist"])

# ------------------ WRITE EXCEL ------------------
with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as xw:
    # Originals
    long_df.to_excel(xw, sheet_name="Series_Long", index=False)
    wide_idx.to_excel(xw, sheet_name="Wide_Index2019")
    latest_df.to_excel(xw, sheet_name="Latest_Dates", index=False)
    meta_df.to_excel(xw, sheet_name="Metadata", index=False)   # includes full Notes
    failed_df.to_excel(xw, sheet_name="Failed", index=False)

    # Family split
    for fam, fam_df in long_df.groupby("family"):
        fam_df.sort_values(["series_id","date"]).to_excel(xw, sheet_name=fam[:31], index=False)

    # New: Prather-style outputs (ALL series)
    if not prather_point_all.empty:
        prather_point_all.to_excel(xw, sheet_name="Prather_Point_ALL", index=False)
    if not prather_fan_all.empty:
        prather_fan_all.to_excel(xw, sheet_name="Prather_Fan_ALL", index=False)
    if not prather_debug_all.empty:
        prather_debug_all.to_excel(xw, sheet_name="Prather_Debug", index=False)

# ------------------ TIMER END ------------------
elapsed = time.time() - t0
n_series_saved = long_df['series_id'].nunique() if not long_df.empty else 0
print(f"✅ Attempted {len(final_map)} unique series; saved {n_series_saved} series to {OUTPUT_XLSX}.")
if not failed_df.empty:
    print(f"⚠️ {len(failed_df)} series failed (see 'Failed' sheet).")
print(f"⏱️ Total runtime: {elapsed:.1f} seconds ({elapsed/60:.2f} minutes) | {datetime.timedelta(seconds=round(elapsed))}")
