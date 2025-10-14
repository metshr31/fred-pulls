# pull_fred_series_bulk_split_pivot_adaptive_test.py
# Minimal, fast smoke test that:
# 1) Pulls a *small* set of FRED series
# 2) Normalizes to 2019=100
# 3) Writes tidy + wide sheets
# 4) Adds Prather-style forecasts (point + MC fan) for each series

import os, time, datetime
import numpy as np
import pandas as pd
from fredapi import Fred

# --- NEW: lightweight forecasting bits ---
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.holtwinters import ExponentialSmoothing

# ------------------ CONFIG ------------------
START_DATE   = "2016-01-01"
BASE_YEAR    = 2019
OUTPUT_XLSX  = "fred_series_2019base.xlsx"

# Keep test small & fast
TEST_SERIES = {
    "FRGSHPUSM649NCIS": "Cass Freight Shipments (NSA via FRED)",
    "FRGEXPUSM649NCIS": "Cass Freight Expenditures (NSA via FRED)",
    "TRUCKD11":         "ATA Truck Tonnage (SA)",
}

# Forecast settings
FORECAST_HORIZON = int(os.environ.get("H", "12"))     # months
BOOT_B = int(os.environ.get("MC_SIMS", "600"))        # MC runs (kept small for speed)
BOOT_BLOCK = int(os.environ.get("MC_BLOCK", "6"))     # block length (months)
MAX_TRAIN = int(os.environ.get("MAX_TRAIN", "120"))   # cap training window to last N months for speed

# ------------------ FRED API KEY (ENV ONLY) ------------------
FRED_API_KEY = os.environ.get("FRED_API_KEY")
if not FRED_API_KEY:
    raise RuntimeError("FRED_API_KEY env var not set (define it in GitHub Secrets or your shell).")

fred = Fred(api_key=FRED_API_KEY)

t0 = time.time()

# ------------------ helpers ------------------
def _to_month_start(s: pd.Series) -> pd.Series:
    if s is None or len(s) == 0:
        return pd.Series(dtype=float)
    s = pd.Series(s.values, index=pd.to_datetime(s.index))
    s.index = s.index.to_period("M").to_timestamp(how="start")
    return s.sort_index()

def _normalize_2019(s: pd.Series) -> pd.Series:
    if s.empty:
        return s
    base_vals = s[s.index.year == BASE_YEAR].dropna()
    if not base_vals.empty:
        base = base_vals.mean()
    else:
        w = s.dropna().iloc[:12]
        base = w.mean() if not w.empty else 1.0
    if not np.isfinite(base) or base == 0:
        base = 1.0
    return (s / base) * 100.0

def _fit_prather_point(series: pd.Series, H: int) -> tuple[pd.Series, pd.Series, str]:
    """
    Return (point_forecast, fitted_in_sample, method) using quick fallback chain:
    ARIMA(1,1,1)(0,1,1,12) -> ETS(A,A,A,12) -> seasonal naive -> carry-forward
    """
    y = series.dropna().astype(float).copy()
    if len(y) == 0:
        raise ValueError("empty series")

    # cap train length for speed
    if len(y) > MAX_TRAIN:
        y = y.iloc[-MAX_TRAIN:].copy()

    # Horizon index
    idx = pd.date_range(y.index[-1] + pd.offsets.MonthBegin(1), periods=H, freq="MS")

    # Try ARIMA
    try:
        mdl = ARIMA(y, order=(1, 1, 1), seasonal_order=(0, 1, 1, 12))
        res = mdl.fit(method_kwargs={"warn_convergence": False})
        fc = pd.Series(res.forecast(H).values, index=idx, name="point")
        fitted = pd.Series(res.fittedvalues, index=y.index, name="fitted")
        return fc, fitted, "ARIMA(1,1,1)(0,1,1,12)"
    except Exception:
        pass

    # Try ETS (Holt–Winters additive)
    try:
        mdl = ExponentialSmoothing(y, trend="add", seasonal="add", seasonal_periods=12)
        res = mdl.fit(optimized=True)
        fc = pd.Series(res.forecast(H).values, index=idx, name="point")
        fitted = pd.Series(res.fittedvalues, index=y.index, name="fitted")
        return fc, fitted, "ETS(A,A,A,12)"
    except Exception:
        pass

    # Seasonal naive if >= 24
    try:
        if len(y) >= 24:
            vals = [y.iloc[-12 + (i % 12)] for i in range(H)]
            fc = pd.Series(vals, index=idx, name="point")
            fitted = y.copy()
            return fc, fitted, "Seasonal-Naive"
    except Exception:
        pass

    # Carry-forward
    fc = pd.Series([y.iloc[-1]] * H, index=idx, name="point")
    fitted = y.copy()
    return fc, fitted, "Carry-Forward"

def _block_bootstrap_fan(point_fc: pd.Series, resid: np.ndarray, B: int, block_len: int) -> pd.DataFrame:
    """Simple block-bootstrap fan to preserve short-run autocorr in residuals."""
    H = len(point_fc)
    if resid is None or len(resid) < 3 or not np.isfinite(resid).any():
        # fall back to white noise with small std
        noise = np.random.normal(0, 0.5, size=(B, H))
        paths = noise + point_fc.values
    else:
        rng = np.random.default_rng(42)
        paths = np.empty((B, H))
        resid = np.asarray(resid, dtype=float)
        block_len = max(3, int(block_len))
        for b in range(B):
            seq = []
            while len(seq) < H:
                start = rng.integers(0, max(1, len(resid) - block_len + 1))
                seq.extend(resid[start:start + block_len])
            seq = np.array(seq[:H])
            paths[b, :] = point_fc.values + seq

    fan = pd.DataFrame(paths.T, index=point_fc.index)
    q = fan.quantile([0.05, 0.10, 0.50, 0.90, 0.95], axis=1).T
    q.columns = ["p05", "p10", "p50", "p90", "p95"]
    return q

# ------------------ PULL + NORMALIZE ------------------
records = []
for sid, label in TEST_SERIES.items():
    s = fred.get_series(sid, observation_start=START_DATE)
    s = _to_month_start(s)
    if s.empty:
        continue
    df = s.to_frame("value")
    df["index_2019=100"] = _normalize_2019(df["value"])
    df["series_id"] = sid
    df["series_label"] = label
    records.append(df.reset_index().rename(columns={"index": "date"}))

if not records:
    raise RuntimeError("No series downloaded for the test set.")

long_df = pd.concat(records, ignore_index=True).sort_values(["series_id", "date"])
wide_idx = long_df.pivot_table(index="date", columns="series_id", values="index_2019=100", aggfunc="last").sort_index()

# ------------------ PRATHER FORECASTS (per series) ------------------
fc_rows = []            # long table of point + fan
point_wide = {}         # wide table of points
fan_wides = {k: {} for k in ["p05","p10","p50","p90","p95"]}

for sid in wide_idx.columns:
    series = wide_idx[sid].dropna()
    if series.empty:
        continue

    # point + fitted + method
    point_fc, fitted, method = _fit_prather_point(series, FORECAST_HORIZON)

    # residuals (in-sample)
    try:
        # align fitted to actual
        common = series.reindex(fitted.index).dropna()
        resid = (common - fitted.reindex(common.index)).dropna().values
        if len(resid) < 3:
            resid = (series.diff().dropna().values)  # fallback
    except Exception:
        resid = (series.diff().dropna().values)

    # fan
    fan = _block_bootstrap_fan(point_fc, resid, B=BOOT_B, block_len=BOOT_BLOCK)

    # pack long rows
    tmp = pd.concat([point_fc.rename("point"), fan], axis=1).reset_index().rename(columns={"index":"date"})
    tmp["series_id"] = sid
    tmp["method"] = method
    fc_rows.append(tmp)

    # wide conveniences
    point_wide[sid] = point_fc
    for q in fan.columns:
        fan_wides[q][sid] = fan[q]

# Assemble forecast tables
prather_long = pd.concat(fc_rows, ignore_index=True).sort_values(["series_id","date"])

prather_point_wide = pd.DataFrame(point_wide).sort_index()
prather_fan_wides = {q: pd.DataFrame(cols).sort_index() for q, cols in fan_wides.items()}

# ------------------ WRITE EXCEL ------------------
with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as xw:
    # originals
    long_df.to_excel(xw, sheet_name="Series_Long", index=False)
    wide_idx.to_excel(xw, sheet_name="Wide_Index2019")

    # forecasts (long + wide)
    prather_long.to_excel(xw, sheet_name="Prather_Forecast_Long", index=False)

    prather_point_wide.to_excel(xw, sheet_name="Prather_Point_ALL")
    # put fan quantiles on separate sheets for quick snapshots
    for q, dfq in prather_fan_wides.items():
        dfq.to_excel(xw, sheet_name=f"Prather_Fan_ALL_{q.upper()}")

print(f"✅ Test Excel written: {OUTPUT_XLSX}")
elapsed = time.time() - t0
print(f"⏱️ Elapsed: {elapsed:.1f}s")
