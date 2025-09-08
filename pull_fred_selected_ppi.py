# pull_fred_selected_ppi.py
import os, time, re, random, warnings
import pandas as pd
import numpy as np
from fredapi import Fred
from dateutil.relativedelta import relativedelta
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import RidgeCV, LinearRegression
from sklearn.isotonic import IsotonicRegression
from statsmodels.tsa.statespace.sarimax import SARIMAX
from scipy.stats import pearsonr

warnings.filterwarnings("ignore")

# ---------------- CONFIG ----------------
START_DATE          = "2016-01-01"
BASE_YEAR           = 2019
OUTPUT_XLSX         = "fred_selected_ppi_2019base.xlsx"
FORECAST_HORIZON    = 12
TOP_K_EXOG          = 10
MAX_LAG_MONTHS      = 12
AR_P                = 6
CAL_WINDOW_MONTHS   = 18
MC_SIMS             = 200

# Env key
FRED_API_KEY = os.environ.get("FRED_API_KEY")
if not FRED_API_KEY:
    raise RuntimeError("FRED_API_KEY env var not set.")

# ---------------- SERIES ----------------
SERIES_IDS_BLOCK = """
# =========================
# CORE FREIGHT SERVICE PPIs
# =========================
PCU4841214841212     # Truckload (TL) line-haul only
PCU4841224841221     # LTL line-haul only
PCU482111482111412   # Rail Intermodal line-haul
PCU4931249312        # Warehousing & storage (general)

# =========================
# VAN / RETAIL / WHOLESALE
# =========================
PCU423423            # Wholesalers durables
PCU424424            # Wholesalers nondurables
PCUARETTRARETTR      # Retail aggregate
PCU452910452910      # Warehouse clubs
PCU445110445110      # Supermarkets
PCU444110444110      # Home centers (hardware, building materials; durables, TL-heavy)
PCU448140448140      # Family clothing stores (apparel, often IMDL-driven through imports)
PCU441110441110      # New car dealers (autos, big for TL, rail ramps)
PCU447110447110      # Gasoline stations (nondurables, discretionary retail)

# ==============
# PACKAGING PPIs
# ==============
PCU322211322211      # Corrugated boxes
PCU322212322212      # Folding paperboard
PCU326160326160      # Plastic bottles
WPU09150301          # Corrugated containers
WPU072A              # Plastic packaging
WPU066               # Plastics & resins

# ===================
# REEFER / COLD CHAIN
# ===================
PCU311311            # Food manufacturing
PCU3116131161        # Meat processing
PCU493120493120      # Refrigerated warehousing
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
        sid = ln.split()[0].upper()
        ids.append(sid)
    return list(dict.fromkeys(ids))

SERIES_IDS = clean_series_ids(SERIES_IDS_BLOCK)

# --------------- DATE HELPERS ---------------
def to_month_start_index(dt_series: pd.Series) -> pd.DatetimeIndex:
    """Normalize any datetime index/series to Month-Start (MS)."""
    # Convert to Period[M], then to timestamp at start of month
    return pd.to_datetime(dt_series).to_period("M").to_timestamp("MS")

def month_range(start_dt: pd.Timestamp, periods: int) -> pd.DatetimeIndex:
    return pd.date_range(start_dt, periods=periods, freq="MS")

# ---------------- ADAPTIVE PACER ----------------
class AdaptivePacer:
    def __init__(self, pause=0.5):
        self.pause = pause
        self.successes = 0
        self.calls = 0
    def sleep(self):
        time.sleep(self.pause + random.uniform(0,0.12))
    def on_success(self): self.successes += 1

def retry_call(func, *args, **kwargs):
    for attempt in range(6):
        try:
            return func(*args, **kwargs)
        except Exception:
            time.sleep(2.0 * (2**attempt) + random.uniform(0,1.0))
    raise

fred = Fred(api_key=FRED_API_KEY)

# ---------------- DATA PULL ----------------
meta_rows, records = [], []
for sid in SERIES_IDS:
    try:
        info = retry_call(fred.get_series_info, sid)
        meta_rows.append({"FRED_Code": sid, "Title": getattr(info, "title", sid)})

        s = retry_call(fred.get_series, sid, observation_start=START_DATE)
        if s is None or len(s) == 0:
            continue

        df = s.to_frame("value").reset_index().rename(columns={"index": "date"})
        # Normalize to Month-Start
        df["date"] = to_month_start_index(df["date"])

        # 2019 base index
        base = df.loc[df["date"].dt.year == BASE_YEAR, "value"].mean()
        if pd.notna(base) and base != 0:
            df["index_2019=100"] = (df["value"] / base) * 100.0
        else:
            df["index_2019=100"] = np.nan

        df["series_id"] = sid
        records.append(df)
    except Exception:
        # Skip series that fail (rare)
        continue

meta_df = pd.DataFrame(meta_rows)
if not records:
    raise RuntimeError("No series pulled from FRED.")

long_df = (
    pd.concat(records, ignore_index=True)
    .sort_values(["series_id", "date"])
)

wide_idx = (
    long_df.pivot(index="date", columns="series_id", values="index_2019=100")
    .sort_index()
)

# ---------------- FEATURES / UTILS ----------------
def best_lag_table(y: pd.Series, X: pd.DataFrame, max_lag: int = 12) -> pd.DataFrame:
    rows = []
    for col in X.columns:
        best = None
        # include lag 0..max_lag
        for lag in range(max_lag + 1):
            xs = X[col].shift(lag)
            dfj = pd.concat([y, xs], axis=1).dropna()
            if dfj.empty:
                continue
            r, _ = pearsonr(dfj.iloc[:, 0], dfj.iloc[:, 1])
            if best is None or abs(r) > abs(best["pearson"]):
                best = {"feature": col, "best_lag": lag, "pearson": r}
        if best:
            rows.append(best)
    if not rows:
        return pd.DataFrame(columns=["feature", "best_lag", "pearson"])
    return pd.DataFrame(rows).sort_values(by="pearson", key=lambda s: s.abs(), ascending=False)

def build_exog_matrix(top_exog: pd.DataFrame, X_all: pd.DataFrame, idx: pd.DatetimeIndex) -> pd.DataFrame:
    Xmat = pd.DataFrame(index=idx)
    for _, r in top_exog.iterrows():
        feat = r["feature"]
        lag  = int(r["best_lag"])
        if feat in X_all.columns:
            Xmat[feat] = X_all[feat].shift(lag)
    return Xmat

def add_ar_terms(X: pd.DataFrame, y: pd.Series, p: int = 6) -> pd.DataFrame:
    out = X.copy()
    for L in range(1, p + 1):
        out[f"y_lag{L}"] = y.shift(L)
    return out

def extend_exog_yoy(X_lagged: pd.DataFrame, last_obs: pd.Timestamp, horizon: int) -> pd.DataFrame:
    """
    Ensure exogenous (already lagged) has future rows.
    Fill each future month by copying the same month from the prior year (YoY repeat).
    Fallback to forward/back fill if last year's month is not available.
    """
    if X_lagged.empty:
        return X_lagged

    # All indices should be MS
    X = X_lagged.copy().sort_index()
    future_idx = pd.date_range(last_obs + relativedelta(months=1), periods=horizon, freq="MS")

    # reindex to include future months
    X = X.reindex(X.index.union(future_idx)).sort_index()

    for dt in future_idx:
        src = dt - relativedelta(years=1)
        if src in X.index:
            X.loc[dt, X.columns] = X.loc[src, X.columns]
        # else leave NaNs; we'll ffill/bfill below

    X = X.ffill().bfill()
    return X

def ridge_iterative_forecast(last_date: pd.Timestamp,
                             horizon: int,
                             model: Pipeline,
                             X_exog_lagged_full: pd.DataFrame,
                             y_hist: pd.Series,
                             p: int = 6) -> pd.Series:
    """
    Iteratively forecast with Ridge + AR terms.
    Assumes X_exog_lagged_full already contains the columns the model saw during training (except AR terms),
    and is extended to include future months (via extend_exog_yoy).
    """
    preds = []
    y_tmp = y_hist.copy()
    # Build future index (MS)
    future_idx = pd.date_range(last_date + relativedelta(months=1), periods=horizon, freq="MS")

    # Ensure exog contains the future months (already YoY-extended by caller)
    X_filled = X_exog_lagged_full.copy()

    for cur in future_idx:
        # Start with exogenous features for this month
        row = {c: X_filled.loc[cur, c] for c in X_filled.columns}

        # Add AR lags from y_tmp (already has actual+forecast so far)
        for L in range(1, p + 1):
            lag_dt = cur - relativedelta(months=L)
            if lag_dt in y_tmp.index:
                row[f"y_lag{L}"] = y_tmp.loc[lag_dt]
            else:
                row[f"y_lag{L}"] = y_tmp.iloc[-1]

        xrow = pd.DataFrame([row], index=[cur])
        # Predict and append
        yhat = float(model.predict(xrow)[0])
        preds.append((cur, yhat))
        y_tmp.loc[cur] = yhat

    return pd.Series([v for _, v in preds], index=[d for d, _ in preds], name="ridge_forecast")

# ---------------- FORECAST PIPELINE ----------------
NORTH_STARS = [
    ("PCU4841214841212",  "TL"),
    ("PCU4841224841221",  "LTL"),
    ("PCU482111482111412","IMDL"),
]

results = {}

# Normalize wide_idx to MS (safety if upstream changes)
wide_idx.index = wide_idx.index.to_period("M").to_timestamp("MS")
wide_idx = wide_idx.sort_index()

for TARGET, TSHORT in NORTH_STARS:
    if TARGET not in wide_idx.columns:
        continue

    # Target and exog
    y = wide_idx[TARGET].dropna()
    y.index = y.index.to_period("M").to_timestamp("MS")
    X_all = wide_idx.drop(columns=[TARGET]).copy()

    # Top exogenous by lagged Pearson
    lag_tbl  = best_lag_table(y, X_all, MAX_LAG_MONTHS)
    top_exog = lag_tbl.head(TOP_K_EXOG)[["feature", "best_lag"]].reset_index(drop=True)

    # Build lagged exogenous on the full index we have today
    X_exog_lagged = build_exog_matrix(top_exog, X_all, X_all.index)

    # Training alignment (drop rows with any NaNs after lags)
    df_train = pd.concat([y.rename("y"), X_exog_lagged], axis=1).dropna()
    if df_train.empty or df_train.shape[0] < (AR_P + 24):
        # Skip if too short to fit
        continue

    y_train = df_train["y"]
    X_train_exog = df_train.drop(columns=["y"])

    # Add AR terms to training design
    XA = add_ar_terms(X_train_exog, y_train, AR_P)
    dfA = pd.concat([y_train.rename("y"), XA], axis=1).dropna()
    yA, XA = dfA["y"], dfA.drop(columns=["y"])

    # Ridge model
    ridge = Pipeline([
        ("scaler", StandardScaler()),
        ("ridge",  RidgeCV(alphas=np.logspace(-4, 3, 40)))
    ])
    ridge.fit(XA, yA)
    ridge_fit = pd.Series(ridge.predict(XA), index=XA.index, name="ridge_fit")

    # Build YoY-extended exog for the forecast period (EXOG without AR terms)
    last_date = y.index[-1]
    X_exog_yoy = extend_exog_yoy(X_exog_lagged, last_date, FORECAST_HORIZON)

    # Ridge forecast (iterative with AR terms added inside the loop)
    ridge_fcst = ridge_iterative_forecast(last_date, FORECAST_HORIZON, ridge, X_exog_yoy, y, AR_P)

    # SARIMAX (uses exog without AR terms)
    try:
        sarimax = SARIMAX(
            y_train,
            exog=X_train_exog.loc[y_train.index],
            order=(2, 0, 1),
            trend="c",
            enforce_stationarity=False,
            enforce_invertibility=False
        ).fit(disp=False)

        sarimax_fit = sarimax.get_prediction(
            start=y_train.index[0],
            end=y_train.index[-1],
            exog=X_train_exog.loc[y_train.index],
            dynamic=False
        ).predicted_mean

        # Future exog for SARIMAX (YoY-extended)
        X_future_exog = X_exog_yoy.loc[ridge_fcst.index]
        sarimax_fcst  = sarimax.get_forecast(steps=len(X_future_exog), exog=X_future_exog).predicted_mean
    except Exception:
        # Fallback: if SARIMAX fails, use ridge only
        sarimax_fit = ridge_fit.reindex_like(ridge_fit)
        sarimax_fcst = ridge_fcst.copy()

    # --- Stacking & calibration ---
    common_idx = ridge_fit.index.intersection(sarimax_fit.index).intersection(y.index)
    stack_df = pd.DataFrame({
        "actual": y.loc[common_idx],
        "ridge":  ridge_fit.loc[common_idx],
        "sarimax": sarimax_fit.loc[common_idx],
    }).dropna()

    # If too short, skip stacking (rare)
    if stack_df.shape[0] < max(12, CAL_WINDOW_MONTHS):
        stack_future_in = pd.DataFrame({"ridge": ridge_fcst, "sarimax": sarimax_fcst}, index=ridge_fcst.index)
        stack_fcst = pd.Series(ridge_fcst, index=ridge_fcst.index) * 0.5 + pd.Series(sarimax_fcst, index=ridge_fcst.index) * 0.5
        stack_fit_cal = pd.Series((ridge_fit + sarimax_fit) / 2.0).reindex(y.index).dropna()
    else:
        tail_idx = stack_df.tail(CAL_WINDOW_MONTHS).index
        stack_lin = LinearRegression().fit(stack_df.loc[tail_idx, ["ridge", "sarimax"]],
                                           stack_df.loc[tail_idx, "actual"])
        stack_fit = pd.Series(stack_lin.predict(stack_df[["ridge", "sarimax"]]), index=stack_df.index)

        iso = IsotonicRegression(out_of_bounds="clip").fit(
            stack_fit.loc[tail_idx].values,
            stack_df.loc[tail_idx, "actual"].values
        )
        stack_fit_cal = stack_fit.copy()
        stack_fit_cal.loc[tail_idx] = iso.transform(stack_fit.loc[tail_idx].values)

        stack_future_in = pd.DataFrame({"ridge": ridge_fcst, "sarimax": sarimax_fcst}, index=ridge_fcst.index)
        stack_fcst = pd.Series(stack_lin.predict(stack_future_in), index=stack_future_in.index)
        stack_fcst = stack_fcst.astype(float)
        stack_fcst = pd.Series(iso.transform(stack_fcst.values), index=stack_fcst.index)

    # --- Monte Carlo (path variety from model uncertainty; exog path stays YoY) ---
    sim_paths = []
    for s in range(MC_SIMS):
        # Use same YoY exog; randomness via ridge residual-like noise on stacked fcst
        # (You can replace with bootstrapped residuals if desired.)
        noise = np.random.default_rng(42 + s).normal(loc=0.0, scale=stack_df["actual"].sub(stack_fit_cal.reindex(stack_df.index)).std(ddof=1) or 0.0, size=len(stack_fcst))
        sim_paths.append((stack_fcst.values + noise).astype(float))

    if sim_paths:
        sim_df = pd.DataFrame(sim_paths, index=[f"sim_{i}" for i in range(MC_SIMS)]).T
        q_df = sim_df.quantile([0.05, 0.10, 0.50, 0.90, 0.95], axis=1).T
        q_df.columns = ["p05", "p10", "p50", "p90", "p95"]
        q_df.index = stack_fcst.index
    else:
        q_df = pd.DataFrame(index=stack_fcst.index, columns=["p05", "p10", "p50", "p90", "p95"], dtype=float)

    # Assemble output
    hist = pd.DataFrame({"actual": y})
    forecast_table = pd.concat([hist, stack_fit_cal.rename("fitted"), q_df], axis=1)

    # Backtest metrics (last 18m of calibrated in-sample fit)
    test_win = min(CAL_WINDOW_MONTHS, len(stack_fit_cal))
    bt_idx = stack_fit_cal.dropna().tail(test_win).index
    bt_pred = stack_fit_cal.loc[bt_idx]
    bt_act  = y.loc[bt_idx]
    if len(bt_idx) >= 3 and not bt_pred.isna().any() and not bt_act.isna().any():
        r = float(np.corrcoef(bt_pred, bt_act)[0,1])
        r2 = float(r * r)
    else:
        r = np.nan
        r2 = np.nan

    leaderboard = pd.DataFrame({"target": [TARGET], "R2_last18": [r2], "r_last18": [r]})
    leaderboard.to_csv(f"backtest_{TSHORT.lower()}.csv", index=False)

    # Store
    results[TSHORT] = {"forecast_table": forecast_table, "leaderboard": leaderboard}

    # Print
    print(f"\n=== {TSHORT} ===")
    print(leaderboard)

# ---------------- WRITE EXCEL ----------------
with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as xw:
    long_df.to_excel(xw, sheet_name="Series_Long", index=False)
    wide_idx.to_excel(xw, sheet_name="Wide_Index2019")
    meta_df.to_excel(xw, sheet_name="Metadata", index=False)
    for TSHORT, pack in results.items():
        pack["forecast_table"].reset_index().rename(columns={"index": "date"}).to_excel(
            xw, sheet_name=f"Forecast_{TSHORT}", index=False
        )
        pack["leaderboard"].to_excel(xw, sheet_name=f"Leaderboard_{TSHORT}", index=False)

print(f"\n✅ Saved {OUTPUT_XLSX} with forecasts + backtests")
