# pull_fred_selected_ppi.py
import os, time, random, warnings
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
MC_SIMS             = 5000

# Env key
FRED_API_KEY = os.environ.get("FRED_API_KEY")
if not FRED_API_KEY:
    raise RuntimeError("FRED_API_KEY env var not set.")

# ---------------- SERIES ----------------
# (Updated IDs inline per your instructions)
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
PCU455210455210A     # Warehouse clubs & supercenters (current)
PCU445110445110      # Supermarkets & grocery stores
PCU444100444100      # Building materials & supplies dealers (home centers proxy)
PCU448448            # Clothing & accessories retailers (family clothing replacement)
PCU441110441110      # New car dealers
PCU447110447110      # Gasoline stations

# ==============
# PACKAGING PPIs
# ==============
PCU322211322211      # Corrugated boxes
PCU322212322212      # Folding paperboard
PCU326160326160      # Plastic bottles
WPU09150301          # Corrugated containers (commodity)
WPU072A              # Plastic packaging (commodity)
WPU066               # Plastics & resins (commodity)

# ===================
# REEFER / COLD CHAIN
# ===================
PCU311311            # Food manufacturing (aggregate)
PCU3116131161        # Meat processing
PCU493120493120      # Refrigerated warehousing
PCU31153115          # Dairy product manufacturing (updated)
PCU311421311421      # Fruit & vegetable preserving (updated)
PCU31193119          # Other food manufacturing (updated)

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

# ---------- Fix B: ID replacements / aliases ----------
ID_FIXUPS = {
    # Retail/wholesale
    "PCU452910452910": "PCU455210455210A",
    "PCU448140448140": "PCU448448",
    "PCU444110444110": "PCU444100444100",

    # Food aggregates
    "PCU3115": "PCU31153115",
    "PCU3114": "PCU311421311421",
    "PCU3119": "PCU31193119",
}
def apply_id_fixups(series_ids):
    fixed = []
    for sid in series_ids:
        new_sid = ID_FIXUPS.get(sid, sid)
        if new_sid != sid:
            print(f"[ID FIXUP] {sid} -> {new_sid}")
        fixed.append(new_sid)
    return fixed
SERIES_IDS = apply_id_fixups(SERIES_IDS)

# --------------- DATE HELPERS ---------------
def to_month_start_index(dt_like):
    """
    Normalize to month-start timestamps safely (no 'MS' as a Period freq).
    Works for Series, DatetimeIndex, array-like, or scalar.
    """
    dt = pd.to_datetime(dt_like)
    if isinstance(dt, pd.Series):
        return dt.dt.to_period("M").dt.start_time
    if isinstance(dt, pd.DatetimeIndex):
        return dt.to_period("M").to_timestamp(how="start")
    idx = pd.to_datetime(pd.Index(dt))
    return idx.to_period("M").to_timestamp(how="start")

# --------------- RETRY (surface real error) ---------------
def retry_call(func, *args, **kwargs):
    last = None
    for attempt in range(6):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last = e
            time.sleep(2.0 * (2**attempt) + random.uniform(0, 1.0))
    raise last

# ---------------- PRE-FLIGHT (auth/connectivity) ----------------
fred = Fred(api_key=FRED_API_KEY)
try:
    _probe = fred.get_series("CPIAUCSL", observation_start="2019-01-01")
    assert _probe is not None and len(_probe) > 0
except Exception as e:
    raise RuntimeError(f"FRED probe failed: {e}. Check FRED_API_KEY & network.") from e

# ---------------- DATA PULL (robust, month-start index, failure summary) ----------------
records, latest_rows, failed, meta_rows = [], [], [], []
for i, sid in enumerate(SERIES_IDS, start=1):
    try:
        # (optional) series title
        try:
            info = retry_call(fred.get_series_info, sid)
            meta_rows.append({"FRED_Code": sid, "Title": getattr(info, "title", sid)})
        except Exception as me:
            meta_rows.append({"FRED_Code": sid, "Title": sid})
            print(f"[META WARN] {sid}: {type(me).__name__}: {me}")

        s = retry_call(fred.get_series, sid, observation_start=START_DATE)
        if s is None or len(s) == 0:
            failed.append({"FRED_Code": sid, "Reason": "Empty or None from FRED"})
            continue

        df = s.to_frame("value").reset_index().rename(columns={"index": "date"})
        df["date"] = to_month_start_index(df["date"])

        base = df.loc[df["date"].dt.year == BASE_YEAR, "value"].mean()
        df["index_2019=100"] = (df["value"] / base) * 100.0 if pd.notna(base) and base != 0 else pd.NA
        df["series_id"] = sid

        title = next((m["Title"] for m in meta_rows if m["FRED_Code"] == sid), sid)
        df["series_label"] = title

        records.append(df)
        latest_rows.append({"FRED_Code": sid, "Latest Available": df["date"].max()})

        if i % 10 == 0:
            print(f"...pulled {i}/{len(SERIES_IDS)} series")

    except Exception as e:
        failed.append({"FRED_Code": sid, "Reason": f"{type(e).__name__}: {e}"})
    finally:
        time.sleep(0.15)

meta_df   = pd.DataFrame(meta_rows) if meta_rows else pd.DataFrame(columns=["FRED_Code","Title"])
long_df   = (pd.concat(records, ignore_index=True).sort_values(["series_id","date"])
             if records else pd.DataFrame(columns=["date","value","index_2019=100","series_id","series_label"]))
latest_df = (
    pd.DataFrame(latest_rows).sort_values("Latest Available", ascending=False)
    if latest_rows else
    pd.DataFrame(columns=["FRED_Code", "Latest Available"])
)
failed_df = pd.DataFrame(failed).sort_values("FRED_Code") if failed else pd.DataFrame(columns=["FRED_Code","Reason"])

wide_idx = long_df.pivot_table(index="date", columns="series_id", values="index_2019=100", aggfunc="last").sort_index()

if long_df.empty:
    print("\n[ERROR] No series pulled. Summary of failures:")
    if not failed_df.empty:
        print(failed_df.head(20).to_string(index=False))
    raise RuntimeError("No series pulled from FRED. See failure summary above.")

print(f"\n[OK] Pulled {len(records)} / {len(SERIES_IDS)} series.")
if not failed_df.empty:
    print(f"[WARN] {len(failed_df)} series failed. Top examples:")
    print(failed_df.head(10).to_string(index=False))

# ---------------- FEATURE / EXPLAIN HELPERS ----------------
def best_lag_table(y: pd.Series, X: pd.DataFrame, max_lag: int = 12) -> pd.DataFrame:
    rows = []
    for col in X.columns:
        best = None
        for lag in range(max_lag + 1):
            xs = X[col].shift(lag)
            dfj = pd.concat([y, xs], axis=1).dropna()
            if dfj.empty:
                continue
            try:
                r, _ = pearsonr(dfj.iloc[:, 0], dfj.iloc[:, 1])
            except Exception:
                continue
            if not np.isfinite(r):
                continue
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
    if X_lagged.empty:
        return X_lagged
    X = X_lagged.copy().sort_index()
    future_idx = pd.date_range(last_obs + relativedelta(months=1), periods=horizon, freq="MS")
    X = X.reindex(X.index.union(future_idx)).sort_index()
    for dt in future_idx:
        src = dt - relativedelta(years=1)
        if src in X.index:
            X.loc[dt, X.columns] = X.loc[src, X.columns]
    X = X.ffill().bfill()
    return X

def ridge_iterative_forecast(last_date: pd.Timestamp,
                             horizon: int,
                             model: Pipeline,
                             X_exog_lagged_full: pd.DataFrame,
                             y_hist: pd.Series,
                             p: int = 6,
                             return_rows: bool = False):
    preds = []
    rows  = []
    y_tmp = y_hist.copy()
    future_idx = pd.date_range(last_date + relativedelta(months=1), periods=horizon, freq="MS")
    X_filled = X_exog_lagged_full.copy()
    for cur in future_idx:
        row = {c: X_filled.loc[cur, c] for c in X_filled.columns}
        for L in range(1, p + 1):
            lag_dt = cur - relativedelta(months=L)
            row[f"y_lag{L}"] = y_tmp.loc[lag_dt] if lag_dt in y_tmp.index else y_tmp.iloc[-1]
        xrow = pd.DataFrame([row], index=[cur])
        yhat = float(model.predict(xrow)[0])
        preds.append((cur, yhat))
        y_tmp.loc[cur] = yhat
        if return_rows:
            rows.append(pd.Series(row, name=cur))
    ser = pd.Series([v for _, v in preds], index=[d for d, _ in preds], name="ridge_forecast")
    if return_rows:
        return ser, pd.DataFrame(rows)
    return ser

def ridge_contributions(pipeline: Pipeline, X_rows: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """
    Return (contrib_df, pred) where contrib_df has per-feature contributions per row,
    using the pipeline's StandardScaler + Ridge coef: contribution = coef_j * (x - mean)/scale.
    """
    scaler = pipeline.named_steps["scaler"]
    model  = pipeline.named_steps["ridge"]
    means  = getattr(scaler, "mean_", None)
    scales = getattr(scaler, "scale_", None)
    coefs  = model.coef_
    intercept = model.intercept_

    # guard zero/None scales
    if scales is None:
        scales = np.ones_like(coefs)
    scales = np.where(scales == 0, 1.0, scales)

    # align and transform
    X_arr = X_rows.values
    X_scaled = (X_arr - means) / scales
    contrib = X_scaled * coefs  # broadcast
    contrib_df = pd.DataFrame(contrib, index=X_rows.index, columns=X_rows.columns)
    pred = contrib_df.sum(axis=1) + intercept
    return contrib_df, pred

# ---------------- FORECAST PIPELINE ----------------
NORTH_STARS = [
    ("PCU4841214841212",  "TL"),
    ("PCU4841224841221",  "LTL"),
    ("PCU482111482111412","IMDL"),
]

results = {}

wide_idx.index = to_month_start_index(wide_idx.index)
wide_idx = wide_idx.sort_index()

for TARGET, TSHORT in NORTH_STARS:
    if TARGET not in wide_idx.columns:
        continue

    y = wide_idx[TARGET].dropna()
    y.index = to_month_start_index(y.index)
    X_all = wide_idx.drop(columns=[TARGET]).copy()

    # --- correlations & lags ---
    lag_tbl  = best_lag_table(y, X_all, MAX_LAG_MONTHS)
    top_exog = lag_tbl.head(TOP_K_EXOG)[["feature", "best_lag"]].reset_index(drop=True)

    # --- design matrices ---
    X_exog_lagged = build_exog_matrix(top_exog, X_all, X_all.index)
    df_train = pd.concat([y.rename("y"), X_exog_lagged], axis=1).dropna()
    if df_train.empty or df_train.shape[0] < (AR_P + 24):
        continue

    y_train = df_train["y"]
    X_train_exog = df_train.drop(columns=["y"])

    XA = add_ar_terms(X_train_exog, y_train, AR_P)
    dfA = pd.concat([y_train.rename("y"), XA], axis=1).dropna()
    yA, XA = dfA["y"], dfA.drop(columns=["y"])

    # --- Ridge ---
    ridge = Pipeline([
        ("scaler", StandardScaler()),
        ("ridge",  RidgeCV(alphas=np.logspace(-4, 3, 40)))
    ])
    ridge.fit(XA, yA)
    ridge_fit = pd.Series(ridge.predict(XA), index=XA.index, name="ridge_fit")

    last_date   = y.index[-1]
    X_exog_yoy  = extend_exog_yoy(X_exog_lagged, last_date, FORECAST_HORIZON)
    ridge_fcst, ridge_rows_future = ridge_iterative_forecast(
        last_date, FORECAST_HORIZON, ridge, X_exog_yoy, y, AR_P, return_rows=True
    )

    # make sure contribution rows match training feature order
    ridge_rows_future = ridge_rows_future.reindex(columns=XA.columns)

    # --- SARIMAX ---
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

        X_future_exog = X_exog_yoy.loc[ridge_fcst.index]
        sarimax_fcst  = sarimax.get_forecast(steps=len(X_future_exog), exog=X_future_exog).predicted_mean
    except Exception:
        sarimax_fit  = ridge_fit.reindex_like(ridge_fit)
        sarimax_fcst = ridge_fcst.copy()

    # --- Stacking & calibration ---
    common_idx = ridge_fit.index.intersection(sarimax_fit.index).intersection(y.index)
    stack_df = pd.DataFrame({
        "actual": y.loc[common_idx],
        "ridge":  ridge_fit.loc[common_idx],
        "sarimax": sarimax_fit.loc[common_idx],
    }).dropna()

    if stack_df.shape[0] < max(12, CAL_WINDOW_MONTHS):
        stack_fit_cal = ((ridge_fit + sarimax_fit) / 2.0).reindex(y.index).dropna()
        stack_fcst = (ridge_fcst * 0.5 + sarimax_fcst * 0.5).astype(float)
        stack_weights = np.array([0.5, 0.5])
    else:
        tail_idx = stack_df.tail(CAL_WINDOW_MONTHS).index
        stack_lin = LinearRegression().fit(stack_df.loc[tail_idx, ["ridge", "sarimax"]],
                                           stack_df.loc[tail_idx, "actual"])
        stack_fit = pd.Series(stack_lin.predict(stack_df[["ridge","sarimax"]]), index=stack_df.index)
        iso = IsotonicRegression(out_of_bounds="clip").fit(
            stack_fit.loc[tail_idx].values, stack_df.loc[tail_idx,"actual"].values
        )
        stack_fit_cal = stack_fit.copy()
        stack_fit_cal.loc[tail_idx] = iso.transform(stack_fit.loc[tail_idx].values)

        stack_future_in = pd.DataFrame({"ridge": ridge_fcst, "sarimax": sarimax_fcst}, index=ridge_fcst.index)
        stack_fcst = pd.Series(stack_lin.predict(stack_future_in), index=stack_future_in.index).astype(float)
        stack_fcst = pd.Series(iso.transform(stack_fcst.values), index=stack_fcst.index)
        stack_weights = np.array([stack_lin.coef_[0], stack_lin.coef_[1]])

    # --- Ridge contributions (what drives the forecast) ---
    contrib_df, pred_chk = ridge_contributions(ridge, ridge_rows_future)

    # summary tables
    coef_s = pd.Series(ridge.named_steps["ridge"].coef_, index=XA.columns, name="ridge_coef")
    coef_s_abs_rank = coef_s.abs().rank(ascending=False, method="dense").astype(int)

    mean_abs_contrib = contrib_df.abs().mean().rename("mean_abs_contrib")
    next_contrib     = contrib_df.iloc[0].rename("t+1_contrib")

    # merge with correlation table for EXOG features
    meta_rows = []
    for col in XA.columns:
        is_ar = col.startswith("y_lag")
        rrow = lag_tbl[lag_tbl["feature"] == col].head(1) if not is_ar else pd.DataFrame()
        meta_rows.append({
            "feature": col,
            "type": "AR" if is_ar else "EXOG",
            "best_lag": (int(rrow["best_lag"].iloc[0]) if not rrow.empty else np.nan),
            "pearson": (float(rrow["pearson"].iloc[0]) if not rrow.empty else np.nan),
            "ridge_coef": coef_s.get(col, np.nan),
            "abs_coef_rank": coef_s_abs_rank.get(col, np.nan),
            "mean_abs_contrib": mean_abs_contrib.get(col, np.nan),
            "t+1_contrib": next_contrib.get(col, np.nan),
        })
    explain_df = pd.DataFrame(meta_rows).sort_values("mean_abs_contrib", ascending=False)

    # console summaries
    top_corr = lag_tbl.head(8)[["feature","best_lag","pearson"]]
    top_contrib = explain_df.query("type=='EXOG'").head(8)[["feature","best_lag","pearson","mean_abs_contrib","t+1_contrib"]]

    print(f"\n--- {TSHORT}: strongest correlations (abs r) ---")
    print(top_corr.to_string(index=False))
    print(f"\n--- {TSHORT}: top EXOG contributors (Ridge forecast) ---")
    print(top_contrib.to_string(index=False))

    # --- Monte Carlo bands (same as before) ---
    sim_paths = []
    resid_std = stack_df["actual"].sub(stack_fit_cal.reindex(stack_df.index)).std(ddof=1) if not stack_df.empty else 0.0
    for s in range(MC_SIMS):
        noise = np.random.default_rng(42 + s).normal(loc=0.0, scale=resid_std or 0.0, size=len(stack_fcst))
        sim_paths.append((stack_fcst.values + noise).astype(float))
    if sim_paths:
        sim_df = pd.DataFrame(sim_paths, index=[f"sim_{i}" for i in range(MC_SIMS)]).T
        q_df = sim_df.quantile([0.05, 0.10, 0.50, 0.90, 0.95], axis=1).T
        q_df.columns = ["p05", "p10", "p50", "p90", "p95"]
        q_df.index = stack_fcst.index
    else:
        q_df = pd.DataFrame(index=stack_fcst.index, columns=["p05", "p10", "p50", "p90", "p95"], dtype=float)

    hist = pd.DataFrame({"actual": y})
    forecast_table = pd.concat([hist, stack_fit_cal.rename("fitted"), q_df], axis=1)

    # Backtest metrics
    test_win = min(CAL_WINDOW_MONTHS, len(stack_fit_cal))
    bt_idx = stack_fit_cal.dropna().tail(test_win).index
    bt_pred = stack_fit_cal.loc[bt_idx]
    bt_act  = y.loc[bt_idx]
    if len(bt_idx) >= 3 and not bt_pred.isna().any() and not bt_act.isna().any():
        r = float(np.corrcoef(bt_pred, bt_act)[0,1]); r2 = float(r*r)
    else:
        r = np.nan; r2 = np.nan

    leaderboard = pd.DataFrame({"target":[TARGET], "R2_last18":[r2], "r_last18":[r]})
    leaderboard.to_csv(f"backtest_{TSHORT.lower()}.csv", index=False)

    # Store
    results[TSHORT] = {
        "forecast_table": forecast_table,
        "leaderboard": leaderboard,
        "correlations": lag_tbl.reset_index(drop=True),
        "explain": explain_df.reset_index(drop=True),
        "ridge_contrib": contrib_df,  # per-month detailed contributions
    }

    print(f"\n=== {TSHORT} ===")
    print(leaderboard)

# ---------------- WRITE EXCEL ----------------
with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as xw:
    long_df.to_excel(xw, sheet_name="Series_Long", index=False)
    wide_idx.to_excel(xw, sheet_name="Wide_Index2019")
    meta_df.to_excel(xw, sheet_name="Metadata", index=False)
    if not failed_df.empty:
        failed_df.to_excel(xw, sheet_name="Failed", index=False)
    if not latest_df.empty:
        latest_df.to_excel(xw, sheet_name="Latest_Available", index=False)

    for TSHORT, pack in results.items():
        # Forecasts & leaderboard
        pack["forecast_table"].reset_index().rename(columns={"index": "date"}).to_excel(
            xw, sheet_name=f"Forecast_{TSHORT}", index=False
        )
        pack["leaderboard"].to_excel(xw, sheet_name=f"Leaderboard_{TSHORT}", index=False)

        # NEW: correlations (abs r ranked)
        if "correlations" in pack:
            corr_df = pack["correlations"].copy()
            corr_df["abs_r"] = corr_df["pearson"].abs()
            corr_df.sort_values("abs_r", ascending=False, inplace=True)
            corr_df.to_excel(xw, sheet_name=f"Corr_{TSHORT}", index=False)

        # NEW: explain table (coeffs + contributions)
        if "explain" in pack:
            pack["explain"].to_excel(xw, sheet_name=f"Explain_{TSHORT}", index=False)

        # NEW: per-month contributions matrix (optional, for audit)
        if "ridge_contrib" in pack:
            contrib_w = pack["ridge_contrib"].copy()
            contrib_w.reset_index().rename(columns={"index":"date"}).to_excel(
                xw, sheet_name=f"Contrib_{TSHORT}", index=False
            )

print(f"\n✅ Saved {OUTPUT_XLSX} with forecasts + backtests + explainability tabs")
