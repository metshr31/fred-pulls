# pull_fred_selected_ppi.py
# TL & IMDL: Ridge + SARIMAX + stack + isotonic (unchanged)
# LTL:       LassoCV + AR as base (stack + isotonic), same MC bands & exports

import os, time, re, random, warnings
import pandas as pd
import numpy as np
from fredapi import Fred

# --- ML / TS deps
from dateutil.relativedelta import relativedelta
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import RidgeCV, LassoCV, LinearRegression
from sklearn.isotonic import IsotonicRegression
from statsmodels.tsa.statespace.sarimax import SARIMAX
from scipy.stats import pearsonr

warnings.filterwarnings("ignore")

# ------------- CONFIG -------------
START_DATE  = "2016-01-01"
BASE_YEAR   = 2019
OUTPUT_XLSX = "fred_selected_ppi_2019base.xlsx"

# Forecasting pipeline (you can tune these)
FORECAST_HORIZON    = 12   # months forward
TOP_K_EXOG          = 10   # # of leading PPIs to use
MAX_LAG_MONTHS      = 12   # search leading effect 0..12m
AR_P                = 6    # AR lags of target for base learner
CAL_WINDOW_MONTHS   = 18   # recency emphasis window for stacking/calibration
MC_SIMS             = 200  # Monte Carlo paths for bands

# LASSO config (LTL only)
LASSO_ALPHAS        = np.logspace(-3, 1, 30)   # CV grid
LASSO_MAX_ITER      = 20000
LASSO_NFOLDS        = 5
STACK_RECENCY_HL    = 12     # half-life (months) for weighting the stack fit; set None to disable

# Targets to run (TL, IMDL unchanged; LTL uses LASSO)
NORTH_STARS = [
    ("PCU4841214841212", "TL"),       # Truckload line-haul
    ("PCU482111482111412", "IMDL"),   # Rail intermodal line-haul
    ("PCU4841224841221", "LTL"),      # LTL line-haul (Lasso base)
]

# FRED key from env (secrets in CI OK)
FRED_API_KEY = os.environ.get("FRED_API_KEY")
if not FRED_API_KEY:
    raise RuntimeError("FRED_API_KEY env var not set.")

# ======================= SERIES (grouped & commented, auto-cleaned below)
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
        sid = ln.split()[0].upper()
        ids.append(sid)
    seen, uniq = set(), []
    for sid in ids:
        if sid not in seen:
            seen.add(sid); uniq.append(sid)
    return uniq

SERIES_IDS = clean_series_ids(SERIES_IDS_BLOCK)

# ======================= Adaptive pacing / retries
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

# ------------------ Pull metadata
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

# ------------------ Pull series
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

# Wide pivots (level space: 2019=100)
wide_idx  = long_df.pivot_table(index="date", columns="series_id", values="index_2019=100", aggfunc="last").sort_index()
wide_raw  = long_df.pivot_table(index="date", columns="series_id", values="value", aggfunc="last").sort_index()

# ------------------ Helper functions
def monthly_index_add(idx_last: pd.Timestamp, months: int) -> pd.DatetimeIndex:
    out, cur = [], idx_last
    for _ in range(months):
        cur = cur + relativedelta(months=1)
        out.append(cur)
    return pd.DatetimeIndex(out)

def best_lag_table(y: pd.Series, X: pd.DataFrame, max_lag: int = 12, min_obs: int = 24) -> pd.DataFrame:
    rows = []
    for col in X.columns:
        x = X[col]
        best = None
        for lag in range(0, max_lag+1):
            xs = x.shift(lag)
            dfj = pd.concat([y, xs], axis=1).dropna()
            if dfj.shape[0] < min_obs:
                continue
            r, p = pearsonr(dfj.iloc[:,0], dfj.iloc[:,1])
            if (best is None) or (abs(r) > abs(best["pearson"])):
                best = {"feature": col, "best_lag": lag, "pearson": r, "p_value": p, "n_obs": dfj.shape[0]}
        if best is not None:
            rows.append(best)
    return pd.DataFrame(rows).sort_values(by="pearson", key=lambda s: s.abs(), ascending=False)

def build_exog_matrix(top_exog: pd.DataFrame, X_all: pd.DataFrame, idx: pd.DatetimeIndex) -> pd.DataFrame:
    Xmat = pd.DataFrame(index=idx)
    for _, r in top_exog.iterrows():
        feat = r["feature"]; lag = int(r["best_lag"]) if not pd.isna(r["best_lag"]) else 0
        Xmat[feat] = X_all[feat].shift(lag)
    return Xmat

def add_ar_terms(X: pd.DataFrame, y: pd.Series, p: int = 6) -> pd.DataFrame:
    out = X.copy()
    for L in range(1, p+1):
        out[f"y_lag{L}"] = y.shift(L)
    return out

def fit_exog_model(series: pd.Series):
    """Fit tiny SARIMA grid; return fitted model and residual std for MC."""
    orders = [(1,0,0),(0,1,1),(1,1,0),(1,1,1),(2,0,0)]
    best_aic, best_model = np.inf, None
    for order in orders:
        try:
            m = SARIMAX(series, order=order, enforce_stationarity=False, enforce_invertibility=False)
            r = m.fit(disp=False)
            if r.aic < best_aic:
                best_aic, best_model = r.aic, r
        except Exception:
            continue
    resid_std = float(np.nanstd(best_model.resid)) if best_model is not None else 0.0
    return best_model, resid_std

def forecast_exog_with_noise(model, last_date, horizon, resid_std, rng):
    idx = monthly_index_add(last_date, horizon)
    try:
        base = model.get_forecast(steps=horizon).predicted_mean.values
    except Exception:
        try:
            last_val = float(model.model.endog[-1])
        except Exception:
            last_val = 0.0
        base = np.repeat(last_val, horizon)
    noise = rng.normal(0.0, resid_std, size=horizon)
    return pd.Series(base + noise, index=idx)

def ridge_iterative_forecast(last_date: pd.Timestamp,
                             horizon: int,
                             model: Pipeline,
                             X_exog_lagged_full: pd.DataFrame,
                             y_hist: pd.Series,
                             p: int = 6) -> pd.Series:
    preds, cur = [], last_date
    y_tmp = y_hist.copy()
    X_filled = X_exog_lagged_full.copy().fillna(method="ffill").fillna(method="bfill")
    for _ in range(horizon):
        cur = cur + relativedelta(months=1)
        row = {c: X_filled.loc[cur, c] for c in X_filled.columns}
        for L in range(1, p+1):
            lag_date = cur - relativedelta(months=L)
            row[f"y_lag{L}"] = y_tmp.loc[lag_date] if lag_date in y_tmp.index else y_tmp.iloc[-1]
        xrow = pd.DataFrame([row], index=[cur])
        yhat = model.predict(xrow)[0]
        preds.append((cur, yhat))
        y_tmp.loc[cur] = yhat
    return pd.Series([v for (_, v) in preds], index=[d for (d, _) in preds], name="ridge_fcst")

def lasso_iterative_forecast(last_date: pd.Timestamp,
                             horizon: int,
                             model: Pipeline,
                             X_exog_lagged_full: pd.DataFrame,
                             y_hist: pd.Series,
                             p: int = 6) -> pd.Series:
    preds, cur = [], last_date
    y_tmp = y_hist.copy()
    X_filled = X_exog_lagged_full.copy().fillna(method="ffill").fillna(method="bfill")
    for _ in range(horizon):
        cur = cur + relativedelta(months=1)
        row = {c: X_filled.loc[cur, c] for c in X_filled.columns}
        for L in range(1, p+1):
            lag_date = cur - relativedelta(months=L)
            row[f"y_lag{L}"] = y_tmp.loc[lag_date] if lag_date in y_tmp.index else y_tmp.iloc[-1]
        xrow = pd.DataFrame([row], index=[cur])
        yhat = model.predict(xrow)[0]
        preds.append((cur, yhat))
        y_tmp.loc[cur] = yhat
    return pd.Series([v for (_, v) in preds], index=[d for (d, _) in preds], name="lasso_fcst")

def exp_weights(n: int, half_life: int) -> np.ndarray:
    idx = np.arange(n) - (n-1)
    lam = np.log(2) / max(half_life, 1)
    return np.exp(lam * idx)

# ------------------ Forecasting pipeline (multi-target)
def _sheet(name: str) -> str:
    return name[:31]

results = {}

for TARGET, TSHORT in NORTH_STARS:
    if TARGET not in wide_idx.columns:
        raise RuntimeError(f"Target '{TARGET}' not in pulled dataset. Check SERIES_IDS.")

    y = wide_idx[TARGET].dropna().copy()
    X_all = wide_idx.drop(columns=[TARGET]).copy()

    # 1) Choose top-K exogs by |corr| with best lead 0..MAX_LAG_MONTHS (LEVEL SPACE)
    lag_tbl = best_lag_table(y, X_all, max_lag=MAX_LAG_MONTHS, min_obs=24)
    top_exog = lag_tbl.head(TOP_K_EXOG)[["feature","best_lag"]].reset_index(drop=True)

    # 2) Deterministic exog forecast H+lag (ensures lagged exog exist in horizon)
    X_full = X_all.copy()
    for _, r in top_exog.iterrows():
        feat, lag = r["feature"], int(r["best_lag"])
        model, _ = fit_exog_model(X_all[feat].dropna())
        try:
            base = model.get_forecast(steps=FORECAST_HORIZON + lag).predicted_mean
            f_idx = monthly_index_add(X_all[feat].dropna().index[-1], FORECAST_HORIZON + lag)
            base = pd.Series(base.values, index=f_idx)
        except Exception:
            last_val = X_all[feat].dropna().iloc[-1]
            f_idx = monthly_index_add(X_all.index[-1], FORECAST_HORIZON + lag)
            base = pd.Series([last_val]*(FORECAST_HORIZON + lag), index=f_idx)
        for dt in base.index:
            if dt not in X_full.index:
                X_full.loc[dt] = np.nan
        X_full.loc[base.index, feat] = base

    full_idx = X_full.index.sort_values()
    X_exog_lagged_full = build_exog_matrix(top_exog, X_full, full_idx)

    # 3) Align train matrices
    df_train = pd.concat([y.rename("y"), X_exog_lagged_full], axis=1).dropna()
    y_train = df_train["y"]
    X_train_exog = df_train.drop(columns=["y"])

    # 4) Base learners by target
    if TSHORT == "LTL":
        # ---- LASSO + AR (LEVEL SPACE) — LTL ONLY
        XA = add_ar_terms(X_train_exog, y_train, p=AR_P)
        dfA = pd.concat([y_train.rename("y"), XA], axis=1).dropna()
        yA, XA = dfA["y"], dfA.drop(columns=["y"])
        lasso = Pipeline([
            ("scaler", StandardScaler(with_mean=True, with_std=True)),
            ("lasso",  LassoCV(alphas=LASSO_ALPHAS, cv=LASSO_NFOLDS, max_iter=LASSO_MAX_ITER, random_state=42))
        ])
        lasso.fit(XA, yA)
        base_fit = pd.Series(lasso.predict(XA), index=XA.index, name="lasso_fit")
        base_fcst = lasso_iterative_forecast(y.index[-1], FORECAST_HORIZON, lasso, X_exog_lagged_full, y, p=AR_P)
        base_name = "lasso"
        base_alpha = float(getattr(lasso.named_steps["lasso"], "alpha_", np.nan))
    else:
        # ---- Ridge + AR (LEVEL SPACE) — TL & IMDL (UNCHANGED)
        XA = add_ar_terms(X_train_exog, y_train, p=AR_P)
        dfA = pd.concat([y_train.rename("y"), XA], axis=1).dropna()
        yA, XA = dfA["y"], dfA.drop(columns=["y"])
        ridge = Pipeline([("scaler", StandardScaler()), ("ridge", RidgeCV(alphas=np.logspace(-4,3,40)))])
        ridge.fit(XA, yA)
        base_fit = pd.Series(ridge.predict(XA), index=XA.index, name="ridge_fit")
        base_fcst = ridge_iterative_forecast(y.index[-1], FORECAST_HORIZON, ridge, X_exog_lagged_full, y, p=AR_P)
        base_name = "ridge"
        base_alpha = float(getattr(ridge.named_steps["ridge"], "alpha_", np.nan))

    # 4b) SARIMAX + exog (LEVEL SPACE; all targets)
    sarimax = SARIMAX(endog=y_train, exog=X_train_exog.loc[y_train.index], order=(2,0,1), trend="c",
                      enforce_stationarity=False, enforce_invertibility=False).fit(disp=False)
    sarimax_fit = sarimax.get_prediction(start=y_train.index[0], end=y_train.index[-1],
                                         exog=X_train_exog.loc[y_train.index], dynamic=False).predicted_mean
    X_future_exog = X_exog_lagged_full.loc[base_fcst.index].copy().fillna(method="ffill").fillna(method="bfill")
    sarimax_fcst = sarimax.get_forecast(steps=len(X_future_exog), exog=X_future_exog).predicted_mean

    # 5) Linear stack trained on last CAL_WINDOW_MONTHS (optionally recency-weighted)
    common_idx = base_fit.index.intersection(sarimax_fit.index).intersection(y.index)
    stack_df = pd.DataFrame({"actual": y.loc[common_idx],
                             base_name: base_fit.loc[common_idx],
                             "sarimax": sarimax_fit.loc[common_idx]}).dropna()
    tail_idx = stack_df.tail(CAL_WINDOW_MONTHS).index
    X_stack = stack_df.loc[tail_idx, [base_name,"sarimax"]].values
    y_stack = stack_df.loc[tail_idx, "actual"].values

    if STACK_RECENCY_HL is not None:
        w = exp_weights(len(y_stack), STACK_RECENCY_HL)
        stack_lin = LinearRegression().fit(X_stack, y_stack, sample_weight=w)
    else:
        stack_lin = LinearRegression().fit(X_stack, y_stack)

    stack_fit = pd.Series(stack_lin.predict(stack_df[[base_name,"sarimax"]]),
                          index=stack_df.index, name="stack_fit")

    # 6) Isotonic calibration on recent window ONLY (applied to recent fit + entire forecast)
    iso = IsotonicRegression(out_of_bounds="clip").fit(
        stack_fit.loc[tail_idx].values, stack_df.loc[tail_idx, "actual"].values
    )
    stack_fit_cal = stack_fit.copy()
    stack_fit_cal.loc[tail_idx] = iso.transform(stack_fit.loc[tail_idx].values)

    stack_future_in = pd.DataFrame({base_name: base_fcst, "sarimax": sarimax_fcst}, index=base_fcst.index)
    stack_fcst = pd.Series(stack_lin.predict(stack_future_in), index=stack_future_in.index, name="stack_fcst")
    stack_fcst_cal = pd.Series(iso.transform(stack_fcst.values), index=stack_fcst.index, name="stack_fcst_cal")

    # 7) Confidence bands via Monte Carlo (propagate exog-forecast uncertainty)
    exog_models = {}
    for _, r in top_exog.iterrows():
        feat = r["feature"]
        model, resid_std = fit_exog_model(X_all[feat].dropna())
        exog_models[feat] = {"model": model, "std": resid_std, "lag": int(r["best_lag"])}

    rng = np.random.default_rng(42)
    sim_paths = []
    for s in range(MC_SIMS):
        X_full_sim = X_all.copy()
        for feat, info in exog_models.items():
            lag = info["lag"]; model = info["model"]; std = info["std"]
            try:
                last_obs_date = X_all[feat].dropna().index[-1]
            except Exception:
                last_obs_date = wide_idx.index[-1]
            f_series = forecast_exog_with_noise(model, last_obs_date, FORECAST_HORIZON + lag, std, rng)
            for dt in f_series.index:
                if dt not in X_full_sim.index:
                    X_full_sim.loc[dt] = np.nan
            X_full_sim.loc[f_series.index, feat] = f_series
        X_exog_lagged_sim = build_exog_matrix(top_exog, X_full_sim, X_full_sim.index.sort_values())
        if TSHORT == "LTL":
            base_fcst_sim = lasso_iterative_forecast(y.index[-1], FORECAST_HORIZON, lasso, X_exog_lagged_sim, y, p=AR_P)
        else:
            base_fcst_sim = ridge_iterative_forecast(y.index[-1], FORECAST_HORIZON, ridge, X_exog_lagged_sim, y, p=AR_P)
        X_future_exog_sim = X_exog_lagged_sim.loc[base_fcst_sim.index].copy().fillna(method="ffill").fillna(method="bfill")
        try:
            sarimax_fcst_sim = sarimax.get_forecast(steps=len(X_future_exog_sim), exog=X_future_exog_sim).predicted_mean
        except Exception:
            sarimax_fcst_sim = pd.Series(np.repeat(sarimax_fcst.iloc[-1], len(X_future_exog_sim)),
                                         index=X_future_exog_sim.index)
        stack_future_in_sim = pd.DataFrame({base_name: base_fcst_sim, "sarimax": sarimax_fcst_sim},
                                           index=base_fcst_sim.index)
        stack_fcst_sim = pd.Series(stack_lin.predict(stack_future_in_sim), index=stack_future_in_sim.index)
        stack_fcst_cal_sim = pd.Series(iso.transform(stack_fcst_sim.values), index=stack_fcst_sim.index)
        sim_paths.append(stack_fcst_cal_sim.rename(f"sim_{s}"))

    sim_df = pd.concat(sim_paths, axis=1)
    q_df = sim_df.quantile([0.05,0.10,0.50,0.90,0.95], axis=1).T
    q_df.columns = ["forecast_p05","forecast_p10","forecast_p50","forecast_p90","forecast_p95"]

    # 8) Assemble tidy outputs
    hist = pd.DataFrame({"actual": y})
    recent_projected = pd.Series(np.nan, index=hist.index, name="projected_fit")
    recent_projected.loc[stack_fit_cal.index] = stack_fit_cal  # calibrated in-sample projection for recent window

    forecast_table = pd.concat([hist, recent_projected, q_df], axis=1)

    # Most recent month summary
    last_month = y.index.max()
    most_recent_summary = {
        "most_recent_month": str(last_month.date()),
        "actual_most_recent": float(y.loc[last_month]),
        "projected_most_recent": float(forecast_table.loc[last_month, "projected_fit"])
                                  if pd.notna(forecast_table.loc[last_month, "projected_fit"]) else np.nan,
    }

    # Metadata / params
    stack_coeffs = {
        "intercept": float(stack_lin.intercept_),
        f"{base_name}_coef": float(stack_lin.coef_[0]),
        "sarimax_coef": float(stack_lin.coef_[1]),
        f"{base_name}_alpha": base_alpha
    }
    pipeline_params = {
        "horizon_months": FORECAST_HORIZON,
        "top_k_exog": TOP_K_EXOG,
        "max_lead_lag_months": MAX_LAG_MONTHS,
        "ar_p": AR_P,
        "cal_window_months": CAL_WINDOW_MONTHS,
        "mc_sims": MC_SIMS,
        "stack_recency_half_life": STACK_RECENCY_HL,
    }

    top_exog_table = top_exog.copy()
    top_exog_table["abs_pearson_rank"] = np.arange(1, len(top_exog_table)+1)

    results[TSHORT] = {
        "target": TARGET,
        "forecast_table": forecast_table,
        "most_recent_summary": most_recent_summary,
        "stack_coeffs": stack_coeffs,
        "pipeline_params": pipeline_params,
        "top_exog_table": top_exog_table,
    }

    # Per-target CSVs
    ft_csv = f"pcu_{TSHORT.lower()}_pipeline_output_with_bands.csv"
    f12_csv = f"pcu_{TSHORT.lower()}_forecast_next{FORECAST_HORIZON}m.csv"
    forecast_table.reset_index().rename(columns={"index":"date"}).to_csv(ft_csv, index=False)
    forecast_table.loc[forecast_table.index > last_month].reset_index().rename(columns={"index":"date"}).to_csv(f12_csv, index=False)
    print(f"✅ Wrote {ft_csv} and {f12_csv} for target {TARGET} ({TSHORT}).")

# ------------------ WRITE EXCEL (all sheets)
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

    # ---- Forecasting outputs (one set per north star)
    def _sheet(name: str) -> str: return name[:31]
    for TSHORT, pack in results.items():
        TARGET = pack["target"]
        forecast_table = pack["forecast_table"]
        most_recent_summary = pack["most_recent_summary"]
        stack_coeffs = pack["stack_coeffs"]
        pipeline_params = pack["pipeline_params"]
        top_exog_table = pack["top_exog_table"]

        forecast_table.reset_index().rename(columns={"index":"date"}).to_excel(
            xw, sheet_name=_sheet(f"Forecast_{TSHORT}"), index=False
        )
        pd.DataFrame([
            {"key":"most_recent_month","value":most_recent_summary["most_recent_month"]},
            {"key":"actual_most_recent","value":most_recent_summary["actual_most_recent"]},
            {"key":"projected_most_recent","value":most_recent_summary["projected_most_recent"]},
        ]).to_excel(xw, sheet_name=_sheet(f"Recent_{TSHORT}"), index=False)

        pd.DataFrame(stack_coeffs, index=[0]).to_excel(xw, sheet_name=_sheet(f"Stack_{TSHORT}"), index=False)
        pd.DataFrame(pipeline_params, index=[0]).to_excel(xw, sheet_name=_sheet(f"Params_{TSHORT}"), index=False)
        top_exog_table.to_excel(xw, sheet_name=_sheet(f"TopExog_{TSHORT}"), index=False)

print(f"✅ Saved {OUTPUT_XLSX} with {long_df['series_id'].nunique()} series.")
if not failed_df.empty:
    print(f"⚠️ {len(failed_df)} series failed (see 'Failed' sheet).")

print("✅ Wrote per-target forecast tables:")
for TSHORT in results.keys():
    print(f" - pcu_{TSHORT.lower()}_pipeline_output_with_bands.csv (history + p-bands)")
    print(f" - pcu_{TSHORT.lower()}_forecast_next{FORECAST_HORIZON}m.csv (forward only)")
print("ℹ️  See Excel sheets per target: Forecast_{TL/IMDL/LTL}, Recent_{TL/IMDL/LTL}, Stack_{TL/IMDL/LTL}, Params_{TL/IMDL/LTL}, TopExog_{TL/IMDL/LTL}.")
