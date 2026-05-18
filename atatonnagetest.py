#!/usr/bin/env python3
"""
ATA Truck Tonnage Forecast Model
Target: TRUCKD11

GitHub-safe forecasting version, patched for prediction-row iteration:
- Reads FRED_API_KEY from GitHub Secrets / environment variable.
- Pulls a capped, priority set of FRED/G.17 candidate series.
- Builds full-sample and train-only correlation screens.
- Builds true forecast targets for TRUCKD11 YoY at horizons 1, 3, 6, and 12 months.
- Compares naive baselines, Ridge, Lasso, and Elastic Net.
- Uses train-only feature selection to reduce leakage.
- Uses TimeSeriesSplit for CV inside the train period.
- Writes Excel workbook with model comparison, selected features, predictions, latest forecasts, and data tabs.

Run:
    python atatonnagetest.py --max-series 75 --max-model-features 75

Raise breadth later:
    python atatonnagetest.py --max-series 150 --max-model-features 100
"""

from __future__ import annotations

import argparse
import os
import re
import time
import warnings
from pathlib import Path
from typing import Iterable, Optional, Tuple

import numpy as np
import pandas as pd
import requests

from sklearn.impute import SimpleImputer
from sklearn.linear_model import RidgeCV, LassoCV, ElasticNetCV
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


# =============================================================================
# API / global settings
# =============================================================================

FRED_API_KEY = os.environ.get("FRED_API_KEY")
if not FRED_API_KEY:
    raise RuntimeError(
        "FRED_API_KEY env var not set. Define it in GitHub Secrets or your shell."
    )

FRED_BASE = "https://api.stlouisfed.org/fred"

TARGET_SERIES = "TRUCKD11"
FORECAST_HORIZONS = [1, 3, 6, 12]
FEATURE_LAGS = [0, 1, 2, 3, 6, 9, 12]

MIN_CORR_OBS = 36
MIN_MODEL_OBS = 72

MAX_SERIES_FIRST_TEST = 75
MAX_MODEL_FEATURES = 75
DEFAULT_WALK_FORWARD_STEP = 3
DEFAULT_MIN_TRAIN_MONTHS = 120

OUTPUT_XLSX = "ata_truck_tonnage_forecast_model.xlsx"
OUTPUT_DATA_DIR = Path("fred_download_cache")

REQUEST_SLEEP_SECONDS = 0.50
REQUEST_RETRIES = 8
FRED_DOWNLOAD_SLEEP_SECONDS = 1.25


# =============================================================================
# Series seeds
# =============================================================================

IP_3DIGIT = [
    "IPG311S", "IPG312S", "IPG313S", "IPG314S", "IPG315S", "IPG316S",
    "IPG321S", "IPG322S", "IPG323S", "IPG324S", "IPG325S", "IPG326S",
    "IPG327S", "IPG331S", "IPG332S", "IPG333S", "IPG334S", "IPG335S",
    "IPG336S", "IPG337S", "IPG339S",
]

IP_4DIGIT = [
    "IPG3111S", "IPG3112S", "IPG3113S", "IPG3114S", "IPG3115S",
    "IPG3116S", "IPG3118S", "IPG3119S",
    "IPG3221S", "IPG3222S",
    "IPG3241S",
    "IPG3251S", "IPG3252S", "IPG3253S", "IPG3254S", "IPG3255S", "IPG3256S",
    "IPG3261S", "IPG3262S",
    "IPG3271S", "IPG3272S", "IPG3273S",
    "IPG3311S", "IPG3312S", "IPG3313S", "IPG3314S", "IPG3315S",
    "IPG3321S", "IPG3322S", "IPG3323S", "IPG3324S", "IPG3325S",
    "IPG3326S", "IPG3327S", "IPG3328S", "IPG3329S",
    "IPG3331S", "IPG3332S", "IPG3333S", "IPG3334S", "IPG3335S",
    "IPG3336S", "IPG3339S",
    "IPG3341S", "IPG3342S", "IPG3343S", "IPG3344S", "IPG3345S", "IPG3346S",
    "IPG3351S", "IPG3352S", "IPG3353S", "IPG3359S",
    "IPG3361S", "IPG3362S", "IPG3363S", "IPG3364S", "IPG3365S", "IPG3366S", "IPG3369S",
    "IPG3371S", "IPG3372S", "IPG3379S",
    "IPG3391S", "IPG3399S",
]

AGGREGATE_SEEDS = [
    "INDPRO",
    "IPGMFN",
    "IPMAN",
    "IPMANSICS",
    "IPGDMFDS",
    "IPGNMFNS",
    "IPB50001S",
    "IPB51110S",
    "IPG1133S",
    "IPG1133N",
    "IPG5111S",
    "IPG5111N",
]

COMPARISON_SERIES = [
    "RRSFS",
    "RSAFS",
    "RSNSR",
    "RETAILIRSA",
    "FRGSHPUSM649NCIS",
    "FRGEXPUSM649NCIS",
    "TCU",
    "MCUMFN",
    "MCUMFND",
    "MCUMFNN",
]

CAPACITY_SEEDS = [
    "TCU", "MCUMFN", "MCUMFND", "MCUMFNN",
    "MCU311", "MCU312", "MCU313", "MCU314", "MCU315", "MCU316",
    "MCU321", "MCU322", "MCU323", "MCU324", "MCU325", "MCU326",
    "MCU327", "MCU331", "MCU332", "MCU333", "MCU334", "MCU335",
    "MCU336", "MCU337", "MCU339",
]

MANUFACTURING_NAICS_PREFIXES = ("31", "32", "33")

REQUESTED_SERIES = sorted(
    set(
        [TARGET_SERIES]
        + AGGREGATE_SEEDS
        + IP_3DIGIT
        + IP_4DIGIT
        + CAPACITY_SEEDS
        + COMPARISON_SERIES
    )
)


def priority_series_order() -> list[str]:
    key_4digit = [
        "IPG3112S", "IPG3116S", "IPG3118S",
        "IPG3221S", "IPG3222S",
        "IPG3241S",
        "IPG3251S", "IPG3252S", "IPG3254S",
        "IPG3261S", "IPG3262S",
        "IPG3273S",
        "IPG3311S", "IPG3313S", "IPG3315S",
        "IPG3323S", "IPG3324S", "IPG3327S",
        "IPG3331S", "IPG3332S", "IPG3335S", "IPG3339S",
        "IPG3344S", "IPG3345S",
        "IPG3353S",
        "IPG3361S", "IPG3362S", "IPG3363S", "IPG3364S", "IPG3365S",
        "IPG3371S", "IPG3372S",
        "IPG3391S",
    ]

    ordered = (
        [TARGET_SERIES]
        + AGGREGATE_SEEDS
        + IP_3DIGIT
        + COMPARISON_SERIES
        + CAPACITY_SEEDS
        + key_4digit
    )

    seen = set()
    out = []
    for sid in ordered:
        if sid not in seen:
            seen.add(sid)
            out.append(sid)
    return out


# =============================================================================
# FRED helpers
# =============================================================================

def fred_get_json(endpoint: str, **params) -> dict:
    url = f"{FRED_BASE}/{endpoint}"
    params = {**params, "api_key": FRED_API_KEY, "file_type": "json"}

    last_error = None
    for attempt in range(REQUEST_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=30)

            if response.status_code == 429:
                wait_seconds = min(120, 10 * (attempt + 1))
                print(f"Rate limited by FRED; waiting {wait_seconds}s before retry.", flush=True)
                time.sleep(wait_seconds)
                continue

            # Invalid/unavailable FRED IDs should not be retried.
            if response.status_code in (400, 404):
                response.raise_for_status()

            response.raise_for_status()
            time.sleep(REQUEST_SLEEP_SECONDS)
            return response.json()

        except Exception as exc:
            last_error = exc
            msg = str(exc).lower()

            if "400 client error" in msg or "404 client error" in msg:
                raise

            if "too many requests" in msg or "rate limit" in msg or "429" in msg:
                wait_seconds = min(120, 10 * (attempt + 1))
            else:
                wait_seconds = min(60, 3 * (attempt + 1))

            print(f"FRED request error: {exc}; waiting {wait_seconds}s before retry.", flush=True)
            time.sleep(wait_seconds)

    raise RuntimeError(
        f"FRED API request failed: endpoint={endpoint}, params={params}, error={last_error}"
    )


def get_series_info(series_id: str) -> Optional[dict]:
    try:
        data = fred_get_json("series", series_id=series_id)
        rows = data.get("seriess", [])
        return rows[0] if rows else None
    except Exception as exc:
        print(f"Metadata unavailable for {series_id}; skipping seed. Reason: {exc}", flush=True)
        return None


def get_releases() -> pd.DataFrame:
    data = fred_get_json("releases", limit=1000)
    return pd.DataFrame(data.get("releases", []))


def get_release_series(release_id: int, limit: int = 1000) -> pd.DataFrame:
    all_rows = []
    offset = 0

    while True:
        data = fred_get_json(
            "release/series",
            release_id=release_id,
            limit=limit,
            offset=offset,
            order_by="series_id",
            sort_order="asc",
        )

        rows = data.get("seriess", [])
        all_rows.extend(rows)

        count = int(data.get("count", len(rows)))
        offset += limit

        if offset >= count or not rows:
            break

    return pd.DataFrame(all_rows)


def find_g17_release_id() -> int:
    releases = get_releases()
    if releases.empty:
        raise RuntimeError("Could not retrieve FRED releases.")

    exact_mask = releases["name"].str.contains(
        "G.17 Industrial Production and Capacity Utilization",
        case=False,
        na=False,
        regex=False,
    )

    if exact_mask.any():
        return int(releases.loc[exact_mask, "id"].iloc[0])

    fuzzy_mask = (
        releases["name"].str.contains("Industrial Production", case=False, na=False)
        & releases["name"].str.contains("Capacity Utilization", case=False, na=False)
    )

    if fuzzy_mask.any():
        return int(releases.loc[fuzzy_mask, "id"].iloc[0])

    raise RuntimeError("Could not find G.17 release ID in FRED releases.")


# =============================================================================
# Inventory logic
# =============================================================================

def normalize_frequency(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def is_monthly(row: pd.Series) -> bool:
    frequency_short = normalize_frequency(row.get("frequency_short", ""))
    frequency_full = normalize_frequency(row.get("frequency", ""))
    return frequency_short == "M" or frequency_full.lower() == "monthly"


def title_contains_any(title: str, terms: Iterable[str]) -> bool:
    title_lower = title.lower()
    return any(term.lower() in title_lower for term in terms)


def extract_naics_from_title(title: str) -> Optional[str]:
    match = re.search(r"NAICS\s*=?\s*(\d{3,6})", str(title), flags=re.IGNORECASE)
    return match.group(1) if match else None


def classify_category(row: pd.Series) -> str:
    series_id = str(row.get("id", row.get("series_id", "")))
    title = str(row.get("title", ""))
    title_lower = title.lower()
    naics = extract_naics_from_title(title)

    if series_id == TARGET_SERIES:
        return "target"

    if series_id in COMPARISON_SERIES:
        return "retail_freight_comparison"

    if "capacity utilization" in title_lower:
        return "capacity_utilization"

    if naics:
        if naics.startswith(MANUFACTURING_NAICS_PREFIXES):
            return "ip_manufacturing_3digit" if len(naics) == 3 else "ip_manufacturing_detail"

        if naics in {"1133", "5111"}:
            return "ip_manufacturing_adjacent"

    if title_contains_any(title, ["manufacturing", "durable goods", "nondurable goods"]):
        return "ip_manufacturing_aggregate"

    if series_id in AGGREGATE_SEEDS:
        return "aggregate_seed"

    return "other"


def candidate_filter(row: pd.Series) -> bool:
    series_id = str(row.get("id", row.get("series_id", "")))
    title = str(row.get("title", ""))
    title_lower = title.lower()
    naics = extract_naics_from_title(title)

    if series_id in REQUESTED_SERIES:
        return True

    if not is_monthly(row):
        return False

    if title_lower.startswith("industrial production:"):
        if naics and (naics.startswith(MANUFACTURING_NAICS_PREFIXES) or naics in {"1133", "5111"}):
            return True

        if title_contains_any(
            title,
            [
                "manufacturing",
                "durable goods",
                "nondurable goods",
                "business equipment",
                "materials",
                "consumer goods",
                "energy",
            ],
        ):
            return True

    if title_lower.startswith("capacity utilization:"):
        if naics and naics.startswith(MANUFACTURING_NAICS_PREFIXES):
            return True

        if title_contains_any(
            title,
            [
                "manufacturing",
                "durable",
                "nondurable",
                "food",
                "chemical",
                "paper",
                "machinery",
                "transportation equipment",
                "computer",
                "electronic",
                "primary metal",
                "fabricated metal",
                "plastics",
                "rubber",
            ],
        ):
            return True

    return False


def concept_key(row: pd.Series) -> str:
    series_id = str(row.get("id", row.get("series_id", "")))
    title = str(row.get("title", ""))
    title_lower = re.sub(r"\s+", " ", title.lower()).strip()
    naics = extract_naics_from_title(title)

    if naics:
        if title_lower.startswith("industrial production"):
            return f"ip_naics_{naics}"
        if title_lower.startswith("capacity utilization"):
            return f"cu_naics_{naics}"

    normalized = title_lower
    normalized = normalized.replace("not seasonally adjusted", "")
    normalized = normalized.replace("seasonally adjusted", "")
    normalized = re.sub(r"\([^)]*\)", "", normalized)
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized).strip("_")

    if series_id in COMPARISON_SERIES or series_id == TARGET_SERIES:
        return series_id

    return normalized or series_id


def seasonal_rank(row: pd.Series) -> int:
    seasonal_short = str(row.get("seasonal_adjustment_short", "")).upper()
    series_id = str(row.get("id", row.get("series_id", "")))

    if seasonal_short == "SA" or series_id.endswith("S"):
        return 0

    if seasonal_short == "NSA" or series_id.endswith("N"):
        return 1

    return 2


def end_date_rank(row: pd.Series) -> pd.Timestamp:
    for column in ["observation_end", "last_updated"]:
        value = row.get(column)

        if pd.notna(value):
            try:
                return pd.to_datetime(value)
            except Exception:
                pass

    return pd.Timestamp.min


def build_candidate_inventory(max_series: Optional[int] = MAX_SERIES_FIRST_TEST) -> Tuple[pd.DataFrame, pd.DataFrame]:
    print("Finding G.17 release ID...", flush=True)
    g17_release_id = find_g17_release_id()
    print(f"G.17 release_id = {g17_release_id}", flush=True)

    print("Downloading G.17 release series inventory...", flush=True)
    g17_inventory = get_release_series(g17_release_id)

    if "id" not in g17_inventory.columns and "series_id" in g17_inventory.columns:
        g17_inventory = g17_inventory.rename(columns={"series_id": "id"})

    known_ids = set(g17_inventory["id"].astype(str)) if not g17_inventory.empty else set()
    seed_rows = []

    for series_id in REQUESTED_SERIES:
        if series_id in known_ids:
            continue

        info = get_series_info(series_id)
        if info:
            seed_rows.append(info)

    seeds = pd.DataFrame(seed_rows)

    full_inventory = (
        pd.concat([g17_inventory, seeds], ignore_index=True, sort=False)
        .drop_duplicates("id")
    )

    full_inventory = full_inventory[full_inventory.apply(candidate_filter, axis=1)].copy()

    full_inventory["category"] = full_inventory.apply(classify_category, axis=1)
    full_inventory["naics"] = full_inventory["title"].apply(extract_naics_from_title)
    full_inventory["concept_key"] = full_inventory.apply(concept_key, axis=1)
    full_inventory["seasonal_rank"] = full_inventory.apply(seasonal_rank, axis=1)
    full_inventory["end_date_rank"] = full_inventory.apply(end_date_rank, axis=1)

    full_inventory = full_inventory.sort_values(
        ["concept_key", "seasonal_rank", "end_date_rank", "id"],
        ascending=[True, True, False, True],
    )

    primary_inventory = (
        full_inventory
        .sort_values(
            ["concept_key", "seasonal_rank", "end_date_rank"],
            ascending=[True, True, False],
        )
        .groupby("concept_key", as_index=False)
        .head(1)
        .copy()
    )

    missing_priority = set([TARGET_SERIES] + COMPARISON_SERIES) - set(
        primary_inventory["id"].astype(str)
    )

    if missing_priority:
        additions = full_inventory[full_inventory["id"].astype(str).isin(missing_priority)]
        primary_inventory = (
            pd.concat([primary_inventory, additions], ignore_index=True, sort=False)
            .drop_duplicates("id")
        )

    primary_inventory = primary_inventory.sort_values(
        ["category", "naics", "id"],
        na_position="last",
    ).reset_index(drop=True)

    full_inventory = full_inventory.sort_values(
        ["category", "naics", "id"],
        na_position="last",
    ).reset_index(drop=True)

    if max_series is not None and max_series > 0 and len(primary_inventory) > max_series:
        priority = priority_series_order()
        priority_rank = {series_id: rank for rank, series_id in enumerate(priority)}

        primary_inventory["priority_rank"] = (
            primary_inventory["id"].astype(str).map(priority_rank).fillna(999999).astype(int)
        )

        primary_inventory = (
            primary_inventory
            .sort_values(["priority_rank", "category", "naics", "id"], na_position="last")
            .head(max_series)
            .drop(columns=["priority_rank"])
            .reset_index(drop=True)
        )

        if TARGET_SERIES not in set(primary_inventory["id"].astype(str)):
            target_row = full_inventory[full_inventory["id"].astype(str) == TARGET_SERIES]
            if not target_row.empty:
                primary_inventory = (
                    pd.concat([target_row.head(1), primary_inventory], ignore_index=True, sort=False)
                    .drop_duplicates("id")
                    .head(max_series)
                )

    return primary_inventory, full_inventory


# =============================================================================
# Download and transformations
# =============================================================================

def download_one_series(series_id: str) -> pd.Series:
    last_error = None

    for attempt in range(REQUEST_RETRIES):
        try:
            data = fred_get_json("series/observations", series_id=series_id)
            observations = data.get("observations", [])

            rows = []
            for obs in observations:
                value = obs.get("value")

                if value in (None, "", ".", "#N/A"):
                    numeric_value = np.nan
                else:
                    try:
                        numeric_value = float(value)
                    except Exception:
                        numeric_value = np.nan

                rows.append((pd.to_datetime(obs["date"]), numeric_value))

            if not rows:
                return pd.Series(dtype=float, name=series_id)

            series = pd.Series(
                data=[value for _, value in rows],
                index=pd.DatetimeIndex([date for date, _ in rows], name="date"),
                name=series_id,
                dtype=float,
            )

            return series.sort_index()

        except Exception as exc:
            last_error = exc
            msg = str(exc).lower()

            if "400 client error" in msg or "404 client error" in msg:
                raise

            if "too many requests" in msg or "rate limit" in msg or "429" in msg:
                wait_seconds = min(120, 10 * (attempt + 1))
            else:
                wait_seconds = min(60, 3 * (attempt + 1))

            print(
                f"  Error/rate-limit on {series_id}: {exc}; waiting {wait_seconds}s "
                f"before retry {attempt + 1}/{REQUEST_RETRIES}",
                flush=True,
            )
            time.sleep(wait_seconds)

    raise RuntimeError(f"Failed downloading {series_id} after retries: {last_error}")


def download_series_matrix(inventory: pd.DataFrame, cache_dir: Path) -> pd.DataFrame:
    cache_dir.mkdir(parents=True, exist_ok=True)

    series_ids = inventory["id"].astype(str).tolist()
    all_series = []

    for index, series_id in enumerate(series_ids, start=1):
        print(f"[{index}/{len(series_ids)}] Downloading {series_id}", flush=True)

        cache_file = cache_dir / f"{series_id}.csv"

        if cache_file.exists():
            try:
                cached = pd.read_csv(cache_file, parse_dates=["date"]).set_index("date")
                cached_series = cached[series_id]
                cached_series.name = series_id
                all_series.append(cached_series)
                continue
            except Exception:
                pass

        try:
            series = download_one_series(series_id)

            if not series.empty:
                series.to_frame().to_csv(cache_file)
                all_series.append(series)

        except Exception as exc:
            print(f"WARNING: failed downloading {series_id}: {exc}", flush=True)

        time.sleep(FRED_DOWNLOAD_SLEEP_SECONDS)

    if not all_series:
        raise RuntimeError("No series downloaded.")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        raw = pd.concat(all_series, axis=1, sort=True).sort_index()

    raw.index = pd.to_datetime(raw.index).to_period("M").to_timestamp()
    raw = raw.groupby(raw.index).last().sort_index()

    return raw


def pct_change_clean(df: pd.DataFrame, periods: int) -> pd.DataFrame:
    return df.pct_change(periods=periods, fill_method=None).replace(
        [np.inf, -np.inf],
        np.nan,
    )


# =============================================================================
# Screens and feature engineering
# =============================================================================

def pearson_pair(x: pd.Series, y: pd.Series, min_obs: int = MIN_CORR_OBS) -> Tuple[float, int]:
    paired = pd.concat([x, y], axis=1).dropna()
    n = len(paired)

    if n < min_obs:
        return np.nan, n

    return float(paired.iloc[:, 0].corr(paired.iloc[:, 1])), n


def keeper_class(abs_corr: float) -> str:
    if pd.isna(abs_corr):
        return "Insufficient data"
    if abs_corr >= 0.60:
        return "Strong keeper"
    if abs_corr >= 0.45:
        return "Useful keeper"
    if abs_corr >= 0.30:
        return "Maybe"
    return "Weak"


def build_full_correlation_screen(
    raw: pd.DataFrame,
    yoy: pd.DataFrame,
    mom: pd.DataFrame,
    inventory: pd.DataFrame,
) -> pd.DataFrame:
    target_raw = raw[TARGET_SERIES]
    target_yoy = yoy[TARGET_SERIES]
    target_mom = mom[TARGET_SERIES]

    metadata = inventory.set_index("id", drop=False)
    rows = []

    for series_id in raw.columns:
        if series_id == TARGET_SERIES:
            continue

        raw_corr, raw_n = pearson_pair(target_raw, raw[series_id])
        mom_lag0, _ = pearson_pair(target_mom, mom[series_id])

        lag_corrs = {}
        lag_counts = {}

        for lag in FEATURE_LAGS:
            corr, count = pearson_pair(target_yoy.shift(-lag), yoy[series_id])
            lag_corrs[lag] = corr
            lag_counts[lag] = count

        valid_lags = {lag: corr for lag, corr in lag_corrs.items() if not pd.isna(corr)}
        if valid_lags:
            best_lag = max(valid_lags, key=lambda lag: abs(valid_lags[lag]))
            best_corr = valid_lags[best_lag]
            abs_best_corr = abs(best_corr)
            observation_count = lag_counts[best_lag]
        else:
            best_lag = np.nan
            best_corr = np.nan
            abs_best_corr = np.nan
            observation_count = max(lag_counts.values()) if lag_counts else 0

        meta = metadata.loc[series_id] if series_id in metadata.index else pd.Series(dtype=object)

        rows.append(
            {
                "feature": series_id,
                "series_name": meta.get("title", ""),
                "source": meta.get("source", ""),
                "frequency": meta.get("frequency", ""),
                "seasonal_adjustment": meta.get("seasonal_adjustment", ""),
                "category": meta.get("category", ""),
                "naics": meta.get("naics", ""),
                "raw_corr": raw_corr,
                "raw_obs": raw_n,
                "yoy_lag0": lag_corrs[0],
                "mom_lag0": mom_lag0,
                "yoy_lag1": lag_corrs[1],
                "yoy_lag2": lag_corrs[2],
                "yoy_lag3": lag_corrs[3],
                "yoy_lag6": lag_corrs[6],
                "yoy_lag9": lag_corrs[9],
                "yoy_lag12": lag_corrs[12],
                "best_lag": best_lag,
                "best_yoy_corr": best_corr,
                "abs_best_yoy_corr": abs_best_corr,
                "observation_count": observation_count,
                "keeper_class": keeper_class(abs_best_corr),
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    return out.sort_values(["abs_best_yoy_corr", "feature"], ascending=[False, True]).reset_index(drop=True)


def make_lagged_feature_matrix(yoy: pd.DataFrame, candidate_ids: list[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    feature_parts = []
    feature_map_rows = []

    for series_id in candidate_ids:
        if series_id == TARGET_SERIES or series_id not in yoy.columns:
            continue

        for lag in FEATURE_LAGS:
            model_feature = f"{series_id}__lag{lag}"
            feature_parts.append(yoy[series_id].shift(lag).rename(model_feature))
            feature_map_rows.append({"model_feature": model_feature, "feature": series_id, "lag": lag})

    x_matrix = pd.concat(feature_parts, axis=1) if feature_parts else pd.DataFrame(index=yoy.index)
    feature_map = pd.DataFrame(feature_map_rows, columns=["model_feature", "feature", "lag"])
    return x_matrix, feature_map


def make_ar_feature_matrix(yoy: pd.DataFrame, mom: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build autoregressive TRUCKD11 features available at forecast origin t.

    These features test whether outside indicators add value beyond TRUCKD11's
    own momentum. For forecast horizon h, the target is TRUCKD11_YOY[t+h],
    while these features use only information at t or earlier.
    """
    parts = []
    rows = []

    target_yoy = yoy[TARGET_SERIES]
    target_mom = mom[TARGET_SERIES]

    yoy_lags = [0, 1, 2, 3, 6, 9, 12]
    mom_lags = [0, 1, 2, 3, 6]
    rolling_windows = [3, 6, 12]

    for lag in yoy_lags:
        col = f"{TARGET_SERIES}_AR_YOY_lag{lag}"
        parts.append(target_yoy.shift(lag).rename(col))
        rows.append({"model_feature": col, "feature": col, "lag": lag, "feature_set": "AR"})

    for lag in mom_lags:
        col = f"{TARGET_SERIES}_AR_MOM_lag{lag}"
        parts.append(target_mom.shift(lag).rename(col))
        rows.append({"model_feature": col, "feature": col, "lag": lag, "feature_set": "AR"})

    for window in rolling_windows:
        col = f"{TARGET_SERIES}_AR_YOY_roll{window}"
        parts.append(target_yoy.rolling(window).mean().rename(col))
        rows.append({"model_feature": col, "feature": col, "lag": 0, "feature_set": "AR"})

    for window in rolling_windows:
        col = f"{TARGET_SERIES}_AR_MOM_roll{window}"
        parts.append(target_mom.rolling(window).mean().rename(col))
        rows.append({"model_feature": col, "feature": col, "lag": 0, "feature_set": "AR"})

    x_matrix = pd.concat(parts, axis=1) if parts else pd.DataFrame(index=yoy.index)
    feature_map = pd.DataFrame(rows, columns=["model_feature", "feature", "lag", "feature_set"])
    return x_matrix, feature_map



def select_features_train_only(
    x_train_all: pd.DataFrame,
    y_train: pd.Series,
    feature_map: pd.DataFrame,
    max_features: int,
) -> Tuple[list[str], pd.DataFrame]:
    rows = []

    for col in x_train_all.columns:
        corr, n = pearson_pair(y_train, x_train_all[col], min_obs=MIN_CORR_OBS)
        fm = feature_map.loc[feature_map["model_feature"] == col]
        if fm.empty:
            continue

        rows.append(
            {
                "model_feature": col,
                "feature": fm["feature"].iloc[0],
                "lag": int(fm["lag"].iloc[0]),
                "train_pearson": corr,
                "train_abs_pearson": abs(corr) if not pd.isna(corr) else np.nan,
                "train_obs": n,
            }
        )

    ranked = pd.DataFrame(rows)
    if ranked.empty:
        return [], ranked

    ranked = ranked[ranked["train_obs"] >= MIN_MODEL_OBS].copy()
    ranked = ranked.dropna(subset=["train_abs_pearson"])

    if ranked.empty:
        return [], ranked

    # Keep only best lag per base series, based on train-period correlation.
    ranked = ranked.sort_values(["feature", "train_abs_pearson"], ascending=[True, False])
    best_per_series = ranked.groupby("feature", as_index=False).head(1).copy()

    selected = (
        best_per_series
        .sort_values("train_abs_pearson", ascending=False)
        .head(max_features)
        .reset_index(drop=True)
    )

    return selected["model_feature"].tolist(), selected


# =============================================================================
# Forecast models
# =============================================================================

def safe_timeseries_cv(n_train: int) -> TimeSeriesSplit:
    n_splits = min(5, max(2, n_train // 36))
    if n_train < 80:
        n_splits = 2
    return TimeSeriesSplit(n_splits=n_splits)


def evaluate_predictions(
    y_true: pd.Series,
    y_pred: pd.Series,
    model_name: str,
    horizon: int,
    n_features: int,
    nonzero_features: int,
    alpha: Optional[float] = None,
    l1_ratio: Optional[float] = None,
) -> dict:
    aligned = pd.concat([y_true.rename("actual"), y_pred.rename("predicted")], axis=1).dropna()

    if aligned.empty:
        return {
            "horizon": horizon,
            "model": model_name,
            "n_obs": 0,
            "n_features": n_features,
            "nonzero_features": nonzero_features,
            "alpha": alpha,
            "l1_ratio": l1_ratio,
            "r2": np.nan,
            "mae": np.nan,
            "mse": np.nan,
            "rmse": np.nan,
            "bias": np.nan,
            "directional_accuracy": np.nan,
        }

    errors = aligned["predicted"] - aligned["actual"]
    mse = float(np.mean(errors ** 2))

    direction = np.sign(aligned["actual"]) == np.sign(aligned["predicted"])
    directional_accuracy = float(direction.mean())

    return {
        "horizon": horizon,
        "model": model_name,
        "n_obs": len(aligned),
        "n_features": n_features,
        "nonzero_features": nonzero_features,
        "alpha": alpha,
        "l1_ratio": l1_ratio,
        "r2": r2_score(aligned["actual"], aligned["predicted"]) if len(aligned) >= 2 else np.nan,
        "mae": mean_absolute_error(aligned["actual"], aligned["predicted"]),
        "mse": mse,
        "rmse": float(np.sqrt(mse)),
        "bias": float(errors.mean()),
        "directional_accuracy": directional_accuracy,
    }


def fit_linear_model(model_name: str, x_train: pd.DataFrame, y_train: pd.Series):
    cv = safe_timeseries_cv(len(y_train))

    if model_name == "Ridge":
        estimator = RidgeCV(alphas=np.logspace(-4, 4, 80), cv=cv)
    elif model_name == "Lasso":
        estimator = LassoCV(
            alphas=np.logspace(-4, 1, 80),
            cv=cv,
            max_iter=50000,
            random_state=42,
        )
    elif model_name == "ElasticNet":
        estimator = ElasticNetCV(
            l1_ratio=[0.10, 0.25, 0.50, 0.75, 0.90],
            alphas=np.logspace(-4, 1, 80),
            cv=cv,
            max_iter=50000,
            random_state=42,
        )
    else:
        raise ValueError(f"Unknown model: {model_name}")

    pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("model", estimator),
        ]
    )

    pipe.fit(x_train, y_train)
    return pipe


def model_alpha(pipe) -> Optional[float]:
    model = pipe.named_steps["model"]
    alpha = getattr(model, "alpha_", None)
    if alpha is None:
        return None
    return float(alpha)


def model_l1_ratio(pipe) -> Optional[float]:
    model = pipe.named_steps["model"]
    l1_ratio = getattr(model, "l1_ratio_", None)
    if l1_ratio is None:
        return None
    return float(l1_ratio)


def model_coefficients(pipe, columns: list[str]) -> pd.Series:
    model = pipe.named_steps["model"]
    coefs = getattr(model, "coef_", np.zeros(len(columns)))
    return pd.Series(coefs, index=columns, name="coefficient")


def build_best_model_summary(model_comparison: pd.DataFrame) -> pd.DataFrame:
    """
    Pick best out-of-sample model by horizon and create a practical recommendation.

    Primary ranking: test RMSE, then test MAE, then test R2.
    We also compare:
    - best AR model vs best baseline
    - best ARPlusIndicators vs best AROnly
    """
    if model_comparison.empty:
        return pd.DataFrame()

    test = model_comparison[
        (model_comparison.get("sample") == "test")
        & model_comparison.get("rmse").notna()
    ].copy()

    if test.empty:
        return pd.DataFrame()

    rows = []

    for horizon, group in test.groupby("horizon"):
        ranked = group.sort_values(["rmse", "mae", "r2"], ascending=[True, True, False]).copy()
        best = ranked.iloc[0]

        baseline = group[group["feature_set"] == "Baseline"].sort_values(["rmse", "mae"], ascending=[True, True])
        ar_only = group[group["feature_set"] == "AROnly"].sort_values(["rmse", "mae"], ascending=[True, True])
        ar_plus = group[group["feature_set"] == "ARPlusIndicators"].sort_values(["rmse", "mae"], ascending=[True, True])
        indicators = group[group["feature_set"] == "IndicatorsOnly"].sort_values(["rmse", "mae"], ascending=[True, True])

        best_baseline = baseline.iloc[0] if not baseline.empty else pd.Series(dtype=object)
        best_ar = ar_only.iloc[0] if not ar_only.empty else pd.Series(dtype=object)
        best_ar_plus = ar_plus.iloc[0] if not ar_plus.empty else pd.Series(dtype=object)
        best_indicators = indicators.iloc[0] if not indicators.empty else pd.Series(dtype=object)

        best_rmse = float(best["rmse"])
        baseline_rmse = float(best_baseline["rmse"]) if not best_baseline.empty else np.nan
        ar_rmse = float(best_ar["rmse"]) if not best_ar.empty else np.nan
        ar_plus_rmse = float(best_ar_plus["rmse"]) if not best_ar_plus.empty else np.nan

        improvement_vs_baseline = (
            (baseline_rmse - best_rmse) / baseline_rmse
            if pd.notna(baseline_rmse) and baseline_rmse != 0 else np.nan
        )

        ar_plus_vs_ar = (
            (ar_rmse - ar_plus_rmse) / ar_rmse
            if pd.notna(ar_rmse) and pd.notna(ar_plus_rmse) and ar_rmse != 0 else np.nan
        )

        if pd.notna(improvement_vs_baseline) and improvement_vs_baseline >= 0.10 and best["r2"] > 0:
            recommendation = "Use"
        elif pd.notna(improvement_vs_baseline) and improvement_vs_baseline >= 0.03 and best["r2"] > 0:
            recommendation = "Watch"
        elif best["r2"] > 0:
            recommendation = "Experimental"
        else:
            recommendation = "Do not use"

        if pd.notna(ar_plus_vs_ar) and ar_plus_vs_ar > 0.03:
            indicator_value = "Indicators add value vs AROnly"
        elif pd.notna(ar_plus_vs_ar) and ar_plus_vs_ar > -0.03:
            indicator_value = "Indicators roughly neutral vs AROnly"
        else:
            indicator_value = "Indicators do not add value vs AROnly"

        rows.append(
            {
                "horizon": horizon,
                "best_model": best["model"],
                "best_feature_set": best["feature_set"],
                "best_test_r2": best["r2"],
                "best_test_mae": best["mae"],
                "best_test_rmse": best["rmse"],
                "best_directional_accuracy": best["directional_accuracy"],
                "best_baseline_model": best_baseline.get("model", np.nan),
                "best_baseline_rmse": best_baseline.get("rmse", np.nan),
                "best_aronly_model": best_ar.get("model", np.nan),
                "best_aronly_rmse": best_ar.get("rmse", np.nan),
                "best_arplus_model": best_ar_plus.get("model", np.nan),
                "best_arplus_rmse": best_ar_plus.get("rmse", np.nan),
                "best_indicators_model": best_indicators.get("model", np.nan),
                "best_indicators_rmse": best_indicators.get("rmse", np.nan),
                "rmse_improvement_vs_best_baseline": improvement_vs_baseline,
                "arplus_rmse_improvement_vs_aronly": ar_plus_vs_ar,
                "indicator_incremental_value": indicator_value,
                "recommendation": recommendation,
            }
        )

    return pd.DataFrame(rows).sort_values("horizon").reset_index(drop=True)


def build_incremental_value_test(model_comparison: pd.DataFrame) -> pd.DataFrame:
    """
    Horizon-by-horizon comparison of ARPlusIndicators vs AROnly and IndicatorsOnly.
    """
    if model_comparison.empty:
        return pd.DataFrame()

    test = model_comparison[
        (model_comparison.get("sample") == "test")
        & model_comparison.get("rmse").notna()
    ].copy()

    rows = []

    for horizon, group in test.groupby("horizon"):
        def best_for(feature_set: str) -> pd.Series:
            subset = group[group["feature_set"] == feature_set]
            if subset.empty:
                return pd.Series(dtype=object)
            return subset.sort_values(["rmse", "mae", "r2"], ascending=[True, True, False]).iloc[0]

        baseline = best_for("Baseline")
        ar = best_for("AROnly")
        arplus = best_for("ARPlusIndicators")
        ind = best_for("IndicatorsOnly")

        def improvement(a, b):
            # Positive means a is better than b using RMSE.
            if a.empty or b.empty:
                return np.nan
            if pd.isna(a.get("rmse")) or pd.isna(b.get("rmse")) or b.get("rmse") == 0:
                return np.nan
            return (b.get("rmse") - a.get("rmse")) / b.get("rmse")

        rows.append(
            {
                "horizon": horizon,
                "best_baseline": baseline.get("model", np.nan),
                "best_baseline_rmse": baseline.get("rmse", np.nan),
                "best_aronly": ar.get("model", np.nan),
                "best_aronly_rmse": ar.get("rmse", np.nan),
                "best_arplus": arplus.get("model", np.nan),
                "best_arplus_rmse": arplus.get("rmse", np.nan),
                "best_indicators": ind.get("model", np.nan),
                "best_indicators_rmse": ind.get("rmse", np.nan),
                "aronly_improvement_vs_baseline": improvement(ar, baseline),
                "arplus_improvement_vs_baseline": improvement(arplus, baseline),
                "arplus_improvement_vs_aronly": improvement(arplus, ar),
                "indicators_improvement_vs_baseline": improvement(ind, baseline),
            }
        )

    return pd.DataFrame(rows).sort_values("horizon").reset_index(drop=True)


def build_recommended_forecasts(
    best_model_summary: pd.DataFrame,
    latest_forecast: pd.DataFrame,
) -> pd.DataFrame:
    """
    Final clean forecast table using the best model by horizon.

    Uses common_origin forecasts first so all models are anchored to the same
    latest known TRUCKD11 month. This is the clean table for practical use.
    """
    if best_model_summary.empty or latest_forecast.empty:
        return pd.DataFrame()

    common = latest_forecast[latest_forecast["forecast_origin_type"] == "common_origin"].copy()
    rows = []

    for _, best in best_model_summary.iterrows():
        horizon = int(best["horizon"])
        model = best["best_model"]

        match = common[
            (common["horizon"] == horizon)
            & (common["model"] == model)
        ]

        if match.empty:
            continue

        forecast = match.iloc[0]

        rows.append(
            {
                "horizon": horizon,
                "recommended_model": model,
                "recommendation": best.get("recommendation", ""),
                "indicator_incremental_value": best.get("indicator_incremental_value", ""),
                "forecast_origin": forecast["forecast_origin"],
                "forecast_target_date": forecast["forecast_target_date"],
                "forecast_truckd11_yoy": forecast["forecast_truckd11_yoy"],
                "test_r2": best.get("best_test_r2", np.nan),
                "test_mae": best.get("best_test_mae", np.nan),
                "test_rmse": best.get("best_test_rmse", np.nan),
                "directional_accuracy": best.get("best_directional_accuracy", np.nan),
                "rmse_improvement_vs_best_baseline": best.get("rmse_improvement_vs_best_baseline", np.nan),
            }
        )

    return pd.DataFrame(rows).sort_values("horizon").reset_index(drop=True)


def evaluate_walk_forward_results(wf_predictions: pd.DataFrame) -> pd.DataFrame:
    """
    Summarize walk-forward predictions by horizon/model.
    """
    if wf_predictions.empty:
        return pd.DataFrame()

    rows = []

    for (horizon, model, feature_set), group in wf_predictions.groupby(["horizon", "model", "feature_set"]):
        aligned = group.dropna(subset=["actual", "predicted"]).copy()
        if aligned.empty:
            continue

        errors = aligned["predicted"] - aligned["actual"]
        mse = float(np.mean(errors ** 2))
        rows.append(
            {
                "horizon": horizon,
                "model": model,
                "feature_set": feature_set,
                "n_obs": len(aligned),
                "r2": r2_score(aligned["actual"], aligned["predicted"]) if len(aligned) >= 2 else np.nan,
                "mae": mean_absolute_error(aligned["actual"], aligned["predicted"]),
                "mse": mse,
                "rmse": float(np.sqrt(mse)),
                "bias": float(errors.mean()),
                "directional_accuracy": float((np.sign(aligned["actual"]) == np.sign(aligned["predicted"])).mean()),
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    return out.sort_values(["horizon", "rmse", "mae"], ascending=[True, True, True]).reset_index(drop=True)


def build_walk_forward_backtest(
    yoy: pd.DataFrame,
    mom: pd.DataFrame,
    max_model_features: int,
    min_train_months: int = DEFAULT_MIN_TRAIN_MONTHS,
    step: int = DEFAULT_WALK_FORWARD_STEP,
) -> dict[str, pd.DataFrame]:
    """
    Expanding-window walk-forward test focused on the model family that has worked:
    AROnly, plus simple baselines.

    For each horizon:
    - Train only on data available before the test row.
    - Select AROnly features using train-only correlations.
    - Fit Ridge/Lasso/ElasticNet.
    - Predict one future observation.
    - Step forward by `step` months.

    This gives a more realistic out-of-sample test than one 80/20 split.
    """
    x_ar, feature_map_ar = make_ar_feature_matrix(yoy, mom)
    target_yoy_now = yoy[TARGET_SERIES]

    prediction_rows = []

    for horizon in FORECAST_HORIZONS:
        print(f"Running walk-forward backtest h={horizon}", flush=True)

        y_future = target_yoy_now.shift(-horizon).rename(f"{TARGET_SERIES}_YOY_h{horizon}")

        supervised = pd.concat(
            [
                y_future,
                target_yoy_now.rename("target_yoy_now"),
                target_yoy_now.rolling(3).mean().rename("target_yoy_roll3"),
                target_yoy_now.rolling(12).mean().rename("target_yoy_roll12"),
                x_ar,
            ],
            axis=1,
        ).dropna(subset=[y_future.name])

        if len(supervised) < min_train_months + 24:
            print(f"Skipping walk-forward h={horizon}: insufficient rows ({len(supervised)})", flush=True)
            continue

        feature_cols = list(x_ar.columns)

        for test_pos in range(min_train_months, len(supervised), step):
            train = supervised.iloc[:test_pos].copy()
            test = supervised.iloc[[test_pos]].copy()

            test_date = test.index[0]

            y_train = train[y_future.name]
            y_test = float(test[y_future.name].iloc[0])

            # Baselines
            baseline_values = {
                "Baseline_LastKnownYoY": test["target_yoy_now"].iloc[0],
                "Baseline_Rolling3M": test["target_yoy_roll3"].iloc[0],
                "Baseline_Rolling12M": test["target_yoy_roll12"].iloc[0],
            }

            for model_name, pred in baseline_values.items():
                prediction_rows.append(
                    {
                        "date": test_date,
                        "horizon": horizon,
                        "model": model_name,
                        "feature_set": "Baseline",
                        "actual": y_test,
                        "predicted": pred,
                        "train_end": train.index.max(),
                        "n_train": len(train),
                    }
                )

            x_train_all = train[feature_cols]
            selected_cols, selected_train_corr = select_features_train_only(
                x_train_all=x_train_all,
                y_train=y_train,
                feature_map=feature_map_ar,
                max_features=min(max_model_features, 20),
            )

            if not selected_cols:
                continue

            x_train = x_train_all[selected_cols]
            x_test = test[selected_cols]

            for base_model_name in ["Ridge", "Lasso", "ElasticNet"]:
                display_model_name = f"{base_model_name}_AROnly"
                try:
                    pipe = fit_linear_model(base_model_name, x_train, y_train)
                    pred = float(pipe.predict(x_test)[0])

                    prediction_rows.append(
                        {
                            "date": test_date,
                            "horizon": horizon,
                            "model": display_model_name,
                            "feature_set": "AROnly",
                            "actual": y_test,
                            "predicted": pred,
                            "train_end": train.index.max(),
                            "n_train": len(train),
                            "n_features": len(selected_cols),
                            "nonzero_features": int((model_coefficients(pipe, selected_cols).abs() > 1e-10).sum()),
                            "alpha": model_alpha(pipe),
                            "l1_ratio": model_l1_ratio(pipe),
                        }
                    )
                except Exception as exc:
                    print(f"Walk-forward model failed h={horizon}, date={test_date}, model={display_model_name}: {exc}", flush=True)

    wf_predictions = pd.DataFrame(prediction_rows)
    wf_summary = evaluate_walk_forward_results(wf_predictions)

    return {
        "walk_forward_predictions": wf_predictions,
        "walk_forward_summary": wf_summary,
    }


def build_chart_data(predictions: pd.DataFrame, best_model_summary: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a compact Actual-vs-Forecast chart data table for the recommended model
    at each horizon using the regular 80/20 predictions.
    """
    if predictions.empty or best_model_summary.empty:
        return pd.DataFrame()

    pieces = []

    for _, row in best_model_summary.iterrows():
        horizon = int(row["horizon"])
        model = row["best_model"]

        subset = predictions[
            (predictions["horizon"] == horizon)
            & (predictions["model"] == model)
        ][["date", "horizon", "model", "sample", "actual", "predicted"]].copy()

        pieces.append(subset)

    if not pieces:
        return pd.DataFrame()

    return pd.concat(pieces, ignore_index=True).sort_values(["horizon", "date"])



def build_forecast_experiment(
    yoy: pd.DataFrame,
    mom: pd.DataFrame,
    inventory: pd.DataFrame,
    max_model_features: int,
) -> dict[str, pd.DataFrame]:
    """
    Forecast experiment with aligned latest forecasts.

    Model families:
    1. Baselines
    2. IndicatorsOnly
    3. AROnly
    4. ARPlusIndicators

    Latest forecasts now include:
    - common_origin: latest month where TRUCKD11 is known, so all model families are comparable
    - feature_origin: latest month where selected model features exist, useful for nowcasting when indicators publish faster
    """
    candidate_ids = [c for c in yoy.columns if c != TARGET_SERIES]

    x_ind, feature_map_ind = make_lagged_feature_matrix(yoy, candidate_ids)
    feature_map_ind["feature_set"] = "IndicatorsOnly"

    x_ar, feature_map_ar = make_ar_feature_matrix(yoy, mom)

    x_combo = pd.concat([x_ar, x_ind], axis=1, sort=True)
    feature_map_combo = pd.concat([feature_map_ar, feature_map_ind], ignore_index=True, sort=False)
    feature_map_combo["feature_set"] = feature_map_combo["feature_set"].fillna("ARPlusIndicators")

    feature_sets = {
        "IndicatorsOnly": (x_ind, feature_map_ind),
        "AROnly": (x_ar, feature_map_ar),
        "ARPlusIndicators": (x_combo, feature_map_combo),
    }

    target_yoy_now = yoy[TARGET_SERIES]
    common_origin = target_yoy_now.dropna().index.max()

    model_comparison_rows = []
    selected_feature_rows = []
    prediction_rows = []
    latest_forecast_rows = []
    train_corr_rows = []

    metadata = inventory.set_index("id", drop=False)

    for horizon in FORECAST_HORIZONS:
        print(f"Running forecast horizon h={horizon}", flush=True)

        y_future = target_yoy_now.shift(-horizon).rename(f"{TARGET_SERIES}_YOY_h{horizon}")

        baseline_frame = pd.concat(
            [
                y_future,
                target_yoy_now.rename("target_yoy_now"),
                target_yoy_now.rolling(3).mean().rename("target_yoy_roll3"),
                target_yoy_now.rolling(12).mean().rename("target_yoy_roll12"),
            ],
            axis=1,
        ).dropna(subset=[y_future.name])

        if len(baseline_frame) < 90:
            print(f"Skipping horizon {horizon}: insufficient rows ({len(baseline_frame)})", flush=True)
            continue

        split_idx = int(len(baseline_frame) * 0.80)
        train_idx = baseline_frame.index[:split_idx]
        test_idx = baseline_frame.index[split_idx:]

        y_train = baseline_frame.loc[train_idx, y_future.name]
        y_test = baseline_frame.loc[test_idx, y_future.name]

        baselines = {
            "Baseline_LastKnownYoY": baseline_frame["target_yoy_now"],
            "Baseline_Rolling3M": baseline_frame["target_yoy_roll3"],
            "Baseline_Rolling12M": baseline_frame["target_yoy_roll12"],
        }

        for baseline_name, pred_all in baselines.items():
            train_pred = pred_all.loc[train_idx]
            test_pred = pred_all.loc[test_idx]

            train_metrics = evaluate_predictions(
                y_train, train_pred, baseline_name, horizon, 0, 0
            )
            train_metrics["sample"] = "train"
            train_metrics["feature_set"] = "Baseline"

            test_metrics = evaluate_predictions(
                y_test, test_pred, baseline_name, horizon, 0, 0
            )
            test_metrics["sample"] = "test"
            test_metrics["feature_set"] = "Baseline"

            model_comparison_rows.extend([train_metrics, test_metrics])

            baseline_prediction_frame = pd.concat(
                [y_future.rename("actual"), pred_all.rename("predicted")], axis=1
            ).dropna()

            for dt, row in baseline_prediction_frame.iterrows():
                prediction_rows.append(
                    {
                        "date": dt,
                        "horizon": horizon,
                        "model": baseline_name,
                        "feature_set": "Baseline",
                        "actual": row["actual"],
                        "predicted": row["predicted"],
                        "sample": "train" if dt in train_idx else ("test" if dt in test_idx else "other"),
                    }
                )

            if common_origin is not None:
                if baseline_name == "Baseline_LastKnownYoY":
                    latest_pred = target_yoy_now.loc[common_origin]
                elif baseline_name == "Baseline_Rolling3M":
                    latest_pred = target_yoy_now.rolling(3).mean().loc[common_origin]
                else:
                    latest_pred = target_yoy_now.rolling(12).mean().loc[common_origin]

                latest_forecast_rows.append(
                    {
                        "forecast_origin_type": "common_origin",
                        "forecast_origin": common_origin,
                        "forecast_target_date": common_origin + pd.DateOffset(months=horizon),
                        "horizon": horizon,
                        "model": baseline_name,
                        "feature_set": "Baseline",
                        "forecast_truckd11_yoy": latest_pred,
                        "n_features": 0,
                        "nonzero_features": 0,
                        "alpha": None,
                        "l1_ratio": None,
                    }
                )

        for feature_set_name, (x_source, fmap_source) in feature_sets.items():
            supervised = pd.concat([y_future, x_source], axis=1).dropna(subset=[y_future.name])

            if len(supervised) < 90:
                print(f"Skipping {feature_set_name}, horizon {horizon}: insufficient rows", flush=True)
                continue

            split_idx = int(len(supervised) * 0.80)
            train_idx = supervised.index[:split_idx]
            test_idx = supervised.index[split_idx:]

            y_train = supervised.loc[train_idx, y_future.name]
            y_test = supervised.loc[test_idx, y_future.name]

            x_train_all = supervised.loc[train_idx, x_source.columns]
            x_test_all = supervised.loc[test_idx, x_source.columns]

            feature_cap = min(max_model_features, 20) if feature_set_name == "AROnly" else max_model_features

            selected_cols, selected_train_corr = select_features_train_only(
                x_train_all=x_train_all,
                y_train=y_train,
                feature_map=fmap_source,
                max_features=feature_cap,
            )

            if selected_train_corr.empty or not selected_cols:
                print(f"Skipping ML models for horizon {horizon}, {feature_set_name}: no selected features", flush=True)
                continue

            selected_train_corr["horizon"] = horizon
            selected_train_corr["feature_set"] = feature_set_name
            train_corr_rows.extend(selected_train_corr.to_dict("records"))

            x_train = x_train_all[selected_cols]
            x_test = x_test_all[selected_cols]

            for model_name in ["Ridge", "Lasso", "ElasticNet"]:
                display_model_name = f"{model_name}_{feature_set_name}"

                try:
                    pipe = fit_linear_model(model_name, x_train, y_train)
                    train_pred = pd.Series(pipe.predict(x_train), index=x_train.index)
                    test_pred = pd.Series(pipe.predict(x_test), index=x_test.index)

                    coefs = model_coefficients(pipe, selected_cols)
                    nonzero = int((coefs.abs() > 1e-10).sum())

                    alpha = model_alpha(pipe)
                    l1_ratio = model_l1_ratio(pipe)

                    train_metrics = evaluate_predictions(
                        y_train, train_pred, display_model_name, horizon, len(selected_cols), nonzero, alpha, l1_ratio
                    )
                    train_metrics["sample"] = "train"
                    train_metrics["feature_set"] = feature_set_name

                    test_metrics = evaluate_predictions(
                        y_test, test_pred, display_model_name, horizon, len(selected_cols), nonzero, alpha, l1_ratio
                    )
                    test_metrics["sample"] = "test"
                    test_metrics["feature_set"] = feature_set_name

                    model_comparison_rows.extend([train_metrics, test_metrics])

                    for model_feature, coef in coefs.items():
                        fm = fmap_source.loc[fmap_source["model_feature"] == model_feature]
                        if fm.empty:
                            continue

                        base_feature = fm["feature"].iloc[0]
                        lag = int(fm["lag"].iloc[0])
                        meta = metadata.loc[base_feature] if base_feature in metadata.index else pd.Series(dtype=object)

                        train_corr_row = selected_train_corr.loc[
                            selected_train_corr["model_feature"] == model_feature
                        ]
                        train_pearson = (
                            train_corr_row["train_pearson"].iloc[0]
                            if not train_corr_row.empty else np.nan
                        )

                        selected_feature_rows.append(
                            {
                                "horizon": horizon,
                                "model": display_model_name,
                                "feature_set": feature_set_name,
                                "model_feature": model_feature,
                                "feature": base_feature,
                                "series_name": meta.get("title", base_feature),
                                "category": meta.get("category", "AR" if feature_set_name != "IndicatorsOnly" else ""),
                                "naics": meta.get("naics", ""),
                                "lag": lag,
                                "train_pearson": train_pearson,
                                "coefficient": float(coef),
                                "abs_coefficient": float(abs(coef)),
                                "nonzero": bool(abs(coef) > 1e-10),
                            }
                        )

                    combined_pred = pd.concat([train_pred, test_pred]).sort_index()
                    for dt, pred in combined_pred.items():
                        prediction_rows.append(
                            {
                                "date": dt,
                                "horizon": horizon,
                                "model": display_model_name,
                                "feature_set": feature_set_name,
                                "actual": y_future.loc[dt] if dt in y_future.index else np.nan,
                                "predicted": pred,
                                "sample": "train" if dt in train_idx else ("test" if dt in test_idx else "other"),
                            }
                        )

                    # Latest forecast 1: common origin, comparable to baselines.
                    if common_origin is not None and common_origin in x_source.index:
                        common_x = x_source.loc[[common_origin], selected_cols]
                        if not common_x.isna().all(axis=1).iloc[0]:
                            common_pred = float(pipe.predict(common_x)[0])
                            latest_forecast_rows.append(
                                {
                                    "forecast_origin_type": "common_origin",
                                    "forecast_origin": common_origin,
                                    "forecast_target_date": common_origin + pd.DateOffset(months=horizon),
                                    "horizon": horizon,
                                    "model": display_model_name,
                                    "feature_set": feature_set_name,
                                    "forecast_truckd11_yoy": common_pred,
                                    "n_features": len(selected_cols),
                                    "nonzero_features": nonzero,
                                    "alpha": alpha,
                                    "l1_ratio": l1_ratio,
                                }
                            )

                    # Latest forecast 2: feature origin, useful for nowcast if indicators are newer than target.
                    feature_origin = x_source[selected_cols].dropna(how="all").index.max()
                    if feature_origin is not None and feature_origin != common_origin:
                        latest_x = x_source.loc[[feature_origin], selected_cols]
                        latest_pred = float(pipe.predict(latest_x)[0])

                        latest_forecast_rows.append(
                            {
                                "forecast_origin_type": "feature_origin",
                                "forecast_origin": feature_origin,
                                "forecast_target_date": feature_origin + pd.DateOffset(months=horizon),
                                "horizon": horizon,
                                "model": display_model_name,
                                "feature_set": feature_set_name,
                                "forecast_truckd11_yoy": latest_pred,
                                "n_features": len(selected_cols),
                                "nonzero_features": nonzero,
                                "alpha": alpha,
                                "l1_ratio": l1_ratio,
                            }
                        )

                except Exception as exc:
                    print(f"Model failed: horizon={horizon}, model={display_model_name}, error={exc}", flush=True)
                    model_comparison_rows.append(
                        {
                            "horizon": horizon,
                            "model": display_model_name,
                            "feature_set": feature_set_name,
                            "sample": "error",
                            "error": str(exc),
                        }
                    )

    model_comparison = pd.DataFrame(model_comparison_rows)
    selected_features = pd.DataFrame(selected_feature_rows)
    predictions = pd.DataFrame(prediction_rows)
    latest_forecast = pd.DataFrame(latest_forecast_rows)
    train_corr = pd.DataFrame(train_corr_rows)

    if not selected_features.empty:
        selected_features = selected_features.sort_values(
            ["horizon", "model", "nonzero", "abs_coefficient"],
            ascending=[True, True, False, False],
        )

    if not model_comparison.empty:
        sort_cols = [c for c in ["horizon", "sample", "feature_set", "model"] if c in model_comparison.columns]
        model_comparison = model_comparison.sort_values(sort_cols)

    if not latest_forecast.empty:
        latest_forecast = latest_forecast.sort_values(["forecast_origin_type", "horizon", "feature_set", "model"])

    best_model_summary = build_best_model_summary(model_comparison)
    incremental_value = build_incremental_value_test(model_comparison)
    recommended_forecasts = build_recommended_forecasts(best_model_summary, latest_forecast)
    chart_data = build_chart_data(predictions, best_model_summary)

    return {
        "model_comparison": model_comparison,
        "selected_features": selected_features,
        "predictions": predictions,
        "latest_forecast": latest_forecast,
        "train_corr": train_corr,
        "best_model_summary": best_model_summary,
        "incremental_value": incremental_value,
        "recommended_forecasts": recommended_forecasts,
        "chart_data": chart_data,
    }


# =============================================================================
# Excel output
# =============================================================================

def write_outputs(
    output_xlsx: str,
    primary_inventory: pd.DataFrame,
    full_inventory: pd.DataFrame,
    raw: pd.DataFrame,
    yoy: pd.DataFrame,
    mom: pd.DataFrame,
    full_corr: pd.DataFrame,
    forecast_outputs: dict[str, pd.DataFrame],
    walk_forward_outputs: dict[str, pd.DataFrame],
) -> None:
    readme = pd.DataFrame(
        [
            {"item": "target", "value": TARGET_SERIES},
            {"item": "forecast horizons", "value": ", ".join(map(str, FORECAST_HORIZONS))},
            {"item": "feature lags", "value": ", ".join(map(str, FEATURE_LAGS))},
            {"item": "target definition", "value": "TRUCKD11 YoY shifted forward by forecast horizon h"},
            {"item": "feature definition", "value": "Candidate YoY lags plus TRUCKD11 autoregressive YoY/MoM lags and rolling averages"},
            {"item": "model selection rule", "value": "Train-only Pearson screen; best lag per base series; top max_model_features"},
            {"item": "models", "value": "Baselines plus Ridge/Lasso/ElasticNet for IndicatorsOnly, AROnly, and ARPlusIndicators"},
            {"item": "metrics", "value": "R2, MAE, MSE, RMSE, bias, directional accuracy"},
            {"item": "new tabs", "value": "Best Model by Horizon, Incremental Value Test, Recommended Forecasts, Walk Forward Summary, Walk Forward Predictions, and Forecast Chart Data."},
            {"item": "caution", "value": "This is a statistical forecast test. Validate with out-of-sample metrics before using operationally."},
        ]
    )

    with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
        readme.to_excel(writer, sheet_name="README", index=False)
        primary_inventory.to_excel(writer, sheet_name="Inventory Primary", index=False)
        full_inventory.to_excel(writer, sheet_name="Inventory All", index=False)
        full_corr.to_excel(writer, sheet_name="Correlation Full Sample", index=False)

        forecast_outputs["train_corr"].to_excel(writer, sheet_name="Correlation Train Only", index=False)
        forecast_outputs["model_comparison"].to_excel(writer, sheet_name="Model Comparison", index=False)
        forecast_outputs["best_model_summary"].to_excel(writer, sheet_name="Best Model by Horizon", index=False)
        forecast_outputs["incremental_value"].to_excel(writer, sheet_name="Incremental Value Test", index=False)
        forecast_outputs["recommended_forecasts"].to_excel(writer, sheet_name="Recommended Forecasts", index=False)
        forecast_outputs["selected_features"].to_excel(writer, sheet_name="Selected Features", index=False)
        forecast_outputs["latest_forecast"].to_excel(writer, sheet_name="Latest Forecast", index=False)
        forecast_outputs["chart_data"].to_excel(writer, sheet_name="Forecast Chart Data", index=False)
        forecast_outputs["predictions"].to_excel(writer, sheet_name="Predictions", index=False)

        walk_forward_outputs["walk_forward_summary"].to_excel(writer, sheet_name="Walk Forward Summary", index=False)
        walk_forward_outputs["walk_forward_predictions"].to_excel(writer, sheet_name="Walk Forward Predictions", index=False)

        raw.tail(240).to_excel(writer, sheet_name="Raw Last 20Y")
        yoy.tail(240).to_excel(writer, sheet_name="YoY Last 20Y")
        mom.tail(240).to_excel(writer, sheet_name="MoM Last 20Y")

    print(f"\nWrote {output_xlsx}", flush=True)


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--output",
        default=OUTPUT_XLSX,
        help="Output Excel file name.",
    )

    parser.add_argument(
        "--cache-dir",
        default=str(OUTPUT_DATA_DIR),
        help="Folder for cached FRED series CSV files.",
    )

    parser.add_argument(
        "--max-series",
        type=int,
        default=MAX_SERIES_FIRST_TEST,
        help="Maximum primary series to download. Use 0 to disable cap.",
    )

    parser.add_argument(
        "--max-model-features",
        type=int,
        default=MAX_MODEL_FEATURES,
        help="Maximum selected model features per horizon.",
    )

    parser.add_argument(
        "--walk-forward-step",
        type=int,
        default=DEFAULT_WALK_FORWARD_STEP,
        help="Walk-forward test step size in months. Default is 3 for speed.",
    )

    parser.add_argument(
        "--min-train-months",
        type=int,
        default=DEFAULT_MIN_TRAIN_MONTHS,
        help="Minimum expanding-window training months for walk-forward backtest.",
    )

    args = parser.parse_args()

    cache_dir = Path(args.cache_dir)
    max_series = None if args.max_series == 0 else args.max_series

    primary_inventory, full_inventory = build_candidate_inventory(max_series=max_series)

    print("\nCandidate inventory:", flush=True)
    print(primary_inventory["category"].value_counts(dropna=False).to_string(), flush=True)
    print(f"\nPrimary candidate count: {len(primary_inventory)}", flush=True)
    print(f"All candidate/duplicate count: {len(full_inventory)}", flush=True)

    raw = download_series_matrix(primary_inventory, cache_dir)

    if TARGET_SERIES not in raw.columns:
        raise RuntimeError(
            f"{TARGET_SERIES} was not downloaded. Check whether the series ID is valid."
        )

    yoy = pct_change_clean(raw, 12)
    mom = pct_change_clean(raw, 1)

    full_corr = build_full_correlation_screen(raw, yoy, mom, primary_inventory)
    forecast_outputs = build_forecast_experiment(
        yoy=yoy,
        mom=mom,
        inventory=primary_inventory,
        max_model_features=args.max_model_features,
    )

    walk_forward_outputs = build_walk_forward_backtest(
        yoy=yoy,
        mom=mom,
        max_model_features=args.max_model_features,
        min_train_months=args.min_train_months,
        step=args.walk_forward_step,
    )

    write_outputs(
        output_xlsx=args.output,
        primary_inventory=primary_inventory,
        full_inventory=full_inventory,
        raw=raw,
        yoy=yoy,
        mom=mom,
        full_corr=full_corr,
        forecast_outputs=forecast_outputs,
        walk_forward_outputs=walk_forward_outputs,
    )

    print("\nTop model comparison rows:", flush=True)
    model_comp = forecast_outputs["model_comparison"]
    if not model_comp.empty:
        cols = [c for c in ["horizon", "sample", "model", "r2", "mae", "rmse", "directional_accuracy", "n_features", "nonzero_features"] if c in model_comp.columns]
        print(model_comp[cols].head(40).to_string(index=False), flush=True)

    print("\nBest model by horizon:", flush=True)
    best = forecast_outputs.get("best_model_summary", pd.DataFrame())
    if not best.empty:
        print(best.to_string(index=False), flush=True)

    print("\nIncremental value test:", flush=True)
    inc = forecast_outputs.get("incremental_value", pd.DataFrame())
    if not inc.empty:
        print(inc.to_string(index=False), flush=True)

    print("\nRecommended forecasts:", flush=True)
    rec = forecast_outputs.get("recommended_forecasts", pd.DataFrame())
    if not rec.empty:
        print(rec.to_string(index=False), flush=True)

    print("\nWalk-forward summary:", flush=True)
    wf = walk_forward_outputs.get("walk_forward_summary", pd.DataFrame())
    if not wf.empty:
        print(wf.to_string(index=False), flush=True)

    print("\nLatest forecasts:", flush=True)
    latest = forecast_outputs["latest_forecast"]
    if not latest.empty:
        print(latest.to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
