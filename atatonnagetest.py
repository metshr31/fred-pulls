import argparse
import os
import re
import time
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
from fredapi import Fred

from sklearn.impute import SimpleImputer
from sklearn.linear_model import RidgeCV
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


# =============================================================================
# FRED API KEY — GitHub-safe
# =============================================================================

FRED_API_KEY = os.environ.get("FRED_API_KEY")
if not FRED_API_KEY:
    raise RuntimeError(
        "FRED_API_KEY env var not set. "
        "Define it in GitHub Secrets or your shell."
    )

fred = Fred(api_key=FRED_API_KEY)


# =============================================================================
# Settings
# =============================================================================

FRED_BASE = "https://api.stlouisfed.org/fred"

TARGET_SERIES = "TRUCKD11"
LAGS = [0, 1, 2, 3, 6, 9, 12]

MIN_CORR_OBS = 36
MIN_RIDGE_OBS = 72

# First-test default. Raise to 500 after you confirm the GitHub run works.
RIDGE_MAX_FEATURES = 150

OUTPUT_XLSX = "ata_truck_tonnage_feature_screen.xlsx"
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


# =============================================================================
# FRED metadata helpers
# =============================================================================

def fred_get_json(endpoint: str, **params) -> dict:
    url = f"{FRED_BASE}/{endpoint}"
    params = {**params, "api_key": FRED_API_KEY, "file_type": "json"}

    last_error = None
    for attempt in range(REQUEST_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=30)

            if response.status_code == 429:
                time.sleep(3 + attempt * 3)
                continue

            response.raise_for_status()
            time.sleep(REQUEST_SLEEP_SECONDS)
            return response.json()

        except Exception as exc:
            last_error = exc
            time.sleep(1 + attempt * 2)

    raise RuntimeError(
        f"FRED API request failed: endpoint={endpoint}, params={params}, error={last_error}"
    )


def get_series_info(series_id: str) -> Optional[dict]:
    try:
        data = fred_get_json("series", series_id=series_id)
        rows = data.get("seriess", [])
        return rows[0] if rows else None
    except Exception:
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


def build_candidate_inventory() -> Tuple[pd.DataFrame, pd.DataFrame]:
    print("Finding G.17 release ID...")
    g17_release_id = find_g17_release_id()
    print(f"G.17 release_id = {g17_release_id}")

    print("Downloading G.17 release series inventory...")
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

    return primary_inventory, full_inventory


# =============================================================================
# Data download and transforms
# =============================================================================

def download_one_series(series_id: str) -> pd.Series:
    """
    Download one FRED series with explicit 429/rate-limit backoff.

    Do NOT use fred.get_series() here. fredapi is convenient, but it does not
    expose enough retry/backoff control for this broad 300-series batch pull.
    """
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

            if "too many requests" in msg or "rate limit" in msg or "429" in msg:
                wait_seconds = min(120, 10 * (attempt + 1))
                print(
                    f"  Rate limited on {series_id}; waiting {wait_seconds}s "
                    f"before retry {attempt + 1}/{REQUEST_RETRIES}"
                )
                time.sleep(wait_seconds)
            else:
                wait_seconds = min(60, 3 * (attempt + 1))
                print(
                    f"  Error on {series_id}: {exc}; waiting {wait_seconds}s "
                    f"before retry {attempt + 1}/{REQUEST_RETRIES}"
                )
                time.sleep(wait_seconds)

    raise RuntimeError(f"Failed downloading {series_id} after retries: {last_error}")


def download_series_matrix(inventory: pd.DataFrame, cache_dir: Path) -> pd.DataFrame:
    cache_dir.mkdir(parents=True, exist_ok=True)

    series_ids = inventory["id"].astype(str).tolist()
    all_series = []

    for index, series_id in enumerate(series_ids, start=1):
        print(f"[{index}/{len(series_ids)}] Downloading {series_id}")

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
            print(f"WARNING: failed downloading {series_id}: {exc}")

        # Be polite to FRED. This is what prevents the 291/300 rate-limit failure.
        time.sleep(FRED_DOWNLOAD_SLEEP_SECONDS)

    if not all_series:
        raise RuntimeError("No series downloaded.")

    raw = pd.concat(all_series, axis=1).sort_index()
    raw.index = pd.to_datetime(raw.index).to_period("M").to_timestamp("MS")
    raw = raw.groupby(raw.index).last().sort_index()

    return raw


def pct_change_clean(df: pd.DataFrame, periods: int) -> pd.DataFrame:
    return df.pct_change(periods=periods, fill_method=None).replace(
        [np.inf, -np.inf],
        np.nan,
    )


# =============================================================================
# Correlation screen
# =============================================================================

def pearson_pair(
    x: pd.Series,
    y: pd.Series,
    min_obs: int = MIN_CORR_OBS,
) -> Tuple[float, int]:
    paired = pd.concat([x, y], axis=1).dropna()
    observation_count = len(paired)

    if observation_count < min_obs:
        return np.nan, observation_count

    return float(paired.iloc[:, 0].corr(paired.iloc[:, 1])), observation_count


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


def build_correlation_screen(
    raw: pd.DataFrame,
    yoy: pd.DataFrame,
    mom: pd.DataFrame,
    inventory: pd.DataFrame,
) -> pd.DataFrame:
    if TARGET_SERIES not in raw.columns:
        raise RuntimeError(f"Target {TARGET_SERIES} was not downloaded.")

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

        for lag in LAGS:
            # Candidate leads target.
            # lag 3 means corr(candidate[t], target[t+3]).
            corr, count = pearson_pair(target_yoy.shift(-lag), yoy[series_id])
            lag_corrs[lag] = corr
            lag_counts[lag] = count

        valid_lags = {
            lag: corr
            for lag, corr in lag_corrs.items()
            if not pd.isna(corr)
        }

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

        exclude_2020_2021_mask = ~(
            (yoy.index >= "2020-01-01")
            & (yoy.index <= "2021-12-31")
        )

        best_corr_ex_2020_2021 = np.nan
        abs_best_corr_ex_2020_2021 = np.nan

        if not pd.isna(best_lag):
            ex_corr, _ = pearson_pair(
                target_yoy.shift(-int(best_lag)).loc[exclude_2020_2021_mask],
                yoy[series_id].loc[exclude_2020_2021_mask],
            )
            best_corr_ex_2020_2021 = ex_corr
            abs_best_corr_ex_2020_2021 = abs(ex_corr) if not pd.isna(ex_corr) else np.nan

        relationship_weakened = (
            abs_best_corr - abs_best_corr_ex_2020_2021
            if not pd.isna(abs_best_corr) and not pd.isna(abs_best_corr_ex_2020_2021)
            else np.nan
        )

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
                "best_yoy_corr_ex_2020_2021": best_corr_ex_2020_2021,
                "abs_best_yoy_corr_ex_2020_2021": abs_best_corr_ex_2020_2021,
                "relationship_weakened_ex_2020_2021": relationship_weakened,
                "keeper_class": keeper_class(abs_best_corr),
                "flag": "",
            }
        )

    output = pd.DataFrame(rows)

    if output.empty:
        return output

    output = output.sort_values(
        ["abs_best_yoy_corr", "feature"],
        ascending=[False, True],
    ).reset_index(drop=True)

    flags = []

    for _, row in output.iterrows():
        row_flags = []

        if (
            pd.notna(row["raw_corr"])
            and pd.notna(row["abs_best_yoy_corr"])
            and abs(row["raw_corr"]) >= 0.80
            and row["abs_best_yoy_corr"] < 0.30
        ):
            row_flags.append("High raw correlation likely trend-driven")

        if (
            pd.notna(row["relationship_weakened_ex_2020_2021"])
            and row["relationship_weakened_ex_2020_2021"] >= 0.15
        ):
            row_flags.append("Relationship weakens materially excluding 2020-2021")

        if row["category"] in {"retail_freight_comparison", "capacity_utilization"}:
            row_flags.append("Use as comparison / context")

        if (
            pd.notna(row["best_lag"])
            and int(row["best_lag"]) in {3, 6, 9, 12}
            and pd.notna(row["abs_best_yoy_corr"])
            and row["abs_best_yoy_corr"] >= 0.30
        ):
            row_flags.append("Potentially useful lagged signal")

        flags.append("; ".join(row_flags))

    output["flag"] = flags

    return output


# =============================================================================
# Ridge model
# =============================================================================

def make_lagged_feature_matrix(
    yoy: pd.DataFrame,
    candidate_ids: List[str],
) -> Tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    target = yoy[TARGET_SERIES].copy()

    feature_parts = []
    feature_map_rows = []

    for series_id in candidate_ids:
        if series_id == TARGET_SERIES or series_id not in yoy.columns:
            continue

        for lag in LAGS:
            column_name = f"{series_id}__lag{lag}"

            # For target at month t, lag 3 uses feature at t-3.
            # This is the correct predictive use of a feature that leads target by 3 months.
            feature_parts.append(
                yoy[series_id].shift(lag).rename(column_name)
            )

            feature_map_rows.append(
                {
                    "model_feature": column_name,
                    "feature": series_id,
                    "lag": lag,
                }
            )

    x_matrix = pd.concat(feature_parts, axis=1) if feature_parts else pd.DataFrame(index=yoy.index)
    feature_map = pd.DataFrame(feature_map_rows)

    return x_matrix, target, feature_map


def build_ridge_model(
    yoy: pd.DataFrame,
    corr_screen: pd.DataFrame,
    inventory: pd.DataFrame,
    max_features: int = RIDGE_MAX_FEATURES,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    candidate_ids = corr_screen["feature"].astype(str).tolist()

    x_all, y, feature_map = make_lagged_feature_matrix(yoy, candidate_ids)

    valid_y = y.dropna()
    x_all = x_all.loc[valid_y.index]
    y = valid_y

    pearson_rows = []

    for column in x_all.columns:
        pearson, obs_count = pearson_pair(y, x_all[column], min_obs=MIN_CORR_OBS)

        pearson_rows.append(
            (
                column,
                pearson,
                obs_count,
                abs(pearson) if not pd.isna(pearson) else np.nan,
            )
        )

    pearson_df = pd.DataFrame(
        pearson_rows,
        columns=["model_feature", "pearson", "obs", "abs_pearson"],
    )

    pearson_df = pearson_df[pearson_df["obs"] >= MIN_RIDGE_OBS].copy()
    pearson_df = pearson_df.sort_values("abs_pearson", ascending=False)

    if max_features and len(pearson_df) > max_features:
        pearson_df = pearson_df.head(max_features).copy()

    selected_columns = pearson_df["model_feature"].tolist()
    x = x_all[selected_columns].copy()

    row_nonmissing_count = x.notna().sum(axis=1)
    minimum_row_features = max(3, min(10, int(len(selected_columns) * 0.05)))
    keep_rows = row_nonmissing_count >= minimum_row_features

    x = x.loc[keep_rows]
    y = y.loc[keep_rows]

    if len(y) < 60 or x.shape[1] == 0:
        raise RuntimeError(
            f"Not enough data for ridge model: rows={len(y)}, cols={x.shape[1]}"
        )

    split_index = int(len(y) * 0.80)

    x_train = x.iloc[:split_index]
    x_test = x.iloc[split_index:]

    y_train = y.iloc[:split_index]
    y_test = y.iloc[split_index:]

    n_splits = min(5, max(2, len(y_train) // 36))
    ts_cv = TimeSeriesSplit(n_splits=n_splits)

    alphas = np.logspace(-4, 4, 80)

    model = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("ridge", RidgeCV(alphas=alphas, cv=ts_cv)),
        ]
    )

    model.fit(x_train, y_train)

    train_predictions = pd.Series(
        model.predict(x_train),
        index=x_train.index,
        name="predicted",
    )

    test_predictions = pd.Series(
        model.predict(x_test),
        index=x_test.index,
        name="predicted",
    )

    def rmse(y_true: pd.Series, y_pred: pd.Series) -> float:
        return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))

    metrics = pd.DataFrame(
        [
            {
                "target": TARGET_SERIES,
                "model": "RidgeCV_TimeSeriesSplit",
                "n_train": len(y_train),
                "n_test": len(y_test),
                "n_features": x.shape[1],
                "cv_splits": n_splits,
                "alpha": float(model.named_steps["ridge"].alpha_),
                "train_r2": r2_score(y_train, train_predictions),
                "test_r2": r2_score(y_test, test_predictions) if len(y_test) >= 2 else np.nan,
                "train_mae": mean_absolute_error(y_train, train_predictions),
                "test_mae": mean_absolute_error(y_test, test_predictions) if len(y_test) >= 1 else np.nan,
                "train_rmse": rmse(y_train, train_predictions),
                "test_rmse": rmse(y_test, test_predictions) if len(y_test) >= 1 else np.nan,
                "train_start": y_train.index.min(),
                "train_end": y_train.index.max(),
                "test_start": y_test.index.min() if len(y_test) else pd.NaT,
                "test_end": y_test.index.max() if len(y_test) else pd.NaT,
            }
        ]
    )

    imputed_x = model.named_steps["imputer"].transform(x)
    scaled_x = model.named_steps["scaler"].transform(imputed_x)
    coefficients = model.named_steps["ridge"].coef_

    contributions = pd.DataFrame(
        scaled_x * coefficients,
        index=x.index,
        columns=x.columns,
    )

    latest_date = contributions.index.max()
    latest_contributions = contributions.loc[latest_date]

    metadata = inventory.set_index("id", drop=False)

    feature_map = feature_map.set_index("model_feature").loc[x.columns].reset_index()
    feature_map = feature_map.merge(
        pearson_df[["model_feature", "pearson", "obs"]],
        on="model_feature",
        how="left",
    )

    rows = []

    for _, row in feature_map.iterrows():
        model_feature = row["model_feature"]
        series_id = row["feature"]
        lag = int(row["lag"])

        coefficient = float(coefficients[list(x.columns).index(model_feature)])

        meta = metadata.loc[series_id] if series_id in metadata.index else pd.Series(dtype=object)

        last_contribution = float(latest_contributions[model_feature])

        if last_contribution > 0:
            contribution_sign = "positive"
        elif last_contribution < 0:
            contribution_sign = "negative"
        else:
            contribution_sign = "zero"

        rows.append(
            {
                "model_feature": model_feature,
                "feature": series_id,
                "series_name": meta.get("title", ""),
                "category": meta.get("category", ""),
                "naics": meta.get("naics", ""),
                "lag": lag,
                "pearson": row["pearson"],
                "paired_obs": row["obs"],
                "ridge_coef": coefficient,
                "mean_abs_contrib": float(contributions[model_feature].abs().mean()),
                "last_contrib": last_contribution,
                "sign_of_latest_contribution": contribution_sign,
                "latest_available_date": latest_date,
            }
        )

    ridge_output = pd.DataFrame(rows)

    ridge_output["abs_coef"] = ridge_output["ridge_coef"].abs()
    ridge_output["abs_coef_rank"] = (
        ridge_output["abs_coef"]
        .rank(ascending=False, method="dense")
        .astype(int)
    )

    ridge_output = ridge_output.sort_values(
        ["mean_abs_contrib", "abs_coef"],
        ascending=False,
    ).reset_index(drop=True)

    all_predictions = pd.concat([train_predictions, test_predictions]).sort_index()

    predictions = pd.DataFrame(
        {
            "actual": y,
            "predicted": all_predictions,
            "sample": [
                "train" if idx <= y_train.index.max() else "test"
                for idx in y.index
            ],
        }
    )

    metrics.attrs["predictions"] = predictions

    return ridge_output, metrics


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
    corr: pd.DataFrame,
    ridge: pd.DataFrame,
    metrics: pd.DataFrame,
) -> None:
    readme = pd.DataFrame(
        [
            {"item": "target", "value": TARGET_SERIES},
            {"item": "lags", "value": ", ".join(map(str, LAGS))},
            {"item": "raw transform", "value": "Raw index level"},
            {"item": "YoY transform", "value": "current month / same month one year earlier - 1"},
            {"item": "MoM transform", "value": "current month / prior month - 1"},
            {"item": "correlation lag definition", "value": "candidate leads target; yoy_lag3 = corr(TRUCKD11_YOY at t+3, candidate_YOY at t)"},
            {"item": "ridge leakage rule", "value": "For target at t, candidate lag L uses candidate value at t-L"},
            {"item": "ridge cross-validation", "value": "RidgeCV uses TimeSeriesSplit, not random or ordinary K-fold CV"},
            {"item": "keeper classes", "value": "Strong >=0.60; Useful 0.45-0.60; Maybe 0.30-0.45; Weak <0.30"},
            {"item": "first-test setting", "value": "RIDGE_MAX_FEATURES defaults to 150. Raise to 500 after confirming GitHub Actions succeeds."},
        ]
    )

    with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
        readme.to_excel(writer, sheet_name="README", index=False)
        primary_inventory.to_excel(writer, sheet_name="Inventory Primary", index=False)
        full_inventory.to_excel(writer, sheet_name="Inventory All", index=False)
        corr.to_excel(writer, sheet_name="Correlation Screen", index=False)
        ridge.to_excel(writer, sheet_name="Ridge Output", index=False)
        metrics.to_excel(writer, sheet_name="Ridge Metrics", index=False)

        predictions = metrics.attrs.get("predictions")

        if isinstance(predictions, pd.DataFrame):
            predictions.to_excel(writer, sheet_name="Ridge Predictions")

        raw.tail(240).to_excel(writer, sheet_name="Raw Last 20Y")
        yoy.tail(240).to_excel(writer, sheet_name="YoY Last 20Y")
        mom.tail(240).to_excel(writer, sheet_name="MoM Last 20Y")

    print(f"\nWrote {output_xlsx}")


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
        "--ridge-max-features",
        type=int,
        default=RIDGE_MAX_FEATURES,
        help="Maximum lagged features to include in ridge model after Pearson pre-screen.",
    )

    args = parser.parse_args()

    cache_dir = Path(args.cache_dir)

    primary_inventory, full_inventory = build_candidate_inventory()

    print("\nCandidate inventory:")
    print(primary_inventory["category"].value_counts(dropna=False).to_string())

    print(f"\nPrimary candidate count: {len(primary_inventory)}")
    print(f"All candidate/duplicate count: {len(full_inventory)}")

    raw = download_series_matrix(primary_inventory, cache_dir)

    if TARGET_SERIES not in raw.columns:
        raise RuntimeError(
            f"{TARGET_SERIES} was not downloaded. "
            "Check whether the series ID is valid and accessible with your FRED API key."
        )

    yoy = pct_change_clean(raw, 12)
    mom = pct_change_clean(raw, 1)

    corr = build_correlation_screen(raw, yoy, mom, primary_inventory)

    try:
        ridge, metrics = build_ridge_model(
            yoy=yoy,
            corr_screen=corr,
            inventory=primary_inventory,
            max_features=args.ridge_max_features,
        )
    except Exception as exc:
        print(f"WARNING: Ridge model failed: {exc}")
        ridge = pd.DataFrame([{"error": str(exc)}])
        metrics = pd.DataFrame([{"error": str(exc)}])

    write_outputs(
        output_xlsx=args.output,
        primary_inventory=primary_inventory,
        full_inventory=full_inventory,
        raw=raw,
        yoy=yoy,
        mom=mom,
        corr=corr,
        ridge=ridge,
        metrics=metrics,
    )

    print("\nTop 25 correlation screen:")

    corr_cols = [
        "feature",
        "series_name",
        "category",
        "best_lag",
        "best_yoy_corr",
        "abs_best_yoy_corr",
        "keeper_class",
        "flag",
    ]

    print(corr[corr_cols].head(25).to_string(index=False))

    if not ridge.empty and "error" not in ridge.columns:
        print("\nTop 25 ridge contributions:")

        ridge_cols = [
            "feature",
            "series_name",
            "lag",
            "pearson",
            "ridge_coef",
            "abs_coef_rank",
            "mean_abs_contrib",
            "last_contrib",
            "sign_of_latest_contribution",
            "latest_available_date",
        ]

        print(ridge[ridge_cols].head(25).to_string(index=False))


if __name__ == "__main__":
    main()
