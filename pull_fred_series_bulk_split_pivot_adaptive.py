# pull_fred_series_bulk_split_pivot_adaptive.py
import os, time, re, datetime, random
import pandas as pd
from fredapi import Fred

# ------------------ TIMER START ------------------
t0 = time.time()

# ------------------ CONFIG ------------------
START_DATE   = "2016-01-01"
BASE_YEAR    = 2019
OUTPUT_XLSX  = "fred_series_2019base.xlsx"

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
    "IPG333N":   "IP: Machinery Manufacturing",
    "IPG334N":   "IP: Computer & Electronic Products",
    "IPG335S":   "IP: Electrical Equipment, Appliances",
    "IPG3361T3S":"IP: Motor Vehicles & Parts (3361–3363)",
    "IPG3363S":  "IP: Motor Vehicle Parts",
    "IPG337N":   "IP: Furniture & Related",
    "IPG339N":   "IP: Miscellaneous Manufacturing",
    "IPG332S":   "IP: Fabricated Metal Products",
    "IPG321S":   "IP: Wood Products",
    "IPN3311A2RS":"IP: Primary Metal Industries (Real)",
    "IPG3311A2S":"IP: Primary Metal Industries",
    "IPG313S":   "IP: Textile Mills",
    "IPG314S":   "IP: Textile Product Mills",

    # --- Producer Prices (PPI) ---
    "WPU0221":         "PPI: Gasoline (Commodity)",
    "PCU325325":       "PPI Industry: Chemical Mfg",
    "PCU325412325412": "PPI Industry: Pharma Prep Mfg",
    "PCU325620325620": "PPI Industry: Toilet Prep Mfg",
    "PCU484121484121": "PPI Industry: General Freight Trucking, Long-Distance TL",
    "WPU057303":       "PPI Commodity: No. 2 Diesel Fuel",
    "PCU336120336120": "PPI Industry: Heavy Duty Truck Manufacturing",  # monthly replacement for WPU141201
    "WPU141302":       "PPI Commodity: Motor Vehicle Parts",
    "WPU02":           "PPI Commodity: Processed Foods & Feeds",

    # --- Retail / Wholesale / Sales ---
    "RSAFS":        "Retail & Food Services Sales (SA)",
    "RSNSR":        "Retail & Food Services (NSA)",
    "MRTSSM4541US": "Retail Sales: Nonstore Retailers (SA, Monthly) — e-commerce proxy",  # monthly replacement for ECOMSA
    "RETAILIRSA":   "Retail Inventories/Sales Ratio (SA)",
    "WHLSLRIRSA":   "Wholesale Inventories/Sales Ratio (SA)",
    "BUSINV":       "Total Business Inventories",
    "ISRATIO":      "Total Business Inventories-to-Sales Ratio",
    "WHLSLRSMSA":   "Merchant Wholesalers Sales: Total (SA, Monthly)",
    "RSFSXMV":      "Retail Sales: Furniture/Electronics/Appliances (SA, Monthly)",
    "RETAILIMSA":   "Retailers: Inventories (SA, Monthly)",
    "R423IRM163SCEN":"Inventories/Sales Ratio: Wholesalers, Durable (SA, Monthly)",

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

# Big ID list (deduped by clean_ids)
SERIES_IDS_RAW = """
# --- Core PPIs / Costs ---
PCU484121484121
WPU057303
PCU336120336120
WPU141302
WPU02

# --- Inventories / Retail / Wholesale ---
BUSINV
ISRATIO
WHLSLRSMSA
RSFSXMV
RETAILIMSA
R423IRM163SCEN
RETAILIRSA
MRTSSM4541US
RSAFS
RSNSR

# --- Imports / PCE ---
IMP0004
DGDSRX1

# --- Orders / Housing / Sentiment ---
AMTMNO
NEWORDER
DGORDER
PERMIT1
PERMIT5
HOUST
UMCSENT

# --- Industrial Production (IP) ---
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
IPG333N
IPG334N
IPG335S
IPG3361T3S
IPG3363S
IPG337N
IPG339N
IPG332S
IPG321S
IPN3311A2RS
IPG3311A2S
IPG313S
IPG314S

# --- Capacity Utilization ---
CUMFNS
CAPUTLG3311A2S
CAPUTLG311S
CAPUTLG312S
CAPUTLG325S
CAPUTLG326S

# --- Vehicles / Assemblies ---
MVAAUTLTTS
HTRUCKSSAAR

# --- Freight / Transport ---
TRUCKD11
FRGSHPUSM649NCIS
FRGEXPUSM649NCIS

# --- Labor / Wages ---
CES4300000003
LNU04032231

# --- Leading Indicators ---
CFNAI
CFNAIMA3
""".strip()

# ------------------ HELPERS ------------------
def clean_ids(multiline_text: str):
    lines = [ln.strip().upper() for ln in multiline_text.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    unique, seen = [], set()
    for s in lines:
        if s not in seen:
            seen.add(s)
            unique.append(s)
    dup_count = len(lines) - len(unique)
    if dup_count:
        print(f"ℹ️ Removed {dup_count} duplicate IDs before pulling.")
    return unique

def polite_pause():
    # Use adaptive pacer (no fixed constant)
    pacer.sleep()

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

            # Detect throttling
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

# ------------------ WRITE EXCEL ------------------
with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as xw:
    long_df.to_excel(xw, sheet_name="Series_Long", index=False)
    wide_idx.to_excel(xw, sheet_name="Wide_Index2019")
    latest_df.to_excel(xw, sheet_name="Latest_Dates", index=False)
    meta_df.to_excel(xw, sheet_name="Metadata", index=False)   # includes full Notes
    failed_df.to_excel(xw, sheet_name="Failed", index=False)
    # Family split
    for fam, fam_df in long_df.groupby("family"):
        fam_df.sort_values(["series_id","date"]).to_excel(xw, sheet_name=fam[:31], index=False)

# ------------------ TIMER END ------------------
elapsed = time.time() - t0
print(f"✅ Attempted {len(final_map)} unique series; saved {long_df['series_id'].nunique()} series to {OUTPUT_XLSX}.")
if not failed_df.empty:
    print(f"⚠️ {len(failed_df)} series failed (see 'Failed' sheet).")

print(f"⏱️ Total runtime: {elapsed:.1f} seconds ({elapsed/60:.2f} minutes) | {datetime.timedelta(seconds=round(elapsed))}")



