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
    "IPG313S":   "IP: Textile Mills",
    "IPG314S":   "IP: Textile Product Mills",

    # --- Producer Prices (PPI) ---
    "WPU0221":        "PPI: Gasoline (Commodity)",
    "PCU325325":      "PPI Industry: Chemical Mfg",
    "PCU325412325412":"PPI Industry: Pharma Prep Mfg",
    "PCU325620325620":"PPI Industry: Toilet Prep Mfg",

    # --- Retail / Wholesale / Sales ---
    "RSAFS":      "Retail & Food Services Sales (SA)",
    "RSNSR":      "Retail & Food Services (NSA)",
    "ECOMSA":     "E-commerce Retail Sales (SA, quarterly)",
    "RETAILIRSA": "Retail Inventories/Sales Ratio (SA)",
    "WHLSLRIRSA": "Wholesale Inventories/Sales Ratio (SA)",

    # --- Orders / Housing / Sentiment ---
    "DGORDER": "Durable Goods Orders (NSA)",
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

    # --- Freight / Transport ---
    "TRUCKD11":          "ATA Truck Tonnage (SA)",
    "FRGSHPUSM649NCIS":  "Cass Freight Shipments (NSA via FRED)",
    "FRGEXPUSM649NCIS":  "Cass Freight Expenditures (NSA via FRED)",
}

# Big unlabelled list — now explicitly includes the IDs above to ensure they’re pulled too.
# Duplicates are fine; clean_ids() will dedupe.
SERIES_IDS_RAW = """
# === Ensure curated groups are included ===
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
IPG313S
IPG314S

WPU0221
PCU325325
PCU325412325412
PCU325620325620

RSAFS
RSNSR
ECOMSA
RETAILIRSA
WHLSLRIRSA

DGORDER
PERMIT1
PERMIT5
HOUST
UMCSENT

CUMFNS
CAPUTLG3311A2S
CAPUTLG311S
CAPUTLG312S
CAPUTLG325S
CAPUTLG326S

MVAAUTLTTS

TRUCKD11
FRGSHPUSM649NCIS
FRGEXPUSM649NCIS

# === Your long industrial list continues here (kept as-is) ===
IPN1133S
IPG211S
IPG212S
IPG213S
IPG2122S
IPG2123S
IPN2121S
IPG21222S
IPG21223S
IPG21223S
IPN21221S
IPG21113S
IPN213111S
IPG21112S
IPG311S
IPG3111S
IPG3112S
IPG3113S
IPG3114S
IPG3116S
IPG3119S
IPN3118S
IPG31151S
IPG31192S
IPN31152S
IPN311511S
IPN311512S
IPN311513S
IPN311514S
IPN311615S
IPG311611T3S
IPN311611T3BS
IPN311611T3PS
IPN311611T3ZS
IPG312S
IPG3121S
IPG3122S
IPN31211S
IPN31212S
IPG313S
IPG3132S
IPG3133S
IPG314S
IPG3141S
IPG3149S
IPG31411S
IPG315S
IPG316S
IPG321S
IPG3212S
IPG3219S
IPN3211S
IPG32191S
IPG32199S
IPN32192S
IPG321219S
IPN321991S
IPG3212A9S
IPG322S
IPG3221S
IPG3222S
IPG32212S
IPG32222S
IPN32211S
IPN32213S
IPN32221S
IPN322121S
IPG32223A9S
IPG323S
IPG324S
IPG32411S
IPG32411XS
IPN32411DS
IPN32411GS
IPN32411RS
IPN32412A9S
IPG325S
IPG3251S
IPG3252S
IPG3253S
IPG3254S
IPG3255S
IPG3256S
IPG32512S
IPG32513S
IPG32521S
IPG32551S
IPG325212S
IPN325211S
IPG32518S
IPG3255A9S
IPG32511A9S
IPG32512T8S
IPG3254NP8S
IPG326S
IPG3261S
IPG3262S
IPG32621S
IPG32622A9S
IPG327S
IPG3271S
IPG3272S
IPG3273S
IPG3274S
IPG3279S
IPG32711S
IPG32712S
IPN32731S
IPN327213S
IPG3271A4A9S
IPG3271A9S
IPN32732T9S
IPG331S
IPG3311A2S
IPG3311A2FS
IPN3311A2BS
IPN3311A2CS
IPN3311A2DS
IPN3311A2ES
IPN3311A2PS
IPN3311A2RS
IPN3311A2ZS
IPG3313S
IPN331314S
IPN331313PS
IPN331314S
IPN331315A8MS
IPN331318ES
IPG3314S
IPG33141S
IPG33141CS
IPN33141NS
IPG3315S
IPG332S
IPG3325S
IPG3327S
IPG3329S
IPN3321S
IPN3322S
IPN3323S
IPN3326S
IPN3328S
IPG332991S
IPG333S
IPG3331S
IPG3332S
IPG3334S
IPG3335S
IPG3336S
IPG33311S
IPG33312S
IPN33313S
IPG333111S
IPG3333A9S
IPG3334T6S
IPG334S
IPG3341S
IPG3342S
IPG3343S
IPG3344S
IPG3345S
IPG335S
IPG3351S
IPG3352S
IPG3353S
IPG3359S
IPG33521S
IPG33522S
IPG33591S
IPN33592S
IPG335A2S
IPG33593T9S
IPG336S
IPG3361S
IPG3362S
IPG3363S
IPG3364S
IPG3366S
IPN3365S
IPN3369S
IPG33611S
IPG33612S
IPG336111S
IPCONGD
IPDCONGD
IPB51110S
IPB51111S
IPB51112S
IPB51120S
IPB51121S
IPB51122S
IPB511221S
IPB511222S
IPB51123S
IPNCONGD
IPB51210S
IPB51211S
IPB51212S
IPB51213S
IPB51214S
IPB51220S
IPFUELS
IPB51222S
IPB52000S
IPBUSEQ
IPB52110S
IPB52120S
IPB52130S
IPB52131S
IPB52132S
IPB52200S
IPB52300S
IPB54000S
IPB54100S
IPB54200S
IPB54210S
IPB54220S
IPMAT
IPZ53010S
IPDMAT
IPB53110S
IPB53120S
IPB53121S
IPB53122S
IPB53123S
IPB53130S
IPB53131S
IPB53132S
IPNMAT
IPB53210S
IPB53220S
IPB53230S
IPB53240S
IPB53241S
IPB53242S
IPB53300S
IPB53310S
IPB53320S
IPG336112S
IPG336212S
IPG336214S
IPN336213S
IPG3361T3S
IPG3364T9S
IPG3365T9S
IPG336411T3S
IPG337S
IPN3371S
IPG3372A9S
IPG339S
IPN3391S
IPG5111S
IPG51111S
IPG51112T9S
IPG311A2S
IPG313A4S
IPG315A6S
CAPUTLG211S
CAPUTLG212S
CAPUTLN2121S
CAPUTLG2122S
CAPUTLG2123S
CAPUTLG213S
CAPUTLG312S
CAPUTLG313S
CAPUTLG314S
CAPUTLG315S
CAPUTLG316S
CAPUTLG321S
CAPUTLG322S
CAPUTLG323S
CAPUTLG324S
CAPUTLG325S
CAPUTLN325211S
CAPUTLG325212S
CAPUTLG326S
CAPUTLG327S
CAPUTLG331S
CAPUTLG3311A2S
CAPUTLG332S
CAPUTLG333S
CAPUTLG334S
CAPUTLG3341S
CAPUTLG3342S
CAPUTLHITEK2S
CAPUTLG335S
CAPUTLG331S
CAPUTLG33611S
CAPUTLG3361T3S
CAPUTLG3364T9S
CAPUTLG337S
CAPUTLG339S
CAPUTLG311A2S
CAPUTLG313A4S
CAPUTLG315A6S
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
    time.sleep(PAUSE_SECONDS_BETWEEN_CALLS + random.uniform(0, 0.15))  # tiny jitter

def retry_call(func, *args, **kwargs):
    """
    Retry with exponential backoff + jitter.
    Explicitly handles 'Too Many Requests' / 429 by backing off harder.
    """
    last_err = None
    for attempt in range(1, MAX_RETRIES_PER_CALL + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_err = e
            msg = str(e).lower()
            # Detect throttling
            if "too many requests" in msg or "429" in msg or "rate limit" in msg:
                wait = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1)) + random.uniform(0, 1.0)
                print(f"⚠️ Rate limit hit. Backing off {wait:.1f}s (attempt {attempt}/{MAX_RETRIES_PER_CALL})...")
                time.sleep(wait)
            else:
                if attempt < MAX_RETRIES_PER_CALL:
                    wait = RETRY_BACKOFF_SECONDS * (attempt ** 1.3) + random.uniform(0, 0.5)
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
