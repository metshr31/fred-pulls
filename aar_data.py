import pandas as pd
import datetime
import requests
from bs4 import BeautifulSoup
import os
import re

# === CONFIG ===
STB_URL = "https://www.stb.gov/reports-data/rail-service-data/"
DOWNLOAD_FOLDER = os.getenv("STB_LOG_DIR", os.getcwd())
EP724_FILENAME = "EP724_latest.xlsx"
OUTPUT_FILE = "north_star_reconstructed.xlsx"
CN_URL = "https://www.cn.ca/-/media/files/investors/investor-performance-measures/perf_measures_en.xlsx"

# === COMMON HELPERS ===
def normalize_label(s):
    """Normalize labels for matching (lowercase, strip, remove punctuation)."""
    return re.sub(r"[^a-z0-9]+", " ", str(s).lower().strip())

def build_skeleton(rr):
    return pd.DataFrame({"Category": categories[rr]})

# === EP724 HELPERS ===
def get_latest_ep724_url():
    r = requests.get(STB_URL)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if "EP724" in a["href"] and a["href"].endswith(".xlsx")]
    if not links:
        raise FileNotFoundError("❌ No EP724 Excel file found on STB site.")
    links.sort()
    url = links[-1]
    if not url.startswith("http"):
        url = "https://www.stb.gov" + url
    print(f"🗌 Latest EP724 file found: {url}")
    return url

def download_ep724():
    url = get_latest_ep724_url()
    local_path = os.path.join(DOWNLOAD_FOLDER, EP724_FILENAME)
    r = requests.get(url)
    r.raise_for_status()
    with open(local_path, "wb") as f:
        f.write(r.content)
    print(f"✅ EP724 saved to {local_path}")
    return local_path

def ep724_get_week_cols(df_raw):
    week_cols = []
    this_year = datetime.date.today().year
    descriptor_cols = ["Railroad/Region", "Category No.", "Sub-Category", "Measure", "Variable", "Sub-Variable"]
    for col in df_raw.columns:
        if col in descriptor_cols: continue
        try:
            dt = pd.to_datetime(col, errors="coerce")
            if pd.notna(dt) and dt.year == this_year:
                week_cols.append((col, f"Week {dt.isocalendar().week}"))
        except Exception:
            continue
    return week_cols

def fill_from_ep724(rr_code):
    ep724_path = os.path.join(DOWNLOAD_FOLDER, EP724_FILENAME)
    print(f"📊 Loading EP724 data for {rr_code}...")
    df_raw = pd.read_excel(ep724_path, sheet_name=0, engine="openpyxl")
    df_raw.columns = (
        df_raw.columns.astype(str).str.strip()
        .str.replace(r"[\s/]+", "_", regex=True)
        .str.lower()
    )
    expected = ["railroad_region", "variable", "sub_variable", "measure"]
    for col in expected:
        if col in df_raw.columns:
            df_raw[col] = df_raw[col].astype(str).str.strip().str.lower()
    df = build_skeleton(rr_code)
    week_cols = ep724_get_week_cols(df_raw)
    for _, wk in week_cols:
        df[wk] = None
    for category in categories[rr_code]:
        norm_cat = category.strip().lower()
        rr_mask = df_raw["railroad_region"] == rr_code.lower() if "railroad_region" in df_raw else True
        cat_mask = (
            (df_raw["variable"].str.contains(norm_cat, na=False) if "variable" in df_raw else False)
            | (df_raw["sub_variable"].str.contains(norm_cat, na=False) if "sub_variable" in df_raw else False)
            | (df_raw["measure"].str.contains(norm_cat, na=False) if "measure" in df_raw else False)
        )
        matches = df_raw[rr_mask & cat_mask]
        if matches.empty:
            print(f"⚠️ Could not find row for '{category}' in {rr_code}")
            continue
        row = matches.iloc[0]
        vals_this_year = [row[col] if col in row else None for col, _ in week_cols]
        df.loc[df["Category"] == category, [wk for _, wk in week_cols]] = vals_this_year
    return df

# === CPKC HELPER ===
def get_cpkc_url():
    today = datetime.date.today()
    offset = (today.weekday() - 0) % 7  # Monday = 0
    last_monday = today - datetime.timedelta(days=offset)
    candidates = [last_monday, last_monday - datetime.timedelta(days=7)]
    base = "https://s21.q4cdn.com/736796105/files/doc_downloads"
    filename = "CPKC-53-Week-Railway-Performance-Report.xlsx"
    for cand in candidates:
        date_str = cand.strftime("%Y/%m/%d")
        url = f"{base}/{date_str}/{filename}"
        try:
            r = requests.head(url, timeout=5)
            if r.status_code == 200:
                print(f"✅ Using CPKC file: {url}")
                return url
        except Exception as e:
            print(f"⚠️ Could not reach {url}: {e}")
            continue
    raise FileNotFoundError("❌ Could not find CPKC report for the last two Mondays.")

def fill_from_cpkc(cpkc_url):
    r = requests.get(cpkc_url)
    tmpfile = os.path.join(DOWNLOAD_FOLDER, "CPKC.xlsx")
    with open(tmpfile, "wb") as f:
        f.write(r.content)
    df_raw = pd.read_excel(tmpfile, sheet_name="Railroad Performance All Years", engine='openpyxl')
    df_raw.iloc[:,0] = df_raw.iloc[:,0].astype(str).apply(normalize_label)

    df = build_skeleton("CPKC")
    week_cols = df_raw.columns[1:].tolist()
    for col in week_cols:
        df[col] = None

    matched, missed = [], []
    for idx, row in df_raw.iterrows():
        label = normalize_label(row.iloc[0])
        found = False
        for cat in categories["CPKC"]:
            if normalize_label(cat) == label:
                values = row.iloc[1:].tolist()
                df.loc[df["Category"] == cat, week_cols] = values
                matched.append(cat)
                found = True
                break
        if not found:
            missed.append(row.iloc[0])

    print(f"🔎 CPKC matched categories: {matched}")
    if missed:
        print(f"⚠️ CPKC missed categories: {missed[:10]}{'...' if len(missed)>10 else ''}")

    return df

# === CN HELPER ===
def fill_from_cn():
    df_raw = pd.read_excel(CN_URL, sheet_name="53 Weeks History", engine='openpyxl')
    df_raw.iloc[:,0] = df_raw.iloc[:,0].astype(str).apply(normalize_label)

    df = build_skeleton("CNI")
    week_cols = df_raw.columns[2:].tolist()
    for col in week_cols:
        df[col] = None

    matched, missed = [], []
    for idx, row in df_raw.iterrows():
        label = normalize_label(row.iloc[0])
        found = False
        for cat in categories["CNI"]:
            if normalize_label(cat) == label:
                values = row.iloc[2:].tolist()
                df.loc[df["Category"] == cat, week_cols] = values
                matched.append(cat)
                found = True
                break
        if not found:
            missed.append(row.iloc[0])

    print(f"🔎 CN matched categories: {matched}")
    if missed:
        print(f"⚠️ CN missed categories: {missed[:10]}{'...' if len(missed)>10 else ''}")

    return df

# === CSX HELPER ===
CSX_METRICS_PAGE = "https://investors.csx.com/metrics/default.aspx"
CSX_CDN_BASE = "https://s2.q4cdn.com/859568992/files/doc_downloads"

def _iso_week_year(date_obj):
    iso = date_obj.isocalendar()
    return iso[0], iso[1]

def _candidate_filenames(year, week):
    return [
        f"Historical_Data_Week_{week}_{year}.xlsx",
        f"Combined-Intermodal-and-Carload-TPC-Week-1-2022-Week-{week}-{year}.xlsx",
    ]

def get_csx_url(max_back_days=10, timeout=6):
    today = datetime.date.today()
    last_week_end = today - datetime.timedelta(days=today.weekday() + 2)
    year, week = _iso_week_year(last_week_end)
    # Probe CDN
    for delta in range(max_back_days):
        d = today - datetime.timedelta(days=delta)
        y, m, day = d.year, f"{d.month:02d}", f"{d.day:02d}"
        for fname in _candidate_filenames(year, week):
            url = f"{CSX_CDN_BASE}/{y}/{m}/{day}/{fname}"
            try:
                r = requests.head(url, timeout=timeout, allow_redirects=True)
                if r.status_code == 200 and "html" not in r.headers.get("Content-Type", ""):
                    return url
            except requests.RequestException:
                pass
    # Fallback scrape
    resp = requests.get(CSX_METRICS_PAGE, timeout=timeout)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".xlsx")]
    for u in links:
        if u.startswith("//"): u = "https:" + u
        elif u.startswith("/"): u = "https://investors.csx.com" + u
        r = requests.head(u, timeout=timeout, allow_redirects=True)
        if r.status_code == 200 and "html" not in r.headers.get("Content-Type", ""):
            return u
    raise FileNotFoundError("❌ Could not locate CSX weekly Excel via CDN probe or scrape.")

def fill_from_csx():
    url = get_csx_url()
    print(f"✅ Using CSX file: {url}")
    r = requests.get(url)
    r.raise_for_status()
    tmp = os.path.join(DOWNLOAD_FOLDER, "CSX.xlsx")
    with open(tmp, "wb") as f:
        f.write(r.content)
    df_raw = pd.read_excel(tmp, sheet_name=0, engine="openpyxl")
    df_raw.iloc[:,0] = df_raw.iloc[:,0].astype(str).apply(normalize_label)

    df = build_skeleton("CSX")
    week_cols = df_raw.columns[1:].tolist()
    for col in week_cols:
        df[col] = None

    matched, missed = [], []
    for idx, row in df_raw.iterrows():
        label = normalize_label(row.iloc[0])
        found = False
        for cat in categories["CSX"]:
            if normalize_label(cat) == label:
                values = row.iloc[1:].tolist()
                df.loc[df["Category"] == cat, week_cols] = values
                matched.append(cat)
                found = True
                break
        if not found:
            missed.append(row.iloc[0])

    print(f"🔎 CSX matched categories: {matched}")
    if missed:
        print(f"⚠️ CSX missed categories: {missed[:10]}{'...' if len(missed)>10 else ''}")

    return df

# --------------------
# CATEGORIES
# --------------------
categories = {
    "BNSF": ["System","Foreign RR","Private","Pct. Private","Box","Covered Hopper","Gondola","Intermodal",
             "Multilevel","Open Hopper","Tank","Other","Total","Manifest","Coal Unit","Grain Unit","All Trains",
             "Barstow, CA","Denver, CO","Fort Worth, TX","Galesburg, IL","Kansas City, KS","Lincoln, NE",
             "Memphis, TN","Northtown, MN","Pasco, WA","Tulsa, OK","Entire Railroad"],
    "CSX": ["System","Total Cars","Pct. Private","Box","Covered Hopper","Gondola","Intermodal","Multilevel",
            "Open Hopper","Tank","Other","Total","Coal","Crude","Ethanol","Grain","Merch","Chicago, IL",
            "Cincinnati, OH","Baltimore, MD","Indianapolis, IN","Jacksonville, FL","Louisville, KY","Nashville, TN",
            "Rocky Mount, NC","Selkirk, NY","Toledo, OH","Waycross, GA"],
    "NS": ["System","Total Cars","Box","Covered Hopper","Gondola","Intermodal","Multilevel","Open Hopper","Tank",
           "Other","Total","Manifest","Coal Unit","Grain Unit","All Trains","Allentown, PA","Bellevue, OH",
           "Birmingham, AL","Chattanooga, TN","Conway, PA","Decatur, IL","Elkhart, IN","Atlanta, GA","Linwood, NC",
           "Macon, GA","Roanoke, VA","Entire Railroad"],
    "UP": ["System","Total Cars","Box","Covered Hopper","Gondola","Intermodal","Multilevel","Open Hopper","Tank",
           "Other","Total","Manifest","Coal Unit","Grain Unit","All Trains","Chicago, IL - Proviso","Fort Worth, TX",
           "Houston, TX - Englewood","Livonia, LA","North Little Rock, AR","Santa Teresa, NM","North Platte West, NE",
           "Pine Bluff, AR","Roseville, CA","West Colton, CA","Entire Railroad"],
    "CNI": ["System","Box","Covered Hopper","Gondola","Intermodal","Multilevel","Open Hopper","Tank","Other"],
    "CPKC": ["System","Box","Covered Hopper","Gondola","Intermodal","Multilevel","Open Hopper","Tank","Other"]
}

# --------------------
# MAIN
# --------------------
def main():
    ep724_path = download_ep724()
    cpkc_url = get_cpkc_url()
    print("📝 Creating output Excel file...")
    with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter") as writer:
        print("🚂 Processing BNSF...")
        fill_from_ep724("BNSF").to_excel(writer, sheet_name="BNSF", index=False)
        print("🚂 Processing CSX...")
        fill_from_csx().to_excel(writer, sheet_name="CSX", index=False)
        print("🚂 Processing NS...")
        fill_from_ep724("NS").to_excel(writer, sheet_name="NS", index=False)
        print("🚂 Processing UP...")
        fill_from_ep724("UP").to_excel(writer, sheet_name="UP", index=False)
        print("🚂 Processing CN...")
        fill_from_cn().to_excel(writer, sheet_name="CN", index=False)
        print("🚂 Processing CPKC...")
        fill_from_cpkc(cpkc_url).to_excel(writer, sheet_name="CPKC", index=False)
    print(f"✅ All carriers written to {OUTPUT_FILE}")
    print("📂 Current working directory:", os.getcwd())
    print("🔎 File exists?", os.path.isfile(OUTPUT_FILE))

if __name__ == "__main__":
    main()
    print("✅ Finished run. File exists:", os.path.isfile(OUTPUT_FILE))
