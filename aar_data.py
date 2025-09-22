import pandas as pd
import datetime
import requests
from bs4 import BeautifulSoup
import re
import os

# === CONFIG ===
STB_URL = "https://www.stb.gov/reports-data/rail-service-data/"
DOWNLOAD_FOLDER = os.getenv("STB_LOG_DIR", os.getcwd())
EP724_FILENAME = "EP724_latest.xlsx"
OUTPUT_FILE = "north_star_reconstructed.xlsx"
CN_URL = "https://www.cn.ca/-/media/files/investors/investor-performance-measures/perf_measures_en.xlsx"

# === EP724 HELPERS ===
def get_latest_ep724_url():
    """
    Scrape STB rail service page and find the latest EP724 Excel file URL.
    """
    r = requests.get(STB_URL)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # Look for all links ending in .xlsx and containing "EP724"
    links = [a["href"] for a in soup.find_all("a", href=True) if "EP724" in a["href"] and a["href"].endswith(".xlsx")]
    if not links:
        raise FileNotFoundError("❌ No EP724 Excel file found on STB site.")

    # Use the most recent one (last in sorted list)
    links.sort()
    url = links[-1]

    # Ensure absolute URL
    if not url.startswith("http"):
        url = "https://www.stb.gov" + url

    print(f"🗌 Latest EP724 file found: {url}")
    return url


def download_ep724():
    """
    Download the latest EP724 file to DOWNLOAD_FOLDER and return its local path.
    """
    url = get_latest_ep724_url()
    local_path = os.path.join(DOWNLOAD_FOLDER, EP724_FILENAME)

    r = requests.get(url)
    r.raise_for_status()
    with open(local_path, "wb") as f:
        f.write(r.content)

    print(f"✅ EP724 saved to {local_path}")
    return local_path


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
# --------------------
# BUILDERS & LOADERS
# --------------------

def build_skeleton(rr):
    """Build a blank skeleton with just the Category column."""
    return pd.DataFrame({"Category": categories[rr]})


def ep724_get_week_cols(df_raw):
    """Return list of (col_name, week_label) for current year week columns."""
    week_cols = []
    this_year = datetime.date.today().year
    # Week data always starts after descriptor columns
    descriptor_cols = ["Railroad/Region", "Category No.", "Sub-Category", "Measure", "Variable", "Sub-Variable"]
    for col in df_raw.columns:
        if col in descriptor_cols:
            continue
        try:
            dt = pd.to_datetime(col, errors="coerce")
            if pd.notna(dt) and dt.year == this_year:
                week_cols.append((col, f"Week {dt.isocalendar().week}"))
        except Exception:
            continue
    return week_cols


def fill_from_ep724(rr_code):
    """
    Fill one RR from EP724 consolidated file using descriptor columns
    (railroad_region, variable, sub_variable, measure).
    Auto-normalizes headers so naming quirks (spaces, slashes, newlines) won't break it.
    """
    ep724_path = os.path.join(DOWNLOAD_FOLDER, EP724_FILENAME)
    print(f"📊 Loading EP724 data for {rr_code}...")
    df_raw = pd.read_excel(ep724_path, sheet_name=0, engine="openpyxl")
    print(f"📊 EP724 data loaded: {df_raw.shape[0]} rows, {df_raw.shape[1]} columns")

    # --- Normalize column names ---
    df_raw.columns = (
        df_raw.columns.astype(str)
        .str.strip()
        .str.replace(r"[\s/]+", "_", regex=True)  # spaces, slashes, newlines → underscore
        .str.lower()
    )
    print("🔎 Normalized columns:", list(df_raw.columns[:10]))

    # Ensure key columns exist
    expected = ["railroad_region", "variable", "sub_variable", "measure"]
    for col in expected:
        if col not in df_raw.columns:
            print(f"⚠️ Warning: expected column '{col}' not found in EP724 file")

    # Normalize descriptor text columns
    for col in expected:
        if col in df_raw.columns:
            df_raw[col] = df_raw[col].astype(str).str.strip().str.lower()

    # Build skeleton
    df = build_skeleton(rr_code)

    # Prepare week columns
    week_cols = ep724_get_week_cols(df_raw)
    for _, wk in week_cols:
        df[wk] = None

    # Loop through categories
    for category in categories[rr_code]:
        norm_cat = category.strip().lower()

        # Filter rows for this RR
        rr_mask = df_raw["railroad_region"] == rr_code.lower() if "railroad_region" in df_raw else True

        # Match category against variable / sub_variable / measure
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

def fill_from_cn():
    df_raw = pd.read_excel(CN_URL, sheet_name="53 Weeks History", engine='openpyxl')

    df = build_skeleton("CNI")
    week_cols = df_raw.columns[2:].tolist()
    for col in week_cols:
        df[col] = None

    for idx, row in df_raw.iterrows():
        label = str(row.iloc[0]).strip()
        if label in categories["CNI"]:
            values = row.iloc[2:].tolist()
            df.loc[df["Category"] == label, week_cols] = values

    return df


def fill_from_cpkc(cpkc_url):
    r = requests.get(cpkc_url)
    tmpfile = os.path.join(DOWNLOAD_FOLDER, "CPKC.xlsx")
    with open(tmpfile, "wb") as f:
        f.write(r.content)

    df_raw = pd.read_excel(tmpfile, sheet_name="Railroad Performance All Years", engine='openpyxl')

    df = build_skeleton("CPKC")
    week_cols = df_raw.columns[1:].tolist()
    for col in week_cols:
        df[col] = None

    for idx, row in df_raw.iterrows():
        label = str(row.iloc[0]).strip()
        if label in categories["CPKC"]:
            values = row.iloc[1:].tolist()
            df.loc[df["Category"] == label, week_cols] = values

    return df


# --------------------
# ROW MAPS (explicit for EP724 RRs)
# --------------------

rowmap_bnsf = {
    "System": 0,
    "Foreign RR": 1,
    "Private": 2,
    "Total  Cars": 29,
    "Pct. Private": 3,
    "Box": 21,
    "Covered Hopper": 22,
    "Gondola": 23,
    "Intermodal": 24,
    "Multilevel": 25,
    "Open Hopper": 26,
    "Tank": 27,
    "Other": 28,
    "Total": 29,
    "Intermodal": 2,
    "Manifest": 8,
    "Multilevel": 5,
    "Coal Unit": 4,
    "Grain Unit": 3,
    "All Trains": 9,
    "Barstow, CA": 10,
    "Denver, CO": 11,
    "Fort Worth, TX": 12,
    "Galesburg, IL": 13,
    "Houston, TX": None,
    "Kansas City, KS": 14,
    "Lincoln, NE": 15,
    "Memphis, TN": 16,
    "Northtown, MN": 17,
    "Pasco, WA": 18,
    "Tulsa, OK": 19,
    "Entire Railroad": 20,
}

rowmap_csx = {
    "System": 1588,
    "Total  Cars": 1588,
    "Pct. Private": None,
    "Box": 1580,
    "Covered Hopper": 1581,
    "Gondola": 1582,
    "Intermodal": 1583,
    "Multilevel": 1584,
    "Open Hopper": 1585,
    "Tank": 1586,
    "Other": 1587,
    "Total": 1588,
    "Coal": 1562,
    "Crude": 1564,
    "Ethanol": 1565,
    "Grain": 1561,
    "Intermodal": 1560,
    "Merch": 1566,
    "System": 1567,
    "Chicago, Il": 1569,
    "Cincinnati, Oh": 1570,
    "Baltimore, Md": 1568,
    "Hamlet, Nc": None,
    "Indianapolis, In": 1571,
    "Jacksonville, Fl": 1572,
    "Louisville, Ky": 1573,
    "Nashville, Tn": 1574,
    "Rocky Mount, Nc": 1575,
    "Selkirk, Ny": 1576,
    "Toledo, Oh": 1577,
    "Waycross, Ga": 1578,
    "Willard, Oh": None,
    "System": 1579,
}

rowmap_ns = {
    "System": None,
    "Foreign RR": None,
    "Private": None,
    "Total  Cars": 2261,
    "Pct. Private": None,
    "Box": 2253,
    "Covered Hopper": 2254,
    "Gondola": 2255,
    "Intermodal": 2256,
    "Multilevel": 2257,
    "Open Hopper": 2258,
    "Tank": 2259,
    "Other": 2260,
    "Total": 2261,
    "Intermodal": 2232,
    "Manifest": 2238,
    "Multilevel": 2235,
    "Coal Unit": 2234,
    "Grain Unit": 2233,
    "All Trains": 2239,
    "Allentown, PA": 2240,
    "Bellevue, OH": 2242,
    "Birmingham, AL": 2243,
    "Chattanooga, TN": 2244,
    "Columbus, OH": None,
    "Conway, PA": 2245,
    "Decatur, IL": 2246,
    "Elkhart, IN": 2247,
    "Atlanta, GA": 2241,
    "Linwood, NC": 2248,
    "Macon, GA": 2249,
    "New Orleans, LA": None,
    "Roanoke, VA": 2251,
    "Sheffield, AL": None,
    "Entire Railroad": 2252,
}

rowmap_up = {
    "System": None,
    "Foreign RR": None,
    "Private": None,
    "Total  Cars": 2765,
    "Pct. Private": None,
    "Box": 2757,
    "Covered Hopper": 2758,
    "Gondola": 2759,
    "Intermodal": 2760,
    "Multilevel": 2761,
    "Open Hopper": 2762,
    "Tank": 2763,
    "Other": 2764,
    "Total": 2765,
    "Intermodal": 2737,
    "Manifest": 2743,
    "Multilevel": 2740,
    "Coal Unit": 2739,
    "Grain Unit": 2738,
    "All Trains": 2744,
    "Chicago, IL - Proviso": 2745,
    "Fort Worth, TX": 2746,
    "Hinkle, OR": None,
    "Houston, TX - Englewood": 2747,
    "Houston, TX - Settegast": None,
    "Kansas City, MO": None,
    "Livonia, LA": 2748,
    "North Little Rock, AR": 2749,
    "Santa Teresa, NM": 2754,
    "North Platte West, NE": 2751,
    "Pine Bluff, AR": 2752,
    "Roseville, CA": 2753,
    "West Colton, CA": 2755,
    "Entire Railroad": 2756,
}
# --------------------
# CATEGORIES (for skeleton building)
# --------------------
categories = {
    "BNSF": ["System", "Foreign RR", "Private", "Pct. Private", 
             "Box", "Covered Hopper", "Gondola", "Intermodal", 
             "Multilevel", "Open Hopper", "Tank", "Other", "Total",
             "Manifest", "Coal Unit", "Grain Unit", "All Trains",
             "Barstow, CA", "Denver, CO", "Fort Worth, TX",
             "Galesburg, IL", "Kansas City, KS", "Lincoln, NE",
             "Memphis, TN", "Northtown, MN", "Pasco, WA", 
             "Tulsa, OK", "Entire Railroad"],

    "CSX": ["System", "Total Cars", "Pct. Private",
            "Box", "Covered Hopper", "Gondola", "Intermodal",
            "Multilevel", "Open Hopper", "Tank", "Other", "Total",
            "Coal", "Crude", "Ethanol", "Grain", "Merch",
            "Chicago, IL", "Cincinnati, OH", "Baltimore, MD",
            "Indianapolis, IN", "Jacksonville, FL", "Louisville, KY",
            "Nashville, TN", "Rocky Mount, NC", "Selkirk, NY",
            "Toledo, OH", "Waycross, GA"],

    "NS": ["System", "Total Cars", "Box", "Covered Hopper", "Gondola",
           "Intermodal", "Multilevel", "Open Hopper", "Tank", "Other", "Total",
           "Manifest", "Coal Unit", "Grain Unit", "All Trains",
           "Allentown, PA", "Bellevue, OH", "Birmingham, AL", "Chattanooga, TN",
           "Conway, PA", "Decatur, IL", "Elkhart, IN", "Atlanta, GA",
           "Linwood, NC", "Macon, GA", "Roanoke, VA", "Entire Railroad"],

    "UP": ["System", "Total Cars", "Box", "Covered Hopper", "Gondola",
           "Intermodal", "Multilevel", "Open Hopper", "Tank", "Other", "Total",
           "Manifest", "Coal Unit", "Grain Unit", "All Trains",
           "Chicago, IL - Proviso", "Fort Worth, TX", "Houston, TX - Englewood",
           "Livonia, LA", "North Little Rock, AR", "Santa Teresa, NM",
           "North Platte West, NE", "Pine Bluff, AR", "Roseville, CA",
           "West Colton, CA", "Entire Railroad"],

    "CNI": ["System", "Box", "Covered Hopper", "Gondola", "Intermodal",
            "Multilevel", "Open Hopper", "Tank", "Other"],

    "CPKC": ["System", "Box", "Covered Hopper", "Gondola", "Intermodal",
             "Multilevel", "Open Hopper", "Tank", "Other"]
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
        fill_from_ep724("CSX").to_excel(writer, sheet_name="CSX", index=False)
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
