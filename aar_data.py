import pandas as pd
import datetime
import requests
from bs4 import BeautifulSoup
import re
import os
import sys

# === CONFIG ===
STB_URL = "https://www.stb.gov/reports-data/rail-service-data/"
DOWNLOAD_FOLDER = os.getenv("STB_LOG_DIR", os.getcwd())
EP724_FILENAME = "EP724_latest.xlsx"

# === EP724 FUNCTIONS ===
def get_latest_ep724_url():
    """Scrape STB page and return the latest EP724 Excel URL and date"""
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(STB_URL, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    links = soup.find_all('a', href=True)
    candidates = []

    for link in links:
        href = link['href']
        if "EP724" in href and href.endswith(".xlsx"):
            match = re.search(r'(\d{4}-\d{2}-\d{2})', href)
            if match:
                date_str = match.group(1)
                try:
                    file_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                    full_url = href if href.startswith("http") else f"https://www.stb.gov{href}"
                    candidates.append((file_date, full_url))
                except ValueError:
                    continue

    if not candidates:
        raise ValueError("❌ No valid EP724 files found.")

    latest_file = max(candidates, key=lambda x: x[0])
    print(f"🗌 Latest EP724 file found: {latest_file[0]} → {latest_file[1]}")
    return latest_file[1]

def download_ep724():
    """Download the latest EP724 and return local path"""
    url = get_latest_ep724_url()
    save_path = os.path.join(DOWNLOAD_FOLDER, EP724_FILENAME)
    print(f"⬇️ Downloading EP724 file: {url}")
    response = requests.get(url)
    if response.status_code == 200:
        with open(save_path, "wb") as f:
            f.write(response.content)
        print(f"✅ EP724 saved to {save_path}")
        return save_path
    else:
        raise Exception(f"❌ Failed to download EP724: Status code {response.status_code}")

# === CPKC FUNCTION ===
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
# CATEGORY SKELETONS
# --------------------
categories = {
    "BNI": [
        "System","Foreign RR","Private","Total  Cars","Pct. Private","Box","Covered Hopper",
        "Gondola","Intermodal","Multilevel","Open Hopper","Tank","Other","Total",
        "Intermodal","Manifest","Multilevel","Coal Unit","Grain Unit","All Trains",
        "Barstow, CA","Denver, CO","Fort Worth, TX","Galesburg, IL","Houston, TX",
        "Kansas City, KS","Lincoln, NE","Memphis, TN","Northtown, MN","Pasco, WA",
        "Tulsa, OK","Entire Railroad"
    ],
    "CNI": [
        "Walker Yard (Edmonton), AB","Fond du Lac Yard, WI","Jackson Yard, MS",
        "MacMillan Yard (Toronto), ON","Markham Yard, IL","Harrison Yard (Memphis), TN",
        "Symington Yard (Winnipeg), MB","Tascherau Yard (Montreal), QC","Thornton Yard (Vancouver), BC",
        "Total Dwell - Major Yards","Entire Railroad","Intermodal","Manifest","Multilevel",
        "Coal Unit","Grain Unit","All Trains","Total Shipments","Shipments without Bill",
        "Percent without Customer Bill","System","Foreign RR","Private","Total  Cars",
        "Box","Covered Hopper","Gondola","Intermodal","Multilevel","Open Hopper","Tank","Other","Total"
    ],
    "CPKC": [
        "System","Foreign RR","Private","Total  Cars","Pct. Private","Box","Covered Hopper",
        "Gondola","Intermodal","Multilevel","Open Hopper","Tank","Other","Total",
        "Intermodal","Manifest","Multilevel","Coal Unit","Grain Unit","All Trains",
        "Calgary, AB","Chicago, IL","Edmonton, AB","Vancouver, BC","Moose Jaw, SK",
        "Montreal, QC","St Paul, MN","Thunder Bay, ON","Toronto, ON","Winnipeg, MB",
        "Kansas City, MO","Sanchez, MX","Shreveport, LA","Monterrey, CA","Laredo Yard, TX",
        "San Luis Potosi, MX","Jackson, MS","Entire Railroad"
    ],
    "CSX": [
        "System","Total  Cars","Pct. Private","Box","Covered Hopper","Gondola",
        "Intermodal","Multilevel","Open Hopper","Tank","Other","Total",
        "Coal","Crude","Ethanol","Grain","Intermodal","Merch","System",
        "Chicago, Il","Cincinnati, Oh","Baltimore, Md","Hamlet, Nc","Indianapolis, In",
        "Jacksonville, Fl","Louisville, Ky","Nashville, Tn","Rocky Mount, Nc",
        "Selkirk, Ny","Toledo, Oh","Waycross, Ga","Willard, Oh","System"
    ],
    "NSC": [
        "System","Foreign RR","Private","Total  Cars","Pct. Private","Box","Covered Hopper",
        "Gondola","Intermodal","Multilevel","Open Hopper","Tank","Other","Total",
        "Intermodal","Manifest","Multilevel","Coal Unit","Grain Unit","All Trains",
        "Allentown, PA","Bellevue, OH","Birmingham, AL","Chattanooga, TN","Columbus, OH",
        "Conway, PA","Decatur, IL","Elkhart, IN","Atlanta, GA","Linwood, NC","Macon, GA",
        "New Orleans, LA","Roanoke, VA","Sheffield, AL","Entire Railroad"
    ],
    "UNP": [
        "System","Foreign RR","Private","Total  Cars","Pct. Private","Box","Covered Hopper",
        "Gondola","Intermodal","Multilevel","Open Hopper","Tank","Other","Total",
        "Intermodal","Manifest","Multilevel","Coal Unit","Grain Unit","All Trains",
        "Chicago, IL - Proviso","Fort Worth, TX","Hinkle, OR","Houston, TX - Englewood",
        "Houston, TX - Settegast","Kansas City, MO","Livonia, LA","North Little Rock, AR",
        "Santa Teresa, NM","North Platte West, NE","Pine Bluff, AR","Roseville, CA",
        "West Colton, CA","Entire Railroad"
    ]
}

# --------------------
# MAPPINGS
# --------------------
mapping_bnsf = {
    "System": ("Cars On Line (Count)", "System"),
    "Foreign RR": ("Cars On Line (Count)", "Foreign"),
    "Private": ("Cars On Line (Count)", "Private"),
    "Total  Cars": ("Cars On Line (Count)", "Total"),
    "Pct. Private": ("Cars On Line (Count)", "% Private"),
    "Box": ("Cars On Line (Count)", "Box"),
    "Covered Hopper": ("Cars On Line (Count)", "Covered Hopper"),
    "Gondola": ("Cars On Line (Count)", "Gondola"),
    "Intermodal": ("Cars On Line (Count)", "Intermodal"),
    "Multilevel": ("Cars On Line (Count)", "Multilevel"),
    "Open Hopper": ("Cars On Line (Count)", "Open Hopper"),
    "Tank": ("Cars On Line (Count)", "Tank"),
    "Other": ("Cars On Line (Count)", "Other"),
    "Total": ("Cars On Line (Count)", "Total"),
    "Intermodal": ("Average Train Speed (MPH)", "Intermodal"),
    "Manifest": ("Average Train Speed (MPH)", "Manifest"),
    "Multilevel": ("Average Train Speed (MPH)", "Multilevel"),
    "Coal Unit": ("Average Train Speed (MPH)", "Coal Unit"),
    "Grain Unit": ("Average Train Speed (MPH)", "Grain Unit"),
    "All Trains": ("Average Train Speed (MPH)", "All Trains"),
    "Barstow, CA": ("Average Terminal Dwell Time (Hrs)", "Barstow"),
    "Denver, CO": ("Average Terminal Dwell Time (Hrs)", "Denver"),
    "Fort Worth, TX": ("Average Terminal Dwell Time (Hrs)", "Fort Worth"),
    "Galesburg, IL": ("Average Terminal Dwell Time (Hrs)", "Galesburg"),
    "Houston, TX": ("Average Terminal Dwell Time (Hrs)", "Houston"),
    "Kansas City, KS": ("Average Terminal Dwell Time (Hrs)", "Kansas City"),
    "Lincoln, NE": ("Average Terminal Dwell Time (Hrs)", "Lincoln"),
    "Memphis, TN": ("Average Terminal Dwell Time (Hrs)", "Memphis"),
    "Northtown, MN": ("Average Terminal Dwell Time (Hrs)", "Northtown"),
    "Pasco, WA": ("Average Terminal Dwell Time (Hrs)", "Pasco"),
    "Tulsa, OK": ("Average Terminal Dwell Time (Hrs)", "Tulsa"),
    "Entire Railroad": ("Average Terminal Dwell Time (Hrs)", "System"),
}

mapping_csx = {
    "System": ("Cars On Line (Count)", "System"),
    "Total  Cars": ("Cars On Line (Count)", "Total"),
    "Pct. Private": ("Cars On Line (Count)", "% Private"),
    "Box": ("Cars On Line (Count)", "Box"),
    "Covered Hopper": ("Cars On Line (Count)", "Covered Hopper"),
    "Gondola": ("Cars On Line (Count)", "Gondola"),
    "Intermodal": ("Cars On Line (Count)", "Intermodal"),
    "Multilevel": ("Cars On Line (Count)", "Multilevel"),
    "Open Hopper": ("Cars On Line (Count)", "Open Hopper"),
    "Tank": ("Cars On Line (Count)", "Tank"),
    "Other": ("Cars On Line (Count)", "Other"),
    "Total": ("Cars On Line (Count)", "Total"),

    # Train Speed
    "Coal": ("Average Train Speed (MPH)", "Coal Unit"),
    "Crude": ("Average Train Speed (MPH)", "Crude"),
    "Ethanol": ("Average Train Speed (MPH)", "Ethanol"),
    "Grain": ("Average Train Speed (MPH)", "Grain Unit"),
    "Intermodal": ("Average Train Speed (MPH)", "Intermodal"),
    "Merch": ("Average Train Speed (MPH)", "Manifest"),
    "System": ("Average Train Speed (MPH)", "All Trains"),

    # Terminal Dwell
    "Chicago, Il": ("Average Terminal Dwell Time (Hrs)", "Chicago"),
    "Cincinnati, Oh": ("Average Terminal Dwell Time (Hrs)", "Cincinnati"),
    "Baltimore, Md": ("Average Terminal Dwell Time (Hrs)", "Baltimore"),
    "Hamlet, Nc": ("Average Terminal Dwell Time (Hrs)", "Hamlet"),
    "Indianapolis, In": ("Average Terminal Dwell Time (Hrs)", "Indianapolis"),
    "Jacksonville, Fl": ("Average Terminal Dwell Time (Hrs)", "Jacksonville"),
    "Louisville, Ky": ("Average Terminal Dwell Time (Hrs)", "Louisville"),
    "Nashville, Tn": ("Average Terminal Dwell Time (Hrs)", "Nashville"),
    "Rocky Mount, Nc": ("Average Terminal Dwell Time (Hrs)", "Rocky Mount"),
    "Selkirk, Ny": ("Average Terminal Dwell Time (Hrs)", "Selkirk"),
    "Toledo, Oh": ("Average Terminal Dwell Time (Hrs)", "Toledo"),
    "Waycross, Ga": ("Average Terminal Dwell Time (Hrs)", "Waycross"),
    "Willard, Oh": ("Average Terminal Dwell Time (Hrs)", "Willard"),
    "System": ("Average Terminal Dwell Time (Hrs)", "System"),
}

mapping_ns = {
    "System": ("Cars On Line (Count)", "System"),
    "Foreign RR": ("Cars On Line (Count)", "Foreign"),
    "Private": ("Cars On Line (Count)", "Private"),
    "Total  Cars": ("Cars On Line (Count)", "Total"),
    "Pct. Private": ("Cars On Line (Count)", "% Private"),
    "Box": ("Cars On Line (Count)", "Box"),
    "Covered Hopper": ("Cars On Line (Count)", "Covered Hopper"),
    "Gondola": ("Cars On Line (Count)", "Gondola"),
    "Intermodal": ("Cars On Line (Count)", "Intermodal"),
    "Multilevel": ("Cars On Line (Count)", "Multilevel"),
    "Open Hopper": ("Cars On Line (Count)", "Open Hopper"),
    "Tank": ("Cars On Line (Count)", "Tank"),
    "Other": ("Cars On Line (Count)", "Other"),
    "Total": ("Cars On Line (Count)", "Total"),

    # Train Speed
    "Intermodal": ("Average Train Speed (MPH)", "Intermodal"),
    "Manifest": ("Average Train Speed (MPH)", "Manifest"),
    "Multilevel": ("Average Train Speed (MPH)", "Multilevel"),
    "Coal Unit": ("Average Train Speed (MPH)", "Coal Unit"),
    "Grain Unit": ("Average Train Speed (MPH)", "Grain Unit"),
    "All Trains": ("Average Train Speed (MPH)", "All Trains"),

    # Terminal Dwell
    "Allentown, PA": ("Average Terminal Dwell Time (Hrs)", "Allentown"),
    "Bellevue, OH": ("Average Terminal Dwell Time (Hrs)", "Bellevue"),
    "Birmingham, AL": ("Average Terminal Dwell Time (Hrs)", "Birmingham"),
    "Chattanooga, TN": ("Average Terminal Dwell Time (Hrs)", "Chattanooga"),
    "Columbus, OH": ("Average Terminal Dwell Time (Hrs)", "Columbus"),
    "Conway, PA": ("Average Terminal Dwell Time (Hrs)", "Conway"),
    "Decatur, IL": ("Average Terminal Dwell Time (Hrs)", "Decatur"),
    "Elkhart, IN": ("Average Terminal Dwell Time (Hrs)", "Elkhart"),
    "Atlanta, GA": ("Average Terminal Dwell Time (Hrs)", "Atlanta"),
    "Linwood, NC": ("Average Terminal Dwell Time (Hrs)", "Linwood"),
    "Macon, GA": ("Average Terminal Dwell Time (Hrs)", "Macon"),
    "New Orleans, LA": ("Average Terminal Dwell Time (Hrs)", "New Orleans"),
    "Roanoke, VA": ("Average Terminal Dwell Time (Hrs)", "Roanoke"),
    "Sheffield, AL": ("Average Terminal Dwell Time (Hrs)", "Sheffield"),
    "Entire Railroad": ("Average Terminal Dwell Time (Hrs)", "System"),
}

mapping_up = {
    "System": ("Cars On Line (Count)", "System"),
    "Foreign RR": ("Cars On Line (Count)", "Foreign"),
    "Private": ("Cars On Line (Count)", "Private"),
    "Total  Cars": ("Cars On Line (Count)", "Total"),
    "Pct. Private": ("Cars On Line (Count)", "% Private"),
    "Box": ("Cars On Line (Count)", "Box"),
    "Covered Hopper": ("Cars On Line (Count)", "Covered Hopper"),
    "Gondola": ("Cars On Line (Count)", "Gondola"),
    "Intermodal": ("Cars On Line (Count)", "Intermodal"),
    "Multilevel": ("Cars On Line (Count)", "Multilevel"),
    "Open Hopper": ("Cars On Line (Count)", "Open Hopper"),
    "Tank": ("Cars On Line (Count)", "Tank"),
    "Other": ("Cars On Line (Count)", "Other"),
    "Total": ("Cars On Line (Count)", "Total"),

    # Train Speed
    "Intermodal": ("Average Train Speed (MPH)", "Intermodal"),
    "Manifest": ("Average Train Speed (MPH)", "Manifest"),
    "Multilevel": ("Average Train Speed (MPH)", "Multilevel"),
    "Coal Unit": ("Average Train Speed (MPH)", "Coal Unit"),
    "Grain Unit": ("Average Train Speed (MPH)", "Grain Unit"),
    "All Trains": ("Average Train Speed (MPH)", "All Trains"),

    # Terminal Dwell
    "Chicago, IL - Proviso": ("Average Terminal Dwell Time (Hrs)", "Chicago"),
    "Fort Worth, TX": ("Average Terminal Dwell Time (Hrs)", "Fort Worth"),
    "Hinkle, OR": ("Average Terminal Dwell Time (Hrs)", "Hinkle"),
    "Houston, TX - Englewood": ("Average Terminal Dwell Time (Hrs)", "Englewood"),
    "Houston, TX - Settegast": ("Average Terminal Dwell Time (Hrs)", "Settegast"),
    "Kansas City, MO": ("Average Terminal Dwell Time (Hrs)", "Kansas City"),
    "Livonia, LA": ("Average Terminal Dwell Time (Hrs)", "Livonia"),
    "North Little Rock, AR": ("Average Terminal Dwell Time (Hrs)", "North Little Rock"),
    "Santa Teresa, NM": ("Average Terminal Dwell Time (Hrs)", "Santa Teresa"),
    "North Platte West, NE": ("Average Terminal Dwell Time (Hrs)", "North Platte"),
    "Pine Bluff, AR": ("Average Terminal Dwell Time (Hrs)", "Pine Bluff"),
    "Roseville, CA": ("Average Terminal Dwell Time (Hrs)", "Roseville"),
    "West Colton, CA": ("Average Terminal Dwell Time (Hrs)", "West Colton"),
    "Entire Railroad": ("Average Terminal Dwell Time (Hrs)", "System"),
}

# === FILL FUNCTIONS ===
def fill_from_ep724(rr_code, rr_name, mapping):
    df = build_skeleton(rr_code)
    raw = pd.read_excel(ep724_raw, sheet_name=0)
    raw_rr = raw[raw["Railroad/\nRegion"].astype(str).str.contains(rr_name, na=False)]
    for cat, (measure, variable) in mapping.items():
        match = raw_rr[
            (raw_rr["Measure"].astype(str)==measure) &
            (raw_rr["Variable"].astype(str).str.contains(variable, case=False, na=False))
        ]
        if not match.empty:
            row_vals = match.iloc[0, 6:58].values
            df.loc[df["Category"]==cat, df.columns[3:]] = row_vals
    return df

def fill_cpkc():
    df = build_skeleton("CPKC")
    raw = pd.read_excel(cpkc_raw, sheet_name=0, header=0)
    for cat, raw_label in mapping_cpkc.items():
        match = raw[raw.iloc[:,0].astype(str).str.contains(raw_label,case=False,na=False)]
        if not match.empty:
            df.loc[df["Category"]==cat, df.columns[3:]] = match.iloc[0,1:53].values
    return df

def fill_cn():
    df = build_skeleton("CNI")
    raw = pd.read_excel(cn_raw, sheet_name=0, header=0)
    for cat, raw_label in mapping_cn.items():
        match = raw[raw.iloc[:,0].astype(str).str.contains(raw_label,case=False,na=False)]
        if not match.empty:
            df.loc[df["Category"]==cat, df.columns[3:]] = match.iloc[0,1:53].values
    return df

# === VALIDATION ===
def validate(df, rr):
    missing = df[df.iloc[:,3:].isna().all(axis=1)]["Category"].tolist()
    if missing:
        print(f"❌ {rr}: Missing data for categories: {missing}")
        sys.exit(1)   # Fail workflow if missing
    else:
        print(f"✅ {rr}: All categories populated.")

# === MASTER PIPELINE ===
output_file = "north_star_reconstructed.xlsx"
with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
    df_bnsf = fill_from_ep724("BNI","BNSF",mapping_bnsf)
    df_bnsf.to_excel(writer,sheet_name="BNSF",index=False)
    validate(df_bnsf,"BNSF")

    df_cn = fill_cn()
    df_cn.to_excel(writer,sheet_name="CN",index=False)
    validate(df_cn,"CN")

    df_cpkc = fill_cpkc()
    df_cpkc.to_excel(writer,sheet_name="CPKC",index=False)
    validate(df_cpkc,"CPKC")

    df_csx = fill_from_ep724("CSX","CSX",mapping_csx)
    df_csx.to_excel(writer,sheet_name="CSX",index=False)
    validate(df_csx,"CSX")

    df_nsc = fill_from_ep724("NSC","NS",mapping_nsc)
    df_nsc.to_excel(writer,sheet_name="NS",index=False)
    validate(df_nsc,"NS")

    df_unp = fill_from_ep724("UNP","UP",mapping_unp)
    df_unp.to_excel(writer,sheet_name="UP",index=False)
    validate(df_unp,"UP")

print(f"🎉 Final North Star workbook written to {output_file}")
