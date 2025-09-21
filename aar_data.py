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

# === RAW SOURCES ===
ep724_raw = download_ep724()
cpkc_raw = get_cpkc_url()
cn_raw = "https://www.cn.ca/-/media/files/investors/investor-performance-measures/perf_measures_en.xlsx"
print(f"✅ CN file source: {cn_raw}")

# === CATEGORY SKELETONS ===
categories = {
    "BNI": [
        "System","Foreign RR","Private","Total  Cars","Pct. Private",
        "Box","Covered Hopper","Gondola","Intermodal","Multilevel",
        "Open Hopper","Tank","Other","Total",
        "Intermodal","Manifest","Multilevel","Coal Unit","Grain Unit","All Trains",
        "Barstow, CA","Denver, CO","Fort Worth, TX","Galesburg, IL","Houston, TX",
        "Kansas City, KS","Lincoln, NE","Memphis, TN","Northtown, MN","Pasco, WA",
        "Tulsa, OK","Entire Railroad"
    ],
    "CNI": [
        "Walker Yard (Edmonton), AB","Fond du Lac Yard, WI","Jackson Yard, MS",
        "MacMillan Yard (Toronto), ON","Markham Yard, IL","Harrison Yard (Memphis), TN",
        "Symington Yard (Winnipeg), MB","Tascherau Yard (Montreal), QC","Thornton Yard (Vancouver), BC",
        "Total Dwell - Major Yards","Entire Railroad",
        "Intermodal","Manifest","Multilevel","Coal Unit","Grain Unit","All Trains",
        "Total Shipments","Shipments without Bill","Percent without Customer Bill",
        "System","Foreign RR","Private","Total  Cars",
        "Box","Covered Hopper","Gondola","Intermodal","Multilevel","Open Hopper","Tank","Other","Total"
    ],
    "CPKC": [
        "System","Foreign RR","Private","Total  Cars","Pct. Private",
        "Box","Covered Hopper","Gondola","Intermodal","Multilevel",
        "Open Hopper","Tank","Other","Total",
        "Intermodal","Manifest","Multilevel","Coal Unit","Grain Unit","All Trains",
        "Calgary, AB","Chicago, IL","Edmonton, AB","Vancouver, BC","Moose Jaw, SK",
        "Montreal, QC","St Paul, MN","Thunder Bay, ON","Toronto, ON","Winnipeg, MB",
        "Kansas City, MO","Sanchez, MX","Shreveport, LA","Monterrey, CA","Laredo Yard, TX",
        "San Luis Potosi, MX","Jackson, MS","Entire Railroad"
    ],
    "CSX": [
        "System","Total  Cars","Pct. Private",
        "Box","Covered Hopper","Gondola","Intermodal","Multilevel","Open Hopper","Tank","Other","Total",
        "Coal","Crude","Ethanol","Grain","Intermodal","Merch","System",
        "Chicago, Il","Cincinnati, Oh","Baltimore, Md","Hamlet, Nc","Indianapolis, In",
        "Jacksonville, Fl","Louisville, Ky","Nashville, Tn","Rocky Mount, Nc",
        "Selkirk, Ny","Toledo, Oh","Waycross, Ga","Willard, Oh","System"
    ],
    "NSC": [
        "System","Foreign RR","Private","Total  Cars","Pct. Private",
        "Box","Covered Hopper","Gondola","Intermodal","Multilevel","Open Hopper","Tank","Other","Total",
        "Intermodal","Manifest","Multilevel","Coal Unit","Grain Unit","All Trains",
        "Allentown, PA","Bellevue, OH","Birmingham, AL","Chattanooga, TN","Columbus, OH","Conway, PA",
        "Decatur, IL","Elkhart, IN","Atlanta, GA","Linwood, NC","Macon, GA","New Orleans, LA",
        "Roanoke, VA","Sheffield, AL","Entire Railroad"
    ],
    "UNP": [
        "System","Foreign RR","Private","Total  Cars","Pct. Private",
        "Box","Covered Hopper","Gondola","Intermodal","Multilevel","Open Hopper","Tank","Other","Total",
        "Intermodal","Manifest","Multilevel","Coal Unit","Grain Unit","All Trains",
        "Chicago, IL - Proviso","Fort Worth, TX","Hinkle, OR","Houston, TX - Englewood","Houston, TX - Settegast",
        "Kansas City, MO","Livonia, LA","North Little Rock, AR","Santa Teresa, NM",
        "North Platte West, NE","Pine Bluff, AR","Roseville, CA","West Colton, CA","Entire Railroad"
    ],
}

# === SKELETON BUILDER ===
def build_skeleton(rr, year=2025):
    cols = ["Railroad","Category","Year"]+[f"Week_{i}" for i in range(1,53)]
    df = pd.DataFrame({"Category": categories[rr]})
    df.insert(0,"Railroad",rr)
    df.insert(2,"Year",year)
    for wk in range(1,53):
        df[f"Week_{wk}"] = pd.NA
    return df[cols]

# === MAPPING DICTIONARIES ===
# (Paste in mapping_bnsf, mapping_cn, mapping_cpkc, mapping_csx, mapping_nsc, mapping_unp from my previous message)

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
        print(f"⚠️ {rr}: Missing data for categories: {missing}")
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
