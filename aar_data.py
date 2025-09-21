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

# === EP724 FUNCTIONS ===
def get_latest_ep724_url():
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
    offset = (today.weekday() - 0) % 7
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
    raise FileNotFoundError("❌ Could not find CPKC report.")

# --------------------
# CATEGORY SKELETONS
# --------------------
categories = {
    "BNI": [
        "BNI",
        "System", "Foreign RR", "Private", "Total  Cars", "Pct. Private",
        "Box", "Covered Hopper", "Gondola", "Intermodal", "Multilevel", "Open Hopper", "Tank", "Other", "Total",
        "Intermodal", "Manifest", "Multilevel", "Coal Unit", "Grain Unit", "All Trains",
        "Barstow, CA", "Denver, CO", "Fort Worth, TX", "Galesburg, IL", "Houston, TX",
        "Kansas City, KS", "Lincoln, NE", "Memphis, TN", "Northtown, MN", "Pasco, WA",
        "Tulsa, OK", "Entire Railroad"
    ],
    "CSX": [
        "CSX",
        "System", "Total  Cars", "Pct. Private",
        "Box", "Covered Hopper", "Gondola", "Intermodal", "Multilevel", "Open Hopper", "Tank", "Other", "Total",
        "Coal", "Crude", "Ethanol", "Grain", "Intermodal", "Merch", "System",
        "Chicago, Il", "Cincinnati, Oh", "Baltimore, Md", "Hamlet, Nc", "Indianapolis, In",
        "Jacksonville, Fl", "Louisville, Ky", "Nashville, Tn", "Rocky Mount, Nc",
        "Selkirk, Ny", "Toledo, Oh", "Waycross, Ga", "Willard, Oh", "System"
    ],
    "NSC": [
        "NSC",
        "System", "Foreign RR", "Private", "Total  Cars", "Pct. Private",
        "Box", "Covered Hopper", "Gondola", "Intermodal", "Multilevel", "Open Hopper", "Tank", "Other", "Total",
        "Intermodal", "Manifest", "Multilevel", "Coal Unit", "Grain Unit", "All Trains",
        "Allentown, PA", "Bellevue, OH", "Birmingham, AL", "Chattanooga, TN",
        "Columbus, OH", "Conway, PA", "Decatur, IL", "Elkhart, IN", "Atlanta, GA",
        "Linwood, NC", "Macon, GA", "New Orleans, LA", "Roanoke, VA", "Sheffield, AL",
        "Entire Railroad"
    ],
    "UNP": [
        "UNP",
        "System", "Foreign RR", "Private", "Total  Cars", "Pct. Private",
        "Box", "Covered Hopper", "Gondola", "Intermodal", "Multilevel", "Open Hopper", "Tank", "Other", "Total",
        "Intermodal", "Manifest", "Multilevel", "Coal Unit", "Grain Unit", "All Trains",
        "Chicago, IL - Proviso", "Fort Worth, TX", "Hinkle, OR",
        "Houston, TX - Englewood", "Houston, TX - Settegast", "Kansas City, MO",
        "Livonia, LA", "North Little Rock, AR", "Santa Teresa, NM", "North Platte West, NE",
        "Pine Bluff, AR", "Roseville, CA", "West Colton, CA", "Entire Railroad"
    ],
    "CNI": [
        "CNI",
        "Walker Yard (Edmonton), AB", "Fond du Lac Yard, WI", "Jackson Yard, MS", "MacMillan Yard (Toronto), ON",
        "Markham Yard, IL", "Harrison Yard (Memphis), TN", "Symington Yard (Winnipeg), MB",
        "Tascherau Yard (Montreal), QC", "Thornton Yard (Vancouver), BC", "Total Dwell - Major Yards",
        "Entire Railroad", "Intermodal", "Manifest", "Multilevel", "Coal Unit", "Grain Unit", "All Trains",
        "Total Shipments", "Shipments without Bill", "Percent without Customer Bill",
        "System", "Foreign RR", "Private", "Total  Cars",
        "Box", "Covered Hopper", "Gondola", "Intermodal", "Multilevel", "Open Hopper", "Tank", "Other", "Total"
    ],
    "CPKC": [
        "CPKC",
        "System", "Foreign RR", "Private", "Total  Cars", "Pct. Private",
        "Box", "Covered Hopper", "Gondola", "Intermodal", "Multilevel", "Open Hopper", "Tank", "Other", "Total",
        "Intermodal", "Manifest", "Multilevel", "Coal Unit", "Grain Unit", "All Trains",
        "Calgary, AB", "Chicago, IL", "Edmonton, AB", "Vancouver, BC", "Moose Jaw, SK",
        "Montreal, QC", "St Paul, MN", "Thunder Bay, ON", "Toronto, ON", "Winnipeg, MB",
        "Kansas City, MO", "Sanchez, MX", "Shreveport, LA", "Monterrey, CA", "Laredo Yard, TX",
        "San Luis Potosi, MX", "Jackson, MS", "Entire Railroad"
    ]
}

# --------------------
# MAPPING DICTIONARIES (EP724)
# --------------------

mapping_bnsf = {
    "System": "System",
    "Foreign RR": "Foreign RR",
    "Private": "Private",
    "Total Cars": "Total  Cars",
    "% Private": "Pct. Private",
    "Box": "Box",
    "Covered Hopper": "Covered Hopper",
    "Gondola": "Gondola",
    "Intermodal": "Intermodal",
    "Multilevel": "Multilevel",
    "Open Hopper": "Open Hopper",
    "Tank": "Tank",
    "Other": "Other",
    "Total": "Total",
    "Intermodal Trains": "Intermodal",
    "Manifest Trains": "Manifest",
    "Multilevel Trains": "Multilevel",
    "Coal Unit Trains": "Coal Unit",
    "Grain Unit Trains": "Grain Unit",
    "All Trains": "All Trains",
    "Barstow, CA": "Barstow, CA",
    "Denver, CO": "Denver, CO",
    "Fort Worth, TX": "Fort Worth, TX",
    "Galesburg, IL": "Galesburg, IL",
    "Houston, TX": "Houston, TX",
    "Kansas City, KS": "Kansas City, KS",
    "Lincoln, NE": "Lincoln, NE",
    "Memphis, TN": "Memphis, TN",
    "Northtown, MN": "Northtown, MN",
    "Pasco, WA": "Pasco, WA",
    "Tulsa, OK": "Tulsa, OK",
    "Entire Railroad": "Entire Railroad"
}

mapping_csx = {
    "System": "System",
    "Total Cars": "Total  Cars",
    "% Private": "Pct. Private",
    "Box": "Box",
    "Covered Hopper": "Covered Hopper",
    "Gondola": "Gondola",
    "Intermodal": "Intermodal",
    "Multilevel": "Multilevel",
    "Open Hopper": "Open Hopper",
    "Tank": "Tank",
    "Other": "Other",
    "Total": "Total",
    "Coal": "Coal",
    "Crude": "Crude",
    "Ethanol": "Ethanol",
    "Grain": "Grain",
    "Intermodal (Service)": "Intermodal",
    "Merch": "Merch",
    "Chicago, Il": "Chicago, Il",
    "Cincinnati, Oh": "Cincinnati, Oh",
    "Baltimore, Md": "Baltimore, Md",
    "Hamlet, Nc": "Hamlet, Nc",
    "Indianapolis, In": "Indianapolis, In",
    "Jacksonville, Fl": "Jacksonville, Fl",
    "Louisville, Ky": "Louisville, Ky",
    "Nashville, Tn": "Nashville, Tn",
    "Rocky Mount, Nc": "Rocky Mount, Nc",
    "Selkirk, Ny": "Selkirk, Ny",
    "Toledo, Oh": "Toledo, Oh",
    "Waycross, Ga": "Waycross, Ga",
    "Willard, Oh": "Willard, Oh",
    "System (Service)": "System"
}

mapping_nsc = {
    "System": "System",
    "Foreign RR": "Foreign RR",
    "Private": "Private",
    "Total Cars": "Total  Cars",
    "% Private": "Pct. Private",
    "Box": "Box",
    "Covered Hopper": "Covered Hopper",
    "Gondola": "Gondola",
    "Intermodal": "Intermodal",
    "Multilevel": "Multilevel",
    "Open Hopper": "Open Hopper",
    "Tank": "Tank",
    "Other": "Other",
    "Total": "Total",
    "Intermodal Trains": "Intermodal",
    "Manifest Trains": "Manifest",
    "Multilevel Trains": "Multilevel",
    "Coal Unit Trains": "Coal Unit",
    "Grain Unit Trains": "Grain Unit",
    "All Trains": "All Trains",
    "Allentown, PA": "Allentown, PA",
    "Bellevue, OH": "Bellevue, OH",
    "Birmingham, AL": "Birmingham, AL",
    "Chattanooga, TN": "Chattanooga, TN",
    "Columbus, OH": "Columbus, OH",
    "Conway, PA": "Conway, PA",
    "Decatur, IL": "Decatur, IL",
    "Elkhart, IN": "Elkhart, IN",
    "Atlanta, GA": "Atlanta, GA",
    "Linwood, NC": "Linwood, NC",
    "Macon, GA": "Macon, GA",
    "New Orleans, LA": "New Orleans, LA",
    "Roanoke, VA": "Roanoke, VA",
    "Sheffield, AL": "Sheffield, AL",
    "Entire Railroad": "Entire Railroad"
}

mapping_unp = {
    "System": "System",
    "Foreign RR": "Foreign RR",
    "Private": "Private",
    "Total Cars": "Total  Cars",
    "% Private": "Pct. Private",
    "Box": "Box",
    "Covered Hopper": "Covered Hopper",
    "Gondola": "Gondola",
    "Intermodal": "Intermodal",
    "Multilevel": "Multilevel",
    "Open Hopper": "Open Hopper",
    "Tank": "Tank",
    "Other": "Other",
    "Total": "Total",
    "Intermodal Trains": "Intermodal",
    "Manifest Trains": "Manifest",
    "Multilevel Trains": "Multilevel",
    "Coal Unit Trains": "Coal Unit",
    "Grain Unit Trains": "Grain Unit",
    "All Trains": "All Trains",
    "Chicago, IL - Proviso": "Chicago, IL - Proviso",
    "Fort Worth, TX": "Fort Worth, TX",
    "Hinkle, OR": "Hinkle, OR",
    "Houston, TX - Englewood": "Houston, TX - Englewood",
    "Houston, TX - Settegast": "Houston, TX - Settegast",
    "Kansas City, MO": "Kansas City, MO",
    "Livonia, LA": "Livonia, LA",
    "North Little Rock, AR": "North Little Rock, AR",
    "Santa Teresa, NM": "Santa Teresa, NM",
    "North Platte West, NE": "North Platte West, NE",
    "Pine Bluff, AR": "Pine Bluff, AR",
    "Roseville, CA": "Roseville, CA",
    "West Colton, CA": "West Colton, CA",
    "Entire Railroad": "Entire Railroad"
}

# --------------------
# BUILDERS & LOADERS
# --------------------

def build_skeleton(rr):
    return pd.DataFrame({"Category": categories[rr]})

def fill_from_ep724(rr_code, mapping):
    ep724_path = os.path.join(DOWNLOAD_FOLDER, EP724_FILENAME)
    # Load the first sheet (EP724 files typically only have one)
    df_raw = pd.read_excel(ep724_path, sheet_name=0)

    df = build_skeleton(rr_code)
    rr_rows = df_raw[df_raw.iloc[:,0] == rr_code]  # filter by RR code in col 0

    for raw_label, mapped in mapping.items():
        row = rr_rows[rr_rows.iloc[:,1] == raw_label]  # labels are in col 1
        if not row.empty:
            values = row.iloc[0,2:].tolist()
            df.loc[df["Category"] == mapped, df.columns[1]:] = values

    return df

def fill_from_cn():
    df_raw = pd.read_excel(CN_URL, sheet_name="53 Weeks History")
    df = build_skeleton("CNI")
    for idx, row in df_raw.iterrows():
        label = str(row.iloc[0]).strip()
        if label in categories["CNI"]:
            values = row.iloc[2:].tolist()
            df.loc[df["Category"] == label, df.columns[1]:] = values
    return df

def fill_from_cpkc(cpkc_url):
    r = requests.get(cpkc_url)
    tmpfile = os.path.join(DOWNLOAD_FOLDER, "CPKC.xlsx")
    with open(tmpfile, "wb") as f:
        f.write(r.content)
    df_raw = pd.read_excel(tmpfile, sheet_name="Railroad Performance All Years")
    df = build_skeleton("CPKC")
    for idx, row in df_raw.iterrows():
        label = str(row.iloc[0]).strip()
        if label in categories["CPKC"]:
            values = row.iloc[1:].tolist()
            df.loc[df["Category"] == label, df.columns[1]:] = values
    return df

# --------------------
# MAIN
# --------------------
# --------------------
# MAIN
# --------------------
def main():
    ep724_path = download_ep724()
    cpkc_url = get_cpkc_url()

    with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter") as writer:
        # EP724 carriers
        df_bnsf = fill_from_ep724("BNI", mapping_bnsf)
        df_bnsf.to_excel(writer, sheet_name="BNSF", index=False)

        df_csx = fill_from_ep724("CSX", mapping_csx)
        df_csx.to_excel(writer, sheet_name="CSX", index=False)

        df_nsc = fill_from_ep724("NSC", mapping_nsc)
        df_nsc.to_excel(writer, sheet_name="NS", index=False)

        df_unp = fill_from_ep724("UNP", mapping_unp)
        df_unp.to_excel(writer, sheet_name="UP", index=False)

        # CN (direct file)
        df_cn = fill_from_cn()
        df_cn.to_excel(writer, sheet_name="CN", index=False)

        # CPKC (direct file)
        df_cpkc = fill_from_cpkc(cpkc_url)
        df_cpkc.to_excel(writer, sheet_name="CPKC", index=False)

    print(f"✅ All carriers written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
