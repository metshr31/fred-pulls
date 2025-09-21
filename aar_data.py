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

# === EP724 HELPER ===
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
# CATEGORY SKELETONS
# --------------------
categories = {
    "BNSF": [
        "BNSF",
        "System", "Foreign RR", "Private", "Total  Cars", "Pct. Private",
        "Box", "Covered Hopper", "Gondola", "Intermodal", "Multilevel",
        "Open Hopper", "Tank", "Other", "Total",
        "Intermodal", "Manifest", "Multilevel", "Coal Unit", "Grain Unit", "All Trains",
        "Barstow, CA", "Denver, CO", "Fort Worth, TX", "Galesburg, IL", "Houston, TX",
        "Kansas City, KS", "Lincoln, NE", "Memphis, TN", "Northtown, MN", "Pasco, WA",
        "Tulsa, OK", "Entire Railroad"
    ],
    "CSX": [
        "CSX",
        "System", "Total  Cars", "Pct. Private",
        "Box", "Covered Hopper", "Gondola", "Intermodal", "Multilevel", "Open Hopper",
        "Tank", "Other", "Total",
        "Coal", "Crude", "Ethanol", "Grain", "Intermodal", "Merch", "System",
        "Chicago, Il", "Cincinnati, Oh", "Baltimore, Md", "Hamlet, Nc", "Indianapolis, In",
        "Jacksonville, Fl", "Louisville, Ky", "Nashville, Tn", "Rocky Mount, Nc",
        "Selkirk, Ny", "Toledo, Oh", "Waycross, Ga", "Willard, Oh", "System"
    ],
    "NS": [
        "NS",
        "System", "Foreign RR", "Private", "Total  Cars", "Pct. Private",
        "Box", "Covered Hopper", "Gondola", "Intermodal", "Multilevel",
        "Open Hopper", "Tank", "Other", "Total",
        "Intermodal", "Manifest", "Multilevel", "Coal Unit", "Grain Unit", "All Trains",
        "Allentown, PA", "Bellevue, OH", "Birmingham, AL", "Chattanooga, TN",
        "Columbus, OH", "Conway, PA", "Decatur, IL", "Elkhart, IN", "Atlanta, GA",
        "Linwood, NC", "Macon, GA", "New Orleans, LA", "Roanoke, VA", "Sheffield, AL",
        "Entire Railroad"
    ],
    "UP": [
        "UP",
        "System", "Foreign RR", "Private", "Total  Cars", "Pct. Private",
        "Box", "Covered Hopper", "Gondola", "Intermodal", "Multilevel",
        "Open Hopper", "Tank", "Other", "Total",
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
    ("Cars On Line (Count)", "System"): "System",
    ("Cars On Line (Count)", "Foreign RR"): "Foreign RR",
    ("Cars On Line (Count)", "Private"): "Private",
    ("Cars On Line (Count)", "Total"): "Total  Cars",
    ("Cars On Line (Count)", "% Private"): "Pct. Private",
    ("Cars On Line (Count)", "Box"): "Box",
    ("Cars On Line (Count)", "Covered Hopper"): "Covered Hopper",
    ("Cars On Line (Count)", "Gondola"): "Gondola",
    ("Cars On Line (Count)", "Intermodal"): "Intermodal",
    ("Cars On Line (Count)", "Multilevel (automotive)"): "Multilevel",
    ("Cars On Line (Count)", "Open Hopper"): "Open Hopper",
    ("Cars On Line (Count)", "Tank"): "Tank",
    ("Cars On Line (Count)", "Other"): "Other",
    ("Cars On Line (Count)", "Total"): "Total",

    ("Average Train Speed  (MPH)", "Intermodal"): "Intermodal",
    ("Average Train Speed  (MPH)", "Manifest"): "Manifest",
    ("Average Train Speed  (MPH)", "Automotive unit"): "Multilevel",
    ("Average Train Speed  (MPH)", "Coal unit"): "Coal Unit",
    ("Average Train Speed  (MPH)", "Grain unit"): "Grain Unit",
    ("Average Train Speed  (MPH)", "System"): "All Trains",

    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Barstow, CA"): "Barstow, CA",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Denver, CO"): "Denver, CO",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Fort Worth, TX"): "Fort Worth, TX",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Galesburg, IL"): "Galesburg, IL",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Houston, TX"): "Houston, TX",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Kansas City, KS"): "Kansas City, KS",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Lincoln, NE"): "Lincoln, NE",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Memphis, TN"): "Memphis, TN",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Northtown, MN"): "Northtown, MN",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Pasco, WA"): "Pasco, WA",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Tulsa, OK"): "Tulsa, OK",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "System"): "Entire Railroad",
}

mapping_csx = {
    ("Cars On Line (Count)", "System"): "System",
    ("Cars On Line (Count)", "Total"): "Total  Cars",
    ("Cars On Line (Count)", "% Private"): "Pct. Private",
    ("Cars On Line (Count)", "Box"): "Box",
    ("Cars On Line (Count)", "Covered Hopper"): "Covered Hopper",
    ("Cars On Line (Count)", "Gondola"): "Gondola",
    ("Cars On Line (Count)", "Intermodal"): "Intermodal",
    ("Cars On Line (Count)", "Multilevel (automotive)"): "Multilevel",
    ("Cars On Line (Count)", "Open Hopper"): "Open Hopper",
    ("Cars On Line (Count)", "Tank"): "Tank",
    ("Cars On Line (Count)", "Other"): "Other",
    ("Cars On Line (Count)", "Total"): "Total",

    ("Average Train Speed  (MPH)", "Coal unit"): "Coal",
    ("Average Train Speed  (MPH)", "Crude oil unit"): "Crude",
    ("Average Train Speed  (MPH)", "Ethanol unit"): "Ethanol",
    ("Average Train Speed  (MPH)", "Grain unit"): "Grain",
    ("Average Train Speed  (MPH)", "Intermodal"): "Intermodal",
    ("Average Train Speed  (MPH)", "Manifest"): "Merch",
    ("Average Train Speed  (MPH)", "System"): "System",

    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Chicago, Il"): "Chicago, Il",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Cincinnati, Oh"): "Cincinnati, Oh",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Baltimore, Md"): "Baltimore, Md",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Hamlet, Nc"): "Hamlet, Nc",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Indianapolis, In"): "Indianapolis, In",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Jacksonville, Fl"): "Jacksonville, Fl",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Louisville, Ky"): "Louisville, Ky",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Nashville, Tn"): "Nashville, Tn",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Rocky Mount, Nc"): "Rocky Mount, Nc",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Selkirk, Ny"): "Selkirk, Ny",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Toledo, Oh"): "Toledo, Oh",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Waycross, Ga"): "Waycross, Ga",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Willard, Oh"): "Willard, Oh",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "System"): "System",
}

mapping_ns = {
    ("Cars On Line (Count)", "System"): "System",
    ("Cars On Line (Count)", "Foreign RR"): "Foreign RR",
    ("Cars On Line (Count)", "Private"): "Private",
    ("Cars On Line (Count)", "Total"): "Total  Cars",
    ("Cars On Line (Count)", "% Private"): "Pct. Private",
    ("Cars On Line (Count)", "Box"): "Box",
    ("Cars On Line (Count)", "Covered Hopper"): "Covered Hopper",
    ("Cars On Line (Count)", "Gondola"): "Gondola",
    ("Cars On Line (Count)", "Intermodal"): "Intermodal",
    ("Cars On Line (Count)", "Multilevel (automotive)"): "Multilevel",
    ("Cars On Line (Count)", "Open Hopper"): "Open Hopper",
    ("Cars On Line (Count)", "Tank"): "Tank",
    ("Cars On Line (Count)", "Other"): "Other",
    ("Cars On Line (Count)", "Total"): "Total",

    ("Average Train Speed  (MPH)", "Intermodal"): "Intermodal",
    ("Average Train Speed  (MPH)", "Manifest"): "Manifest",
    ("Average Train Speed  (MPH)", "Automotive unit"): "Multilevel",
    ("Average Train Speed  (MPH)", "Coal unit"): "Coal Unit",
    ("Average Train Speed  (MPH)", "Grain unit"): "Grain Unit",
    ("Average Train Speed  (MPH)", "System"): "All Trains",

    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Allentown, PA"): "Allentown, PA",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Bellevue, OH"): "Bellevue, OH",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Birmingham, AL"): "Birmingham, AL",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Chattanooga, TN"): "Chattanooga, TN",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Columbus, OH"): "Columbus, OH",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Conway, PA"): "Conway, PA",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Decatur, IL"): "Decatur, IL",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Elkhart, IN"): "Elkhart, IN",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Atlanta, GA"): "Atlanta, GA",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Linwood, NC"): "Linwood, NC",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Macon, GA"): "Macon, GA",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "New Orleans, LA"): "New Orleans, LA",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Roanoke, VA"): "Roanoke, VA",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Sheffield, AL"): "Sheffield, AL",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "System"): "Entire Railroad",
}

mapping_up = {
    ("Cars On Line (Count)", "System"): "System",
    ("Cars On Line (Count)", "Foreign RR"): "Foreign RR",
    ("Cars On Line (Count)", "Private"): "Private",
    ("Cars On Line (Count)", "Total"): "Total  Cars",
    ("Cars On Line (Count)", "% Private"): "Pct. Private",
    ("Cars On Line (Count)", "Box"): "Box",
    ("Cars On Line (Count)", "Covered Hopper"): "Covered Hopper",
    ("Cars On Line (Count)", "Gondola"): "Gondola",
    ("Cars On Line (Count)", "Intermodal"): "Intermodal",
    ("Cars On Line (Count)", "Multilevel (automotive)"): "Multilevel",
    ("Cars On Line (Count)", "Open Hopper"): "Open Hopper",
    ("Cars On Line (Count)", "Tank"): "Tank",
    ("Cars On Line (Count)", "Other"): "Other",
    ("Cars On Line (Count)", "Total"): "Total",

    ("Average Train Speed  (MPH)", "Intermodal"): "Intermodal",
    ("Average Train Speed  (MPH)", "Manifest"): "Manifest",
    ("Average Train Speed  (MPH)", "Automotive unit"): "Multilevel",
    ("Average Train Speed  (MPH)", "Coal unit"): "Coal Unit",
    ("Average Train Speed  (MPH)", "Grain unit"): "Grain Unit",
    ("Average Train Speed  (MPH)", "System"): "All Trains",

    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Chicago, IL - Proviso"): "Chicago, IL - Proviso",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Fort Worth, TX"): "Fort Worth, TX",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Hinkle, OR"): "Hinkle, OR",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Houston, TX - Englewood"): "Houston, TX - Englewood",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Houston, TX - Settegast"): "Houston, TX - Settegast",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Kansas City, MO"): "Kansas City, MO",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Livonia, LA"): "Livonia, LA",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "North Little Rock, AR"): "North Little Rock, AR",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Santa Teresa, NM"): "Santa Teresa, NM",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "North Platte West, NE"): "North Platte West, NE",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Pine Bluff, AR"): "Pine Bluff, AR",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "Roseville, CA"): "Roseville, CA",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "West Colton, CA"): "West Colton, CA",
    ("Average Terminal Dwell Time (Excluding Cars on Line)", "System"): "Entire Railroad",
}


# --------------------
# BUILDERS & LOADERS
# --------------------

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

# === EP724 HELPER ===
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
# CATEGORY SKELETONS
# --------------------
categories = {
    "BNSF": [
        "BNSF",
        "System", "Foreign RR", "Private", "Total  Cars", "Pct. Private",
        "Box", "Covered Hopper", "Gondola", "Intermodal", "Multilevel",
        "Open Hopper", "Tank", "Other", "Total",
        "Intermodal", "Manifest", "Multilevel", "Coal Unit", "Grain Unit", "All Trains",
        "Barstow, CA", "Denver, CO", "Fort Worth, TX", "Galesburg, IL", "Houston, TX",
        "Kansas City, KS", "Lincoln, NE", "Memphis, TN", "Northtown, MN", "Pasco, WA",
        "Tulsa, OK", "Entire Railroad"
    ],
    "CSX": [
        "CSX",
        "System", "Total  Cars", "Pct. Private",
        "Box", "Covered Hopper", "Gondola", "Intermodal", "Multilevel", "Open Hopper",
        "Tank", "Other", "Total",
        "Coal", "Crude", "Ethanol", "Grain", "Intermodal", "Merch", "System",
        "Chicago, Il", "Cincinnati, Oh", "Baltimore, Md", "Hamlet, Nc", "Indianapolis, In",
        "Jacksonville, Fl", "Louisville, Ky", "Nashville, Tn", "Rocky Mount, Nc",
        "Selkirk, Ny", "Toledo, Oh", "Waycross, Ga", "Willard, Oh", "System"
    ],
    "NS": [
        "NS",
        "System", "Foreign RR", "Private", "Total  Cars", "Pct. Private",
        "Box", "Covered Hopper", "Gondola", "Intermodal", "Multilevel",
        "Open Hopper", "Tank", "Other", "Total",
        "Intermodal", "Manifest", "Multilevel", "Coal Unit", "Grain Unit", "All Trains",
        "Allentown, PA", "Bellevue, OH", "Birmingham, AL", "Chattanooga, TN",
        "Columbus, OH", "Conway, PA", "Decatur, IL", "Elkhart, IN", "Atlanta, GA",
        "Linwood, NC", "Macon, GA", "New Orleans, LA", "Roanoke, VA", "Sheffield, AL",
        "Entire Railroad"
    ],
    "UP": [
        "UP",
        "System", "Foreign RR", "Private", "Total  Cars", "Pct. Private",
        "Box", "Covered Hopper", "Gondola", "Intermodal", "Multilevel",
        "Open Hopper", "Tank", "Other", "Total",
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
# BUILDERS & LOADERS
# --------------------
def build_skeleton(rr):
    return pd.DataFrame({"Category": categories[rr]})

def fill_from_ep724(rr_code, mapping):
    """
    Build skeleton for a railroad and fill values from EP724 file
    using (metric, label) tuple keys from mapping.
    """
    ep724_path = os.path.join(DOWNLOAD_FOLDER, EP724_FILENAME)

    # Load the first sheet (EP724 files typically only have one)
    df_raw = pd.read_excel(ep724_path, sheet_name=0)

    # Build skeleton for the given railroad
    df = build_skeleton(rr_code)

    # Filter rows for this RR
    rr_rows = df_raw[df_raw.iloc[:, 0] == rr_code]

    # Add week columns dynamically
    week_cols = df_raw.columns[3:].tolist()
    for col in week_cols:
        df[col] = None

    # Map values into the skeleton
    for (metric, label), mapped in mapping.items():
        row = rr_rows[(rr_rows.iloc[:, 1] == metric) & (rr_rows.iloc[:, 2] == label)]
        if not row.empty:
            values = row.iloc[0, 3:].tolist()
            df.loc[df["Category"] == mapped, week_cols] = values

    return df

def fill_from_cn():
    df_raw = pd.read_excel(CN_URL, sheet_name="53 Weeks History")

    # Build skeleton
    df = build_skeleton("CNI")

    # Add week columns from CN file (everything after the first 2 cols)
    week_cols = df_raw.columns[2:].tolist()
    for col in week_cols:
        df[col] = None

    # Fill values into skeleton
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

    df_raw = pd.read_excel(tmpfile, sheet_name="Railroad Performance All Years")

    # Build skeleton
    df = build_skeleton("CPKC")

    # Add week columns from CPKC file (everything after the first col)
    week_cols = df_raw.columns[1:].tolist()
    for col in week_cols:
        df[col] = None

    # Fill values into skeleton
    for idx, row in df_raw.iterrows():
        label = str(row.iloc[0]).strip()
        if label in categories["CPKC"]:
            values = row.iloc[1:].tolist()
            df.loc[df["Category"] == label, week_cols] = values

    return df


# --------------------
# MAIN
# --------------------
def main():
    # Download EP724 and get CPKC URL
    ep724_path = download_ep724()
    cpkc_url = get_cpkc_url()

    with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter") as writer:
        # EP724 carriers
        df_bnsf = fill_from_ep724("BNSF", mapping_bnsf)
        df_bnsf.to_excel(writer, sheet_name="BNSF", index=False)

        df_csx = fill_from_ep724("CSX", mapping_csx)
        df_csx.to_excel(writer, sheet_name="CSX", index=False)

        df_ns = fill_from_ep724("NS", mapping_ns)
        df_ns.to_excel(writer, sheet_name="NS", index=False)

        df_up = fill_from_ep724("UP", mapping_up)
        df_up.to_excel(writer, sheet_name="UP", index=False)

        # CN (direct URL file)
        df_cn = fill_from_cn()
        df_cn.to_excel(writer, sheet_name="CN", index=False)

        # CPKC (direct URL file)
        df_cpkc = fill_from_cpkc(cpkc_url)
        df_cpkc.to_excel(writer, sheet_name="CPKC", index=False)

    print(f"✅ All carriers written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
