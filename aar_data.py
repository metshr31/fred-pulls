import os
import re
import datetime as dt
import requests
from bs4 import BeautifulSoup

# === CONFIG ===
# URL for the Surface Transportation Board (STB) Rail Service Data
STB_URL = "https://www.stb.gov/reports-data/rail-service-data/"
# Direct URL for CN's main performance spreadsheet (often stable)
CN_PERF_URL = "https://www.cn.ca/-/media/files/investors/investor-performance-measures/perf_measures_en.xlsx"
# CN's public metrics page (for RTMs/Carloads links)
CN_METRICS_PAGE = "https://www.cn.ca/en/investors/key-weekly-metrics/"

# CSX Investor Relations page (used for fallback link scraping)
CSX_METRICS_PAGE = "https://investors.csx.com/metrics/default.aspx"
# Base URL for CSX's CDN where weekly reports are hosted
CSX_CDN_BASE = "https://s2.q4cdn.com/859568992/files/doc_downloads"

# Base URL for CPKC's CDN where weekly reports are hosted
CPKC_CDN_BASE = "https://s21.q4cdn.com/736796105/files/doc_downloads"
# Standardized part of the CPKC 53-week report filename
CPKC_53WEEK_FILENAME = "CPKC-53-Week-Railway-Performance-Report.xlsx"

# Set download directory based on environment variable or current working directory
DOWNLOAD_FOLDER = os.getenv("STB_LOG_DIR", os.getcwd())
TIMEOUT = 15
# User Agent for requests (identifying the script)
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) excel-fetcher"}

# --- Utility functions ---
def ensure_dir(path): os.makedirs(path, exist_ok=True)
def datestamp(): return dt.date.today().strftime("%Y-%m-%d")
def sanitize_filename(name): return re.sub(r"[^\w\-.]+", "_", name)

def save_bytes(content, filename):
    ensure_dir(DOWNLOAD_FOLDER)
    full = os.path.join(DOWNLOAD_FOLDER, filename)
    with open(full, "wb") as f: f.write(content)
    print(f"✅ Saved: {full}")
    return full

def http_get(url):
    r = requests.get(url, headers=UA, timeout=TIMEOUT)
    r.raise_for_status()
    return r

def http_head_ok(url):
    """Checks if a URL exists and returns a non-HTML file (or any 200 OK non-HTML)."""
    try:
        r = requests.head(url, headers=UA, timeout=TIMEOUT, allow_redirects=True)
        # Check for 200 OK and ensure it's not a generic redirect to an HTML error page
        if r.status_code == 200 and "text/html" not in r.headers.get("Content-Type", "").lower():
            return True
    except requests.RequestException:
        # Ignore network/timeout errors
        return False
    return False

# --------------------------
# === EP724 (STB) ===
# --------------------------
def get_latest_ep724_url():
    """Scrapes the STB page for the latest EP724 .xlsx link."""
    r = http_get(STB_URL)
    soup = BeautifulSoup(r.text, "html.parser")
    # Find all links containing "EP724" and ending in ".xlsx"
    links = [a["href"] for a in soup.find_all("a", href=True) if "EP724" in a["href"] and a["href"].endswith(".xlsx")]
    
    if not links: 
        raise FileNotFoundError("No EP724 .xlsx link found on STB page.")
    
    # Assuming the latest file has the alphabetically largest name/date
    links.sort()
    url = links[-1]
    
    # Construct absolute URL if it's relative
    if not url.startswith("http"): url = "https://www.stb.gov" + url
    return url

def download_ep724():
    """Downloads the latest EP724 data file."""
    print("\n--- Scraping STB EP724 ---")
    url = get_latest_ep724_url()
    print(f"⬇️ EP724 found at {url}")
    resp = http_get(url)
    fname = f"EP724_{datestamp()}.xlsx"
    return save_bytes(resp.content, fname)

# --------------------------
# === CN Performance ===
# --------------------------
def download_cn_perf():
    """Downloads the static, main CN Performance Report XLSX file."""
    print("\n--- Scraping CN Performance (Direct Link) ---")
    resp = http_get(CN_PERF_URL)
    fname = f"CN_Performance_{datestamp()}.xlsx"
    return save_bytes(resp.content, fname)

# --------------------------
# === CN RTM Summary ===
# --------------------------
def download_cn_rtm():
    """Scrapes the CN metrics page for all current RTM/Carload XLSX links."""
    print("\n--- Scraping CN RTM (Metrics Page) ---")
    r = http_get(CN_METRICS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    # Find all XLSX links on the page
    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".xlsx")]
    
    if not links: 
        raise FileNotFoundError("No CN RTM .xlsx link found on metrics page.")
    
    saved = []
    for url in links:
        # Normalize relative URLs
        if url.startswith("//"): url = "https:" + url
        elif url.startswith("/"): url = "https://www.cn.ca" + url
            
        fname_base = url.split("/")[-1]
        print(f"⬇️ Downloading CN RTM {fname_base}")
        try:
            resp = http_get(url)
            # Add date and the base file name to prevent overwriting
            custom_name = f"CN_RTM_{datestamp()}_{fname_base}"
            saved.append(save_bytes(resp.content, custom_name))
        except Exception as e:
             print(f"❌ Failed to download {url}: {e}")
             
    if not saved:
        raise Exception("CN RTM links were found but failed to download.")
    return saved

# --------------------------
# === CPKC (Canadian Pacific Kansas City) ===
# --------------------------

def discover_cpkc_53week_url():
    """Attempts to guess the URL for the CPKC 53-week report based on the last two Mondays."""
    today = dt.date.today()
    # Find the date of the last Monday
    offset = (today.weekday() - 0) % 7 # Monday is 0
    last_monday = today - dt.timedelta(days=offset)
    
    # Check the last Monday and the Monday before that
    candidates = [last_monday, last_monday - dt.timedelta(days=7)]
    
    for d in candidates:
        # Construct the URL based on the CDN folder structure (Year/Month/Day)
        url = f"{CPKC_CDN_BASE}/{d.strftime('%Y/%m/%d')}/{CPKC_53WEEK_FILENAME}"
        if http_head_ok(url): 
            return url
        
    raise FileNotFoundError("CPKC 53-week file not found for last two Mondays.")

def download_cpkc_53week():
    """Downloads the CPKC 53-week performance file."""
    print("\n--- Scraping CPKC 53-week Report ---")
    url = discover_cpkc_53week_url()
    print(f"⬇️ CPKC 53-Week found at {url}")
    resp = http_get(url)
    fname = f"CPKC_53_Week_{datestamp()}.xlsx"
    return save_bytes(resp.content, fname)

def discover_cpkc_rtm_url():
    """Attempts to guess the URL for the CPKC Weekly RTMs and Carloads, checking the last 14 days."""
    today = dt.date.today()
    for delta in range(0, 14):
        d = today - dt.timedelta(days=delta)
        # File name convention uses the current year in the name
        url = f"{CPKC_CDN_BASE}/{d.strftime('%Y/%m/%d')}/CPKC-Weekly-RTMs-and-Carloads-{d.year}.xlsx"
        if http_head_ok(url): 
            return url
        
    raise FileNotFoundError("CPKC Weekly RTM/Carloads not found in last 14 days.")

def download_cpkc_rtm():
    """Downloads the CPKC Weekly RTMs and Carloads file."""
    print("\n--- Scraping CPKC Weekly RTM/Carloads ---")
    url = discover_cpkc_rtm_url()
    print(f"⬇️ CPKC RTM/Carloads found at {url}")
    resp = http_get(url)
    fname = f"CPKC_Weekly_RTM_{datestamp()}.xlsx"
    return save_bytes(resp.content, fname)

# --------------------------
# === CSX ===
# --------------------------

def _iso_week_year(date_obj): 
    """Returns the ISO Year and Week Number for CDN file naming."""
    return date_obj.isocalendar()[0], date_obj.isocalendar()[1]

def _csx_candidate_filenames(year, week):
    """Returns potential CSX file names based on week/year convention."""
    return [
        f"Historical_Data_Week_{week}_{year}.xlsx",
        # Example of a known filename convention for newer data
        f"Combined-Intermodal-and-Carload-TPC-Week-1-{year}-Week-{week}-{year}.xlsx",
    ]

def discover_csx_url(max_back_days=10):
    """
    Attempts to guess the CSX CDN URL based on recent weeks, then falls back 
    to scraping the metrics page.
    """
    print("\n--- Scraping CSX Metrics ---")
    today = dt.date.today()
    # Calculate the end of the previous rail week (usually Sunday/Monday reporting)
    last_week_end = today - dt.timedelta(days=today.weekday() + 2) 
    year, week = _iso_week_year(last_week_end)
    
    # 1. Attempt CDN Guessing
    for delta in range(max_back_days):
        d = today - dt.timedelta(days=delta)
        folder = d.strftime("%Y/%m/%d")
        for fname in _csx_candidate_filenames(year, week):
            url = f"{CSX_CDN_BASE}/{folder}/{fname}"
            if http_head_ok(url): 
                print(f"✅ Found CSX URL via CDN guess: {url}")
                return url
            
    # 2. Fallback to Scraping the Metrics Page
    print("⚠️ CDN guess failed. Falling back to scraping metrics page.")
    try:
        r = http_get(CSX_METRICS_PAGE)
        soup = BeautifulSoup(r.text, "html.parser")
        # Find all direct links to XLSX files
        links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".xlsx")]
        for u in links:
            # Normalize URL
            if u.startswith("//"): u = "https:" + u
            elif u.startswith("/"): u = "https://investors.csx.com" + u
            
            # Use the first valid Excel link found
            if http_head_ok(u): 
                print(f"✅ Found CSX URL via scrape: {u}")
                return u
    except Exception as e:
        # Log scrape error but continue to final FileNotFoundError
        print(f"❌ CSX scrape fallback failed: {e}")
        
    raise FileNotFoundError("CSX Excel not found via CDN guess or metrics page scrape.")

def download_csx():
    """Downloads the discovered CSX Excel file."""
    url = discover_csx_url()
    resp = http_get(url)
    # Use the filename from the server, cleaned up
    server_name = url.rstrip("/").rsplit("/", 1)[-1]
    fname = sanitize_filename(f"CSX_{datestamp()}_{server_name}")
    return save_bytes(resp.content, fname)

# --------------------------
# === MAIN EXECUTION ===
# --------------------------
def main():
    print(f"📂 Download folder: {DOWNLOAD_FOLDER}")
    fetched = []
    
    # List of functions to execute
    tasks = [
        ("STB EP724", download_ep724),
        ("CN Performance (Direct)", download_cn_perf),
        ("CN RTM (Scrape)", download_cn_rtm),
        ("CPKC 53-week", download_cpkc_53week),
        ("CPKC Weekly RTM", download_cpkc_rtm),
        ("CSX", download_csx),
    ]
    
    for name, fn in tasks:
        try:
            result = fn()
            # Handle both single file returns and list returns
            if isinstance(result, list): fetched.extend(result)
            else: fetched.append(result)
        except Exception as e:
            print(f"❌ {name} failed: {e}")
            
    print("\n--------------------------------")
    if fetched:
        print("✅ SCRAPING COMPLETE: Files downloaded:")
        for f in fetched: print(" •", f)
    else:
        print("❌ SCRAPING COMPLETE: No files downloaded.")

if __name__ == "__main__":
    main()
