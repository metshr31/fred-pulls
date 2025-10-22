import os
import re
import time
import datetime as dt
import requests
from bs4 import BeautifulSoup
from typing import List, Union
from urllib.parse import urljoin

# =========================
# Config
# =========================
STB_URL = "https://www.stb.gov/reports-data/rail-service-data/"
CN_PERF_URL = "https://www.cn.ca/-/media/files/investors/investor-performance-measures/perf_measures_en.xlsx"
CN_METRICS_PAGE = "https://www.cn.ca/en/investors/key-weekly-metrics/"
CSX_CDN_BASE = "https://s2.q4cdn.com/859568992/files/doc_downloads"
CPKC_CDN_BASE = "https://s21.q4cdn.com/736796105/files/doc_downloads"
CPKC_53WEEK_FILENAME = "CPKC-53-Week-Railway-Performance-Report.xlsx"
NS_REPORTS_PAGE = "https://norfolksouthern.investorroom.com/weekly-performance-reports"
BNSF_REPORTS_PAGE = "https://www.bnsf.com/about-bnsf/financial-information/weekly-carload-reports/"
DOWNLOAD_FOLDER = os.getenv("STB_LOG_DIR", os.getcwd())
TIMEOUT_DEFAULT = 15 # Lowered as suggested in previous discussion
TIMEOUT_UP = 30      # Lowered, but will not be used
UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) excel-fetcher",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# =========================
# Utilities
# =========================
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def datestamp() -> str:
    return dt.date.today().strftime("%Y-%m-%d")

def sanitize_filename(name: str) -> str:
    # Use re.sub to remove characters that are not alphanumeric, hyphen, period, or underscore
    return re.sub(r"[^\w\-.]+", "_", name)

def save_bytes(content: bytes, filename: str) -> str:
    ensure_dir(DOWNLOAD_FOLDER)
    full = os.path.join(DOWNLOAD_FOLDER, filename)
    with open(full, "wb") as f:
        f.write(content)
    print(f"✅ Saved: {full}")
    return full

def http_get(url: str, timeout: Union[int, None] = None, referer: Union[str, None] = None,
             retries: int = 3, backoff: int = 5) -> requests.Response:
    headers = dict(UA)
    if referer: headers["Referer"] = referer
    # TIMEOUT_UP check simplified since UP is removed, but we keep the structure for safety
    t = (timeout or TIMEOUT_DEFAULT)
    
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=t, allow_redirects=True, stream=False)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == retries:
                raise
            print(f"⚠️ Attempt {attempt} failed for {url}: {e} — retrying in {backoff}s")
            time.sleep(backoff)
    raise requests.RequestException(f"Failed to get {url} after {retries} attempts.")

def http_head_ok(url: str, timeout: Union[int, None] = None) -> bool:
    try:
        r = requests.head(url, headers=UA, timeout=timeout or TIMEOUT_DEFAULT, allow_redirects=True)
        r.raise_for_status() # Check for 4xx/5xx errors
        ctype = r.headers.get("Content-Type", "").lower()
        # Fallback to GET/stream if HEAD is blocked or returns generic HTML
        if r.status_code != 200 or "text/html" in ctype:
            raise requests.RequestException("HEAD failed or returned HTML, trying GET.")
        return True
    except requests.RequestException:
        try:
            # Fallback to GET/stream to test URL accessibility
            r = requests.get(url, headers=UA, timeout=timeout or TIMEOUT_DEFAULT, allow_redirects=True, stream=True)
            r.raise_for_status()
            ctype = r.headers.get("Content-Type", "").lower()
            r.close()
            return "text/html" not in ctype
        except requests.RequestException:
            return False
    except Exception:
        return False

def normalize_url(base: str, href: str) -> Union[str, None]:
    if not href: return None
    href = href.strip()
    if href.startswith("//"): return "https:" + href
    if href.startswith("http"): return href
    return urljoin(base, href)

# =========================
# STB - EP724
# =========================
def get_latest_ep724_url() -> str:
    r = http_get(STB_URL)
    soup = BeautifulSoup(r.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if "EP724" in a["href"] and a["href"].endswith(".xlsx")]
    if not links: raise FileNotFoundError("No EP724 .xlsx link found")
    links.sort()
    return normalize_url("https://www.stb.gov", links[-1])

def download_ep724() -> str:
    resp = http_get(get_latest_ep724_url())
    return save_bytes(resp.content, f"EP724_{datestamp()}.xlsx")

# =========================
# CN
# =========================
def download_cn_perf() -> str:
    resp = http_get(CN_PERF_URL)
    return save_bytes(resp.content, f"CN_Performance_{datestamp()}.xlsx")

def download_cn_rtm() -> List[str]:
    r = http_get(CN_METRICS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".xlsx")]
    if not links: raise FileNotFoundError("No CN RTM .xlsx link found")
    saved = []
    for url in links:
        url = normalize_url("https://www.cn.ca", url)
        fname = url.split("/")[-1]
        print(f"⬇️ CN RTM {fname}")
        resp = http_get(url)
        saved.append(save_bytes(resp.content, f"CN_RTM_{datestamp()}_{fname}"))
        time.sleep(0.5)
    return saved

# =========================
# CPKC (FIXED)
# =========================

def _get_most_recent_monday(today: dt.date) -> dt.date:
    """
    Calculates the date of the most recent Monday, including today if today is Monday.
    (Monday is weekday 1 in Python's isoweekday() system).
    """
    # Calculate how many days back the last Monday was
    # isoweekday: Mon=1, Tue=2, ... Sun=7
    days_since_monday = today.isoweekday() - 1
    
    # If today is Monday, days_since_monday is 0.
    return today - dt.timedelta(days=days_since_monday)

def _discover_cpkc_weekly_rtm_url(filename: str, max_back_days: int = 60) -> str:
    """
    Probes CPKC's CDN for the *dated* weekly RTM/Carloads file.
    FIX: Now prioritizes the folder based on the most recent Monday.
    """
    WEEKLY_PATH = "key-metrics/weekly"
    today = dt.date.today()
    
    # Calculate the target date (the Monday for the latest reporting week)
    target_date = _get_most_recent_monday(today)
    
    # --- Primary Check: Target Monday's folder and the current month's folder ---
    
    # 1. YYYY/MM/DD (e.g., 2025/10/20) - Highest priority
    primary_folder = target_date.strftime("%Y/%m/%d")
    
    # 2. YYYY/MM (e.g., 2025/10) - Fallback, as some CDN providers use less specific folders
    secondary_folder = target_date.strftime("%Y/%m")
    
    checked_folders = [primary_folder, secondary_folder]

    for folder in checked_folders:
        url = f"{CPKC_CDN_BASE}/{WEEKLY_PATH}/{folder}/{filename}"
        print(f"      Probing PRIMARY target folder: {url}")
        if http_head_ok(url):
            print(f"✅ Found CPKC Weekly file (Latest Monday: {target_date.strftime('%Y-%m-%d')}): {url}")
            return url

    # --- Fallback Check: Back-search (if the file isn't uploaded yet for this week) ---
    print("⚠️ Primary check failed. Falling back to backward-discovery...")
    # Start from yesterday and go backwards
    for delta in range(1, max_back_days + 1):
        d = today - dt.timedelta(days=delta)
        
        # Optimize fallback search: Only check folder dates that are Mondays
        if d.isoweekday() == 1: # 1 is Monday
            for folder in (d.strftime("%Y/%m/%d"), d.strftime("%Y/%m")):
                url = f"{CPKC_CDN_BASE}/{WEEKLY_PATH}/{folder}/{filename}"
                print(f"      Probing FALLBACK: {url}")
                if http_head_ok(url, timeout=5):
                    print(f"✅ Found CPKC Weekly file (Folder Date: {d.strftime('%Y-%m-%d')}): {url}")
                    return url

    raise FileNotFoundError(
        f"CPKC weekly RTM/Carloads file ({filename}) not found. "
        f"Primary search failed for folder date: {target_date.strftime('%Y/%m/%d')}. "
        f"Backward search of {max_back_days} days also failed."
    )


def download_cpkc_rtm() -> str:
    """
    Downloads the dynamic CPKC Weekly RTMs and Carloads file, using the new logic
    to find the correct dated folder.
    """
    current_year = dt.date.today().year
    filename = f"CPKC-Weekly-RTMs-and-Carloads-{current_year}.xlsx"
    
    # Use the specific logic for weekly RTM files
    url = _discover_cpkc_weekly_rtm_url(filename)
    resp = http_get(url)
    
    # Save using the specific filename found
    return save_bytes(resp.content, sanitize_filename(f"CPKC_Weekly_RTM_{current_year}.xlsx"))


def download_cpkc_53week() -> str:
    """Downloads the static CPKC 53-Week Railway Performance Report."""
    # The URL is fixed, but includes the path component
    url = f"{CPKC_CDN_BASE}/key-metrics/weekly/{CPKC_53WEEK_FILENAME}"
    print(f"    Probing fixed 53-Week file: {url}")

    # http_get will raise an exception if it can't find it, or if status is not 200
    resp = http_get(url)
    fname = sanitize_filename(f"CPKC_53_Week_Performance.xlsx")
    return save_bytes(resp.content, fname)

# =========================
# CSX Excel (Historical_Data only) (FIXED)
# =========================
def discover_csx_historical(max_back_weeks: int = 12) -> str:
    """
    Searches for multiple filename patterns across a 14-day posting window.
    """
    today = dt.date.today()
    tried = []

    # The outer loop rolls back the ISO week number for the FILENAME
    for delta in range(max_back_weeks):
        # Calculate the year and week we are looking for (Week 43, 42, 41...)
        d_target_week = today - dt.timedelta(weeks=delta)
        target_year, target_week, _ = d_target_week.isocalendar()
        
        # --- Candidate Filenames for the Target Week ---
        filenames = [
            f"Historical_Data_Week_{target_week}_{target_year}.xlsx",
            "Historical_Data.xlsx" # Check for the generic file name
        ]

        # The inner loop iterates through recent daily folders for POSTING LAG
        for day_delta in range(0, 14):
            d_folder = today - dt.timedelta(days=day_delta)
            folder = d_folder.strftime("%Y/%m/%d")

            for fname in filenames:
                url = f"{CSX_CDN_BASE}/{folder}/{fname}"
                tried.append(url)
                print(f"      Probing: {url}")
                if http_head_ok(url):
                    print(f"✅ Found CSX Historical Data: {url}")
                    return url
    
    raise FileNotFoundError(f"❌ Could not find CSX Historical_Data file. Tried: {tried[-5:]} (and more)")

def download_csx() -> str:
    url = discover_csx_historical()
    resp = http_get(url)
    # Use the discovered file name to save
    fname = sanitize_filename(f"CSX_Historical_{os.path.basename(url)}")
    return save_bytes(resp.content, fname)

# =========================
# CSX AAR (PDF) (FIXED)
# =========================
def download_csx_aar(max_back_weeks: int = 12) -> str:
    """
    Searches multiple possible URL structures for the AAR PDF, starting with the
    most recent ISO week.
    """
    today = dt.date.today()
    tried_urls = []
    
    for delta in range(max_back_weeks):
        d = today - dt.timedelta(weeks=delta)
        year, week, _ = d.isocalendar()
        
        # Candidate URLs, from most common to fallbacks
        candidate_urls = [
            # 1. Primary expected location
            f"{CSX_CDN_BASE}/volume_trends/{year}/{year}-Week-{week}-AAR.pdf",
            # 2. Secondary location (Sometimes they drop the PDF right into the general file_downloads folder)
            f"{CSX_CDN_BASE}/{year}-Week-{week}-AAR.pdf",
            # 3. Third location: Direct link inside a recent daily folder (for posting lag)
            f"{CSX_CDN_BASE}/{d.strftime('%Y/%m/%d')}/{year}-Week-{week}-AAR.pdf",
        ]

        for url in candidate_urls:
            print(f"      Probing AAR: {url}")
            tried_urls.append(url)
            if http_head_ok(url):
                print(f"⬇️ CSX AAR PDF found: {url}")
                resp = http_get(url)
                fname = sanitize_filename(f"CSX_AAR_{year}-Week-{week}.pdf")
                return save_bytes(resp.content, fname)

    raise FileNotFoundError(f"❌ No CSX AAR PDF found in last {max_back_weeks} weeks. Tried {tried_urls[-5:]}")

# =========================
# NS
# =========================
def download_ns() -> List[str]:
    r = http_get(NS_REPORTS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    saved = []

    perf_link = None
    regex_perf = re.compile(r"(weekly[-_ ]?performance.*\.xlsx|speed[-_]?dwell[-_]?cars[-_]?on[-_]?line.*\.xlsx)", re.IGNORECASE)

    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if href.endswith(".xlsx") and regex_perf.search(href):
            perf_link = normalize_url(NS_REPORTS_PAGE, a["href"])
            break

    if perf_link:
        print(f"⬇️ NS Weekly Performance XLSX: {perf_link}")
        resp = http_get(perf_link, referer=NS_REPORTS_PAGE)
        saved.append(save_bytes(resp.content, f"NS_Performance_{datestamp()}.xlsx"))
    else:
        print("⚠️ No Weekly Performance XLSX found")

    today = dt.date.today()
    year_str = str(today.year)
    month_now = today.strftime("%B").lower()
    month_prev = (today.replace(day=1) - dt.timedelta(days=1)).strftime("%B").lower()

    def find_carloads(month_lower: str) -> Union[str, None]:
        target = f"investor-weekly-carloads-{month_lower}-{year_str}.pdf"
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if target in href:
                return normalize_url(NS_REPORTS_PAGE, a["href"])
        return None

    carload_link = find_carloads(month_now) or find_carloads(month_prev)
    if carload_link:
        print(f"⬇️ NS Carloading PDF: {carload_link}")
        resp = http_get(carload_link, referer=NS_REPORTS_PAGE)
        saved.append(save_bytes(resp.content, f"NS_Carloads_{datestamp()}.pdf"))
    else:
        print(f"⚠️ No Carloading Report PDF found for {month_now}/{year_str} or fallback to {month_prev}")

    if not saved:
        raise FileNotFoundError("NS reports not found")
    return saved

# =========================
# BNSF
# =========================
def download_bnsf() -> str:
    """
    Downloads the latest BNSF Weekly Surface Transportation Board Update Excel file
    from the BNSF customer notifications page.
    """
    url = "https://www.bnsf.com/news-media/customer-notifications/notification.page?notId=weekly-surface-transportation-board-update"
    base = "https://www.bnsf.com"
    resp = http_get(url)
    soup = BeautifulSoup(resp.text, "html.parser")

    # Find the link containing 'stb-update-report'
    link_tag = soup.find("a", href=re.compile(r"stb-update-report", re.IGNORECASE))
    if not link_tag or not link_tag.get("href"):
        raise FileNotFoundError("❌ Could not find BNSF STB Update link on the page.")

    file_url = normalize_url(base, link_tag["href"])
    print(f"⬇️ Found BNSF STB Update: {file_url}")

    file_resp = http_get(file_url)
    filename = f"BNSF_STB_Update_{datestamp()}.xlsx"
    return save_bytes(file_resp.content, filename)

# =========================
# Main
# =========================
def download_all():
    print(f"📂 Download folder: {DOWNLOAD_FOLDER}")
    fetched: List[str] = []
    tasks = [
        ("EP724 (STB)", download_ep724),
        ("CN Performance", download_cn_perf),
        ("CN RTM", download_cn_rtm),
        ("CPKC 53-week", download_cpkc_53week),
        ("CPKC Weekly RTM", download_cpkc_rtm),
        ("CSX Excel", download_csx),
        ("CSX AAR", download_csx_aar),
        # ("UP", download_up) - Removed as requested
        ("NS", download_ns),
        ("BNSF", download_bnsf),
    ]
    for name, fn in tasks:
        try:
            print(f"\n🚀 Running {name}...")
            result = fn()
            if isinstance(result, list):
                fetched.extend(result)
            elif isinstance(result, str):
                fetched.append(result)
        except Exception as e:
            print(f"❌ {name} failed: {e}")

    print("\n" + "="*35)
    if fetched:
        print(f"✅ All completed downloads ({len(fetched)} files):")
        for f in fetched: print(" •", f)
    else:
        print("❌ No files downloaded.")

if __name__ == "__main__":
    download_all()
