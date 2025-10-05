import os
import re
import time
import datetime as dt
import requests
from bs4 import BeautifulSoup
from typing import List, Union

# =========================
# Config
# =========================
STB_URL = "https://www.stb.gov/reports-data/rail-service-data/"
CN_PERF_URL = "https://www.cn.ca/-/media/files/investors/investor-performance-measures/perf_measures_en.xlsx"
CN_METRICS_PAGE = "https://www.cn.ca/en/investors/key-weekly-metrics/"

# CSX — keep working CDN-first logic, page only as fallback
CSX_METRICS_PAGE = "https://investors.csx.com/metrics/default.aspx"
CSX_CDN_BASE = "https://s2.q4cdn.com/859568992/files/doc_downloads"

# CPKC
CPKC_CDN_BASE = "https://s21.q4cdn.com/736796105/files/doc_downloads"
CPKC_53WEEK_FILENAME = "CPKC-53-Week-Railway-Performance-Report.xlsx"

# UP — avoid page scrape; use stable static files
UP_STATIC = {
    "RTM_Carloadings": "https://investor.unionpacific.com/static-files/42fe7816-51a0-4844-9e24-ab51fb378299",
    "Performance_Measures": "https://investor.unionpacific.com/static-files/cedd1572-83c5-49e4-9bc2-753e75ed6814",
}

# NS + BNSF
NS_REPORTS_PAGE = "https://norfolksouthern.investorroom.com/weekly-performance-reports"
BNSF_REPORTS_PAGE = "https://www.bnsf.com/about-bnsf/financial-information/weekly-carload-reports/"

DOWNLOAD_FOLDER = os.getenv("STB_LOG_DIR", os.getcwd())
TIMEOUT_DEFAULT = 20
TIMEOUT_UP = 60  # UP is slow

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) excel-fetcher",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# =========================
# Utilities
# =========================
def ensure_dir(path: str) -> None:
    """Ensure the download directory exists."""
    os.makedirs(path, exist_ok=True)

def datestamp() -> str:
    """Return today's date in YYYY-MM-DD format."""
    return dt.date.today().strftime("%Y-%m-%d")

def sanitize_filename(name: str) -> str:
    """Remove non-allowed characters from a string for use in a filename."""
    return re.sub(r"[^\w\-.]+", "_", name)

def save_bytes(content: bytes, filename: str) -> str:
    """Save bytes content to a file in the download folder."""
    ensure_dir(DOWNLOAD_FOLDER)
    full = os.path.join(DOWNLOAD_FOLDER, filename)
    with open(full, "wb") as f:
        f.write(content)
    print(f"✅ Saved: {full}")
    return full

def http_get(url: str, timeout: Union[int, None] = None, referer: Union[str, None] = None, retries: int = 3, backoff: int = 5) -> requests.Response:
    """Robust HTTP GET request with retries and status check."""
    headers = dict(UA)
    if referer: headers["Referer"] = referer
    
    # Use longer timeout for Union Pacific
    t = TIMEOUT_UP if "unionpacific.com" in url else (timeout or TIMEOUT_DEFAULT)
    
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=t, allow_redirects=True)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == retries:
                raise
            print(f"⚠️ Attempt {attempt} failed for {url}: {e} — retrying in {backoff}s")
            time.sleep(backoff)
    # Should be unreachable, but helps satisfy type checker
    raise requests.RequestException(f"Failed to get {url} after {retries} attempts.")


def http_head_ok(url: str, timeout: Union[int, None] = None) -> bool:
    """Check if a URL is accessible and not an HTML page using HEAD request."""
    try:
        r = requests.head(url, headers=UA, timeout=timeout or TIMEOUT_DEFAULT, allow_redirects=True)
        ctype = r.headers.get("Content-Type", "").lower()
        # Ensure 200 OK and not serving an HTML page
        return r.status_code == 200 and "text/html" not in ctype
    except requests.RequestException:
        return False

def normalize_url(base: str, href: str) -> Union[str, None]:
    """Convert a relative href into a full, absolute URL."""
    if not href: return None
    href = href.strip()
    if href.startswith("//"): return "https:" + href
    if href.startswith("http"): return href
    # Handle relative paths: /path or path
    from urllib.parse import urljoin
    return urljoin(base, href)

# =========================
# STB - EP724
# =========================
def get_latest_ep724_url() -> str:
    """Scrape STB page for the latest EP724 Excel link."""
    r = http_get(STB_URL)
    soup = BeautifulSoup(r.text, "html.parser")
    
    # Find links that contain "EP724" and end with ".xlsx"
    links = [a["href"] for a in soup.find_all("a", href=True) if "EP724" in a["href"] and a["href"].endswith(".xlsx")]
    
    if not links: raise FileNotFoundError("No EP724 .xlsx link found")
    
    # Links are not guaranteed to be ordered, so we assume the latest one has a later date in the name/URL
    # For stability, we take the last one after sorting (often helps get the newest date)
    links.sort() 
    url = links[-1]
    
    # Ensure it's an absolute URL
    return normalize_url("https://www.stb.gov", url)

def download_ep724() -> str:
    """Download the latest EP724 Excel file."""
    resp = http_get(get_latest_ep724_url())
    return save_bytes(resp.content, f"EP724_{datestamp()}.xlsx")

# =========================
# CN
# =========================
def download_cn_perf() -> str:
    """Download CN Performance Measures via direct link."""
    resp = http_get(CN_PERF_URL)
    return save_bytes(resp.content, f"CN_Performance_{datestamp()}.xlsx")

def download_cn_rtm() -> List[str]:
    """Scrape CN page for RTM Excel links and download all found."""
    r = http_get(CN_METRICS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    
    # Find all .xlsx links
    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".xlsx")]
    
    if not links: raise FileNotFoundError("No CN RTM .xlsx link found")
    
    saved = []
    for url in links:
        url = normalize_url("https://www.cn.ca", url)
        fname = url.split("/")[-1]
        print(f"⬇️ CN RTM {fname}")
        resp = http_get(url)
        # Save with a unique name based on the original filename
        saved.append(save_bytes(resp.content, f"CN_RTM_{datestamp()}_{fname}"))
        time.sleep(0.5)
    return saved

# =========================
# CPKC
# =========================
def _discover_cpkc_cdn_url(filename_pattern: str, max_back_days: int) -> str:
    """Helper to find the latest CPKC CDN file based on date folder structure."""
    today = dt.date.today()
    for delta in range(max_back_days):
        d = today - dt.timedelta(days=delta)
        # CPKC seems to use a folder structure based on the date the file was published
        folder = d.strftime("%Y/%m/%d")
        url = f"{CPKC_CDN_BASE}/{folder}/{filename_pattern}"
        if http_head_ok(url): 
            return url
    raise FileNotFoundError(f"CPKC file ({filename_pattern}) not found in last {max_back_days} days.")

def download_cpkc_53week() -> str:
    """Download CPKC 53-week report by probing CDN dates."""
    # This report is typically updated less frequently or has a stable filename
    url = _discover_cpkc_cdn_url(CPKC_53WEEK_FILENAME, 14) 
    resp = http_get(url)
    return save_bytes(resp.content, f"CPKC_53_Week_{datestamp()}.xlsx")

def download_cpkc_rtm() -> str:
    """Download CPKC Weekly RTM/Carloads by probing CDN dates."""
    # The RTM file has a filename that includes the year, need to find the latest one.
    today = dt.date.today()
    year_filename = f"CPKC-Weekly-RTMs-and-Carloads-{today.year}.xlsx"
    
    url = _discover_cpkc_cdn_url(year_filename, 14)
    resp = http_get(url)
    return save_bytes(resp.content, f"CPKC_Weekly_RTM_{datestamp()}.xlsx")

# =========================
# CSX — CDN-first (reverted), page as fallback
# =========================
def _iso_week_year(date_obj: dt.date) -> tuple[int, int]: 
    """Get the ISO Year and ISO Week Number."""
    iso = date_obj.isocalendar()
    return iso[0], iso[1]

def _csx_candidate_filenames(year: int, week: int) -> List[str]:
    """Generate known candidate filenames for CSX's weekly report."""
    return [
        # Old, predictable format
        f"Historical_Data_Week_{week}_{year}.xlsx",
        # New, longer format (often year-specific)
        f"Combined-Intermodal-and-Carload-TPC-Week-1-2022-Week-{week}-{year}.xlsx",
        f"Combined-Intermodal-and-Carload-TPC-Week-1-2023-Week-{week}-{year}.xlsx",
        f"Combined-Intermodal-and-Carload-TPC-Week-1-2024-Week-{week}-{year}.xlsx",
        f"Combined-Intermodal-and-Carload-TPC-Week-1-2025-Week-{week}-{year}.xlsx",
    ]

def discover_csx_url(max_back_days: int = 10) -> str:
    """Find the latest CSX Excel URL by trying CDN paths first, then scraping."""
    today = dt.date.today()
    
    # CSX files report on the *previous* week. We base our search on the most recently completed week.
    last_week_end = today - dt.timedelta(days=today.weekday() + 2)
    year, week = _iso_week_year(last_week_end)

    # 1) CDN-first: try past N days of folders (the logic that worked before)
    for delta in range(max_back_days):
        d = today - dt.timedelta(days=delta)
        folder = d.strftime("%Y/%m/%d")
        for fname in _csx_candidate_filenames(year, week):
            url = f"{CSX_CDN_BASE}/{folder}/{fname}"
            if http_head_ok(url): 
                print(f"✅ CSX URL found via CDN: {url}")
                return url

    # 2) Fallback: parse metrics page for any .xlsx (may 403 sometimes, so only 1 retry)
    print(f"⚠️ CDN attempts failed. Trying CSX page scrape: {CSX_METRICS_PAGE}")
    try:
        # Reduced retries for the page itself as it seems sensitive
        r = http_get(CSX_METRICS_PAGE, retries=1) 
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Look for the "Weekly Carload Reports" section links
        links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".xlsx")]
        
        # Take the first available .xlsx link and validate it
        for u in links:
            u = normalize_url("https://investors.csx.com", u)
            if u and http_head_ok(u): 
                print(f"✅ CSX URL found via HTML scrape: {u}")
                return u
    except Exception as e:
        print(f"❌ CSX HTML scrape failed: {e}")

    raise FileNotFoundError("CSX Excel not found (CDN + fallback both failed).")

def download_csx() -> str:
    """Download the latest CSX Excel file."""
    url = discover_csx_url()
    resp = http_get(url)
    # Use the server-provided filename as a tag
    server_name = url.rstrip("/").rsplit("/", 1)[-1]
    fname = sanitize_filename(f"CSX_{datestamp()}_{server_name}")
    return save_bytes(resp.content, fname)

# =========================
# UP — use static-file endpoints (avoid scrape timeout)
# =========================
def download_up() -> List[str]:
    """Download UP's two main Excel files via stable static links."""
    saved = []
    # Use the long timeout setting for UP
    t = TIMEOUT_UP 
    
    # UP_STATIC keys are used to name the saved files
    for label, url in UP_STATIC.items():
        print(f"⬇️ UP {label}")
        # The http_get utility already applies TIMEOUT_UP for UP URLs
        resp = http_get(url, timeout=t, retries=3) 
        saved.append(save_bytes(resp.content, f"UP_{label}_{datestamp()}.xlsx"))
        time.sleep(0.5)
    return saved

# =========================
# NS — Excel + PDF (normalize & fetch latest)
# =========================
def download_ns() -> List[str]:
    """Scrape NS page for the latest Performance (.xlsx) and Carloads (.pdf) and download both."""
    r = http_get(NS_REPORTS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    anchors = soup.find_all("a", href=True)
    
    xlsx_links = []
    pdf_links = []

    for a in anchors:
        href = a.get("href")
        text = (a.get_text() or "").lower()
        if not href: 
            continue
        
        # Ensure only current reports are considered (they are listed newest first)
        url = normalize_url("https://norfolksouthern.investorroom.com", href)

        if href.lower().endswith(".xlsx") and "performance" in text:
            xlsx_links.append(url)

        if href.lower().endswith(".pdf") and ("carload" in text or "carloading" in text):
            pdf_links.append(url)

    saved = []
    
    # NS reports are listed newest-first, so the first one found is the latest.
    if xlsx_links:
        latest_xlsx = xlsx_links[0]
        resp = http_get(latest_xlsx, referer=NS_REPORTS_PAGE, retries=3)
        saved.append(save_bytes(resp.content, f"NS_Performance_{datestamp()}.xlsx"))

    if pdf_links:
        latest_pdf = pdf_links[0]
        resp = http_get(latest_pdf, referer=NS_REPORTS_PAGE, retries=3)
        saved.append(save_bytes(resp.content, f"NS_Carloads_{datestamp()}.pdf"))

    if not saved: 
        raise FileNotFoundError("NS reports not found")
    return saved

# =========================
# BNSF — Current Weekly Carload (PDF)
# =========================
def download_bnsf() -> str:
    """Scrape BNSF page for the latest Weekly Carload PDF and download it."""
    r = http_get(BNSF_REPORTS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    
    for a in soup.find_all("a", href=True):
        txt = (a.get_text() or "").lower()
        # Look for anchor text that mentions "carload" AND the link ends in .pdf
        if "carload" in txt and a["href"].lower().endswith(".pdf"):
            url = normalize_url("https://www.bnsf.com", a["href"])
            # The first one found is generally the "Current" one.
            resp = http_get(url, retries=3)
            return save_bytes(resp.content, f"BNSF_Carloads_{datestamp()}.pdf")
            
    raise FileNotFoundError("BNSF weekly carload PDF not found")

# =========================
# Main
# =========================
def main():
    print(f"📂 Download folder: {DOWNLOAD_FOLDER}")
    fetched: List[str] = []
    tasks = [
        ("EP724", download_ep724),
        ("CN Performance", download_cn_perf),
        ("CN RTM", download_cn_rtm),
        ("CPKC 53-week", download_cpkc_53week),
        ("CPKC Weekly RTM", download_cpkc_rtm),
        ("CSX", download_csx),            # Reverted to CDN-first logic
        ("UP", download_up),              # Static-file endpoints
        ("NS", download_ns),
        ("BNSF", download_bnsf),
    ]
    
    for name, fn in tasks:
        try:
            result = fn()
            # Handle both single file (str) and multiple files (List[str]) return types
            if isinstance(result, list): 
                fetched.extend(result)
            elif isinstance(result, str): 
                fetched.append(result)
        except Exception as e:
            print(f"❌ {name} failed: {e}")

    if fetched:
        print("✅ Files downloaded:")
        for f in fetched: print(" •", f)
    else:
        print("❌ No files downloaded.")

if __name__ == "__main__":
    main()
