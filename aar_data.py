# aar_data.py
import os
import re
import time
import datetime as dt
import requests
from bs4 import BeautifulSoup
from typing import List

# << NEW PLAYWRIGHT INTEGRATION >>
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    # Set a flag if Playwright isn't installed
    PLAYWRIGHT_AVAILABLE = False

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

# UP — NEW: Avoid static files; use page scrape as static files break often
UP_METRICS_PAGE = "https://investor.unionpacific.com/key-performance-metrics/"
UP_STATIC = {} # Removed old URLs, now using page scrape

# NS + BNSF
NS_REPORTS_PAGE = "https://norfolksouthern.investorroom.com/weekly-performance-reports"
BNSF_REPORTS_PAGE = "https://www.bnsf.com/about-bnsf/financial-information/weekly-carload-reports/"

DOWNLOAD_FOLDER = os.getenv("STB_LOG_DIR", os.getcwd())
TIMEOUT_DEFAULT = 20
TIMEOUT_UP = 60 # UP is slow

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) excel-fetcher",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# =========================
# Utilities
# =========================
def ensure_dir(path): os.makedirs(path, exist_ok=True)
def datestamp(): return dt.date.today().strftime("%Y-%m-%d")
def sanitize_filename(name): return re.sub(r"[^\w\-.]+", "_", name)

def save_bytes(content, filename):
    ensure_dir(DOWNLOAD_FOLDER)
    full = os.path.join(DOWNLOAD_FOLDER, filename)
    with open(full, "wb") as f: f.write(content)
    print(f"✅ Saved: {full}")
    return full

def http_get(url, timeout=None, referer=None, retries=3, backoff=5):
    headers = dict(UA)
    if referer: headers["Referer"] = referer
    t = TIMEOUT_UP if "unionpacific.com" in url else (timeout or TIMEOUT_DEFAULT)
    for attempt in range(1, retries+1):
        try:
            r = requests.get(url, headers=headers, timeout=t, allow_redirects=True)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == retries:
                raise
            print(f"⚠️ Attempt {attempt} failed for {url}: {e} — retrying in {backoff}s")
            time.sleep(backoff)

def http_head_ok(url, timeout=None):
    try:
        r = requests.head(url, headers=UA, timeout=timeout or TIMEOUT_DEFAULT, allow_redirects=True)
        ctype = r.headers.get("Content-Type", "").lower()
        return r.status_code == 200 and "text/html" not in ctype
    except requests.RequestException:
        return False

def normalize_url(base, href):
    if not href: return None
    href = href.strip()
    if href.startswith("//"): return "https:" + href
    if href.startswith("http"): return href
    if href.startswith("/"): return base.rstrip("/") + href
    return base.rstrip("/") + "/" + href

def _discover_cpkc_cdn_url(filename_pattern: str, max_back_days: int) -> str:
    """Helper to find CPKC files across recent date-based CDN folders."""
    today = dt.date.today()
    for delta in range(max_back_days):
        d = today - dt.timedelta(days=delta)
        folder = d.strftime("%Y/%m/%d")
        # Handle filename pattern substitution if needed (e.g., for year)
        filename = filename_pattern.replace("{year}", str(d.year))
        url = f"{CPKC_CDN_BASE}/{folder}/{filename}"
        if http_head_ok(url): 
            return url
    raise FileNotFoundError(f"CPKC file matching '{filename_pattern}' not found in last {max_back_days} days.")

# =========================
# STB
# =========================
def get_latest_ep724_url():
    r = http_get(STB_URL)
    soup = BeautifulSoup(r.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if "EP724" in a["href"] and a["href"].endswith(".xlsx")]
    if not links: raise FileNotFoundError("No EP724 .xlsx link found")
    links.sort()
    url = links[-1]
    if not url.startswith("http"): url = "https://www.stb.gov" + url
    return url

def download_ep724():
    resp = http_get(get_latest_ep724_url())
    return save_bytes(resp.content, f"EP724_{datestamp()}.xlsx")

# =========================
# CN
# =========================
def download_cn_perf():
    resp = http_get(CN_PERF_URL)
    return save_bytes(resp.content, f"CN_Performance_{datestamp()}.xlsx")

def download_cn_rtm():
    r = http_get(CN_METRICS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".xlsx")]
    if not links: raise FileNotFoundError("No CN RTM .xlsx link found")
    saved = []
    for url in links:
        url = normalize_url("https://www.cn.ca", url)
        fname = url.split("/")[-1]
        # Skip duplicate or non-weekly files if possible
        if "performance" in fname.lower(): continue 
        print(f"⬇️ CN RTM {fname}")
        resp = http_get(url)
        # Use filename from URL if possible, otherwise use a generic name
        base_name = re.sub(r"[\d\-]+", "", fname).strip(".-_").replace(".xlsx", "")
        saved.append(save_bytes(resp.content, f"CN_RTM_{datestamp()}_{base_name}.xlsx"))
    return saved

# =========================
# CPKC
# =========================
def discover_cpkc_53week_url():
    # Search last 14 days, Monday logic is often rigid.
    return _discover_cpkc_cdn_url(CPKC_53WEEK_FILENAME, max_back_days=14)

def download_cpkc_53week():
    resp = http_get(discover_cpkc_53week_url())
    return save_bytes(resp.content, f"CPKC_53_Week_{datestamp()}.xlsx")

def discover_cpkc_rtm_url():
    # RTM file uses the year in the filename
    filename_pattern = "CPKC-Weekly-RTMs-and-Carloads-{year}.xlsx"
    # Search last 14 days
    return _discover_cpkc_cdn_url(filename_pattern, max_back_days=14)

def download_cpkc_rtm():
    resp = http_get(discover_cpkc_rtm_url())
    return save_bytes(resp.content, f"CPKC_Weekly_RTM_{datestamp()}.xlsx")

# =========================
# CSX — CDN-first (working), page as fallback (Excel)
# =========================
def _iso_week_year(date_obj): 
    iso = date_obj.isocalendar(); return iso[0], iso[1]

def _csx_candidate_filenames(year: int, week: int) -> List[str]:
    """Provides Excel filename candidates including future-proofed year references."""
    # Note: These names are based on observed patterns and may need yearly adjustment
    return [
        f"Historical_Data_Week_{week}_{year}.xlsx",
        f"Combined-Intermodal-and-Carload-TPC-Week-1-2022-Week-{week}-{year}.xlsx",
        f"Combined-Intermodal-and-Carload-TPC-Week-1-2023-Week-{week}-{year}.xlsx",
        f"Combined-Intermodal-and-Carload-TPC-Week-1-2024-Week-{week}-{year}.xlsx",
        f"Combined-Intermodal-and-Carload-TPC-Week-1-2025-Week-{week}-{year}.xlsx", # Good through next year
    ]

def discover_csx_url(max_back_days=10):
    today = dt.date.today()
    # Use last week (files are generally published after the week ends)
    last_week_end = today - dt.timedelta(days=today.weekday() + 2)
    year, week = _iso_week_year(last_week_end)

    # 1) CDN-first: try past N days of folders
    for delta in range(max_back_days):
        d = today - dt.timedelta(days=delta)
        folder = d.strftime("%Y/%m/%d")
        for fname in _csx_candidate_filenames(year, week):
            url = f"{CSX_CDN_BASE}/{folder}/{fname}"
            if http_head_ok(url): 
                print(f"✅ CSX URL found via CDN: {url}")
                return url

    # 2) Fallback: parse metrics page for any .xlsx
    print("⚠️ CSX CDN failed. Trying metrics page scrape...")
    try:
        r = http_get(CSX_METRICS_PAGE, retries=2)
        soup = BeautifulSoup(r.text, "html.parser")
        links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].lower().endswith(".xlsx")]
        for u in links:
            u = normalize_url("https://investors.csx.com", u)
            if http_head_ok(u): 
                print(f"✅ CSX URL found via fallback scrape: {u}")
                return u
    except Exception as e:
        print(f"⚠️ CSX fallback scrape failed: {e}")

    raise FileNotFoundError("CSX Excel not found (CDN + fallback both failed).")

def download_csx():
    """Downloads the CSX Weekly Performance Excel file."""
    url = discover_csx_url()
    resp = http_get(url)
    server_name = url.rstrip("/").rsplit("/", 1)[-1]
    fname = sanitize_filename(f"CSX_Performance_{datestamp()}_{server_name}")
    return save_bytes(resp.content, fname)

# << NEW PLAYWRIGHT INTEGRATION >>
def download_csx_aar_reports():
    """
    Uses Playwright to scrape the dynamically loaded Weekly Carload Report (PDF) links 
    from the CSX Investor Relations page and downloads the latest one.
    """
    if not PLAYWRIGHT_AVAILABLE:
        print("❌ Playwright not installed. Skipping CSX AAR PDF download.")
        print("   Run: pip install playwright requests && playwright install")
        return []

    print("🚀 Launching headless browser to fetch CSX AAR PDF links...")
    carload_links = []
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.goto(CSX_METRICS_PAGE, wait_until="networkidle") 
            
            # Wait for the dynamic 'Weekly Carload Reports' section to load
            page.wait_for_selector('h2:has-text("Weekly Carload Reports")', timeout=15000)
            print("✅ Dynamic content loaded.")
            
            # Find the main container for the reports and get all link elements
            reports_container = page.locator('div.module.module-metrics-volume').first
            link_elements = reports_container.locator('a:has-text("AAR(opens in new window)")').all()

            for element in link_elements:
                link_text = element.inner_text().replace('(opens in new window)', '').strip()
                link_url = element.get_attribute('href')

                # Handle relative URLs
                if link_url and link_url.startswith('/'):
                    link_url = f"https://investors.csx.com{link_url}"

                if link_url:
                    carload_links.append((link_text, link_url))

            browser.close()
            
    except Exception as e:
        print(f"❌ An error occurred during Playwright scraping: {e}")
        return []

    if not carload_links:
        print("⚠️ Found no CSX AAR PDF links via Playwright.")
        return []
    
    # Download the latest report (it's usually the first one in the list)
    latest_name, latest_url = carload_links[0]
    
    print(f"⬇️ Downloading latest CSX AAR PDF: {latest_name}")
    
    try:
        # Use existing http_get for the final download
        resp = http_get(latest_url, retries=3) 
        filename = sanitize_filename(f"CSX_AAR_Carloads_{datestamp()}_{latest_name}.pdf")
        return [save_bytes(resp.content, filename)]
    except Exception as e:
        print(f"❌ Failed to download CSX AAR PDF from {latest_url}: {e}")
        return []

# =========================
# UP — Scrape the metrics page for current links (FIXED)
# =========================
def download_up():
    print(f"⬇️ Connecting to Union Pacific metrics page: {UP_METRICS_PAGE}")
    r = http_get(UP_METRICS_PAGE, timeout=TIMEOUT_UP, retries=3)
    soup = BeautifulSoup(r.text, "html.parser")
    anchors = soup.find_all("a", href=True)
    saved = []
    
    # Target file names/keywords mentioned on the UP page
    keywords = ["rtm", "carload", "performance", "metric"] 
    downloaded_urls = set()
    
    for a in anchors:
        href = a["href"]
        text = (a.get_text() or "").lower()
        if not href.lower().endswith((".xlsx", ".xls")):
            continue
            
        is_relevant = any(k in href.lower() or k in text for k in keywords)
        
        if is_relevant:
            url = normalize_url("https://investor.unionpacific.com", href)
            
            # Label based on content
            label = "RTM_Carloadings" if "rtm" in url.lower() or "carload" in url.lower() else "Performance_Measures"
            
            if url not in downloaded_urls:
                print(f"⬇️ UP {label} found at {url}")
                try:
                    resp = http_get(url, timeout=TIMEOUT_UP, retries=3)
                    saved.append(save_bytes(resp.content, f"UP_{label}_{datestamp()}.xlsx"))
                    downloaded_urls.add(url)
                    time.sleep(0.5) # Be polite
                except Exception as e:
                    print(f"❌ Failed to download UP file from {url}: {e}")

    if not saved:
        raise FileNotFoundError("No UP RTM or Performance XLSX links found on the metrics page.")
    return saved

# =========================
# NS — Excel + Latest Weekly PDF (FIXED)
# =========================
def download_ns():
    r = http_get(NS_REPORTS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    anchors = soup.find_all("a", href=True)
    saved = []
    
    pdf_links = []
    xlsx_links = []
    
    for a in anchors:
        href = a["href"]
        text = (a.get_text() or "").lower()
        if not href: continue
        url = normalize_url("https://norfolksouthern.investorroom.com", href)

        # Look for the latest WEEKLY Carloads PDF (must contain 'weekly carload' or 'aar weekly carload')
        if href.lower().endswith(".pdf") and ("weekly carload" in text or "aar weekly carload" in text):
            pdf_links.append(url)

        # Look for the latest Performance XLSX
        if href.lower().endswith(".xlsx") and "performance" in text:
            xlsx_links.append(url)

    # 1. Download Latest Weekly Carloads PDF (NS lists newest-first)
    if pdf_links:
        latest_pdf_url = pdf_links[0] 
        print(f"⬇️ NS Weekly Carload PDF: {latest_pdf_url}")
        resp = http_get(latest_pdf_url, referer=NS_REPORTS_PAGE, retries=3)
        saved.append(save_bytes(resp.content, f"NS_Weekly_Carloads_{datestamp()}.pdf"))

    # 2. Download Latest Performance XLSX
    if xlsx_links:
        latest_xlsx_url = xlsx_links[0]
        print(f"⬇️ NS Performance XLSX: {latest_xlsx_url}")
        resp = http_get(latest_xlsx_url, referer=NS_REPORTS_PAGE, retries=3)
        saved.append(save_bytes(resp.content, f"NS_Performance_{datestamp()}.xlsx"))

    if not saved:
        raise FileNotFoundError("NS reports not found (Weekly Carload PDF or Performance XLSX).")
    return saved

# =========================
# BNSF — Current Weekly Carload (PDF)
# =========================
def download_bnsf():
    r = http_get(BNSF_REPORTS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        txt = (a.get_text() or "").lower()
        # Look for the current weekly carload report (usually the top link)
        if "carload" in txt and a["href"].lower().endswith(".pdf"):
            url = normalize_url("https://www.bnsf.com", a["href"])
            # Ensure it's not a historical or monthly link if a specific 'weekly' link is available
            if "weekly" in txt or "carloadings" in txt: 
                resp = http_get(url, retries=3)
                return save_bytes(resp.content, f"BNSF_Carloads_{datestamp()}.pdf")
            
    raise FileNotFoundError("BNSF weekly carload PDF not found")

# =========================
# Main
# =========================
def main():
    print(f"📂 Download folder: {DOWNLOAD_FOLDER}")
    fetched = []
    tasks = [
        ("STB EP724", download_ep724),
        ("CN Performance", download_cn_perf),
        ("CN RTM", download_cn_rtm),
        ("CPKC 53-week", download_cpkc_53week),
        ("CPKC Weekly RTM", download_cpkc_rtm),
        # CSX Excel is first
        ("CSX Performance XLSX", download_csx),
        # << NEW PLAYWRIGHT INTEGRATION >> CSX AAR PDF is new
        ("CSX AAR Carloads PDF", download_csx_aar_reports), 
        ("UP", download_up),
        ("NS", download_ns),
        ("BNSF", download_bnsf),
    ]
    for name, fn in tasks:
        try:
            print(f"\n--- Starting {name} ---")
            result = fn()
            if isinstance(result, list): fetched.extend(result)
            elif result: fetched.append(result) # Only append if the result is not None/empty
        except Exception as e:
            print(f"❌ {name} failed: {e}")

    print("\n" + "="*30)
    if fetched:
        print(f"✅ ALL TARGETED FILES DOWNLOADED ({len(fetched)} total):")
        for f in fetched: print(" •", f)
    else:
        print("❌ NO files downloaded.")
    print("="*30)

if __name__ == "__main__":
    main()
