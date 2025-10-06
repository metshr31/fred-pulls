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
CSX_METRICS_PAGE = "https://investors.csx.com/metrics/default.aspx"
CSX_CDN_BASE = "https://s2.q4cdn.com/859568992/files/doc_downloads"
CPKC_CDN_BASE = "https://s21.q4cdn.com/736796105/files/doc_downloads"
CPKC_53WEEK_FILENAME = "CPKC-53-Week-Railway-Performance-Report.xlsx"
UP_STATIC = {
    "RTM_Carloadings": "https://investor.unionpacific.com/static-files/42fe7816-51a0-4844-9e24-ab51fb378299",
    "Performance_Measures": "https://investor.unionpacific.com/static-files/cedd1572-83c5-49e4-9bc2-753e75ed6814",
}
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
    os.makedirs(path, exist_ok=True)

def datestamp() -> str:
    return dt.date.today().strftime("%Y-%m-%d")

def sanitize_filename(name: str) -> str:
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
    raise requests.RequestException(f"Failed to get {url} after {retries} attempts.")

def http_head_ok(url: str, timeout: Union[int, None] = None) -> bool:
    try:
        r = requests.head(url, headers=UA, timeout=timeout or TIMEOUT_DEFAULT, allow_redirects=True)
        ctype = r.headers.get("Content-Type", "").lower()
        return r.status_code == 200 and "text/html" not in ctype
    except requests.RequestException:
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
# CPKC
# =========================
def _discover_cpkc_cdn_url(filename_pattern: str, max_back_days: int) -> str:
    today = dt.date.today()
    for delta in range(max_back_days):
        d = today - dt.timedelta(days=delta)
        folder = d.strftime("%Y/%m/%d")
        url = f"{CPKC_CDN_BASE}/{folder}/{filename_pattern}"
        if http_head_ok(url):
            return url
    raise FileNotFoundError(f"CPKC file ({filename_pattern}) not found in last {max_back_days} days.")

def download_cpkc_53week() -> str:
    url = _discover_cpkc_cdn_url(CPKC_53WEEK_FILENAME, 14)
    resp = http_get(url)
    return save_bytes(resp.content, f"CPKC_53_Week_{datestamp()}.xlsx")

def download_cpkc_rtm() -> str:
    today = dt.date.today()
    year_filename = f"CPKC-Weekly-RTMs-and-Carloads-{today.year}.xlsx"
    url = _discover_cpkc_cdn_url(year_filename, 14)
    resp = http_get(url)
    return save_bytes(resp.content, f"CPKC_Weekly_RTM_{datestamp()}.xlsx")

# =========================
# CSX Excel – Historical_Data_Week only
# =========================
def discover_csx_excel(max_back_days: int = 30) -> str:
    """
    Find the latest CSX Historical_Data_Week Excel file by checking daily
    folders in the CDN path (year/month/day).
    """
    today = dt.date.today()
    tried_urls = []

    for delta in range(max_back_days):
        d = today - dt.timedelta(days=delta)
        folder = d.strftime("%Y/%m/%d")  # e.g. 2025/09/30
        year, week, _ = d.isocalendar()
        fname = f"Historical_Data_Week_{week}_{year}.xlsx"
        url = f"{CSX_CDN_BASE}/{folder}/{fname}"
        tried_urls.append(url)

        if http_head_ok(url):
            print(f"✅ CSX Excel found: {url}")
            return url

    raise FileNotFoundError(f"CSX Historical_Data Excel not found. Tried: {tried_urls}")

def download_csx() -> str:
    url = discover_csx_excel()
    resp = http_get(url)
    server_name = url.rstrip("/").rsplit("/", 1)[-1]
    fname = sanitize_filename(f"CSX_{datestamp()}_{server_name}")
    return save_bytes(resp.content, fname)

# =========================
# CSX AAR (PDF) – robust back-search
# =========================
def download_csx_aar(max_back_weeks: int = 12) -> str:
    """
    Find and download the latest available CSX Weekly AAR PDF by searching
    backwards up to `max_back_weeks`. Names the saved file with the actual
    Year + Week from the PDF URL.
    """
    today = dt.date.today()
    tried_urls = []

    for delta in range(max_back_weeks):
        d = today - dt.timedelta(weeks=delta)
        year, week, _ = d.isocalendar()
        url = f"{CSX_CDN_BASE}/volume_trends/{year}/{year}-Week-{week}-AAR.pdf"
        tried_urls.append(url)

        if http_head_ok(url):
            print(f"⬇️ CSX AAR PDF found: {url}")
            resp = http_get(url)
            fname = sanitize_filename(f"CSX_AAR_{year}-Week-{week}.pdf")
            return save_bytes(resp.content, fname)

    raise FileNotFoundError(
        f"CSX AAR PDF not found in the last {max_back_weeks} weeks. Tried: {tried_urls}"
    )

# =========================
# UP
# =========================
def download_up() -> List[str]:
    saved = []
    for label, url in UP_STATIC.items():
        print(f"⬇️ UP {label}")
        resp = http_get(url, timeout=TIMEOUT_UP, retries=3)
        saved.append(save_bytes(resp.content, f"UP_{label}_{datestamp()}.xlsx"))
        time.sleep(0.5)
    return saved

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
    r = http_get(BNSF_REPORTS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        txt = (a.get_text() or "").lower()
        if "carload" in txt and a["href"].lower().endswith(".pdf"):
            url = normalize_url("https://www.bnsf.com", a["href"])
            resp = http_get(url, retries=3)
            return save_bytes(resp.content, f"BNSF_Carloads_{datestamp()}.pdf")
    raise FileNotFoundError("BNSF weekly carload PDF not found")

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
        ("CSX", download_csx),
        ("CSX AAR", download_csx_aar),  # now robust
        ("UP", download_up),
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
