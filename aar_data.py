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
def _discover_cpkc_cdn_url(filename: str, max_back_days: int = 45) -> str:
    """
    Probe CPKC's CDN for a given filename by walking back in time and trying both
    folder layouts that appear on s21.q4cdn.com:
      - /YYYY/MM/DD/filename
      - /YYYY/MM/filename
    Returns the first URL that responds OK to HEAD.
    """
    today = dt.date.today()
    for delta in range(max_back_days):
        d = today - dt.timedelta(days=delta)
        # Try both folder patterns for each day we probe
        for folder in (d.strftime("%Y/%m/%d"), d.strftime("%Y/%m")):
            url = f"{CPKC_CDN_BASE}/{folder}/{filename}"
            if http_head_ok(url):
                print(f"✅ Found CPKC file at: {url}")
                return url
    raise FileNotFoundError(f"CPKC file ({filename}) not found in last {max_back_days} days.")

def download_cpkc_53week() -> str:
    """
    Grab the '53 Week Railway Performance' report from the CPKC CDN, handling
    both /YYYY/MM/DD and /YYYY/MM folder styles.
    """
    filename = "CPKC-53-Week-Railway-Performance-Report.xlsx"
    url = _discover_cpkc_cdn_url(filename, max_back_days=60)
    resp = http_get(url)
    return save_bytes(resp.content, f"CPKC_53_Week_{datestamp()}.xlsx")

def download_cpkc_rtm() -> str:
    """
    Grab the 'Weekly RTMs and Carloads' spreadsheet for the current year.
    CPKC sometimes appends a numeric suffix (e.g., -2025-2.xlsx).
    We try the highest plausible suffix first and fall back to no suffix.
    """
    year = dt.date.today().year
    base = f"CPKC-Weekly-RTMs-and-Carloads-{year}"
    # Try possible suffixes from 9 down to 1, then no suffix, to prefer the newest
    candidates = [f"{base}-{k}.xlsx" for k in range(9, 0, -1)] + [f"{base}.xlsx"]

    last_error = None
    for fname in candidates:
        try:
            url = _discover_cpkc_cdn_url(fname, max_back_days=60)
            resp = http_get(url)
            return save_bytes(resp.content, f"CPKC_Weekly_RTM_{datestamp()}.xlsx")
        except Exception as e:
            last_error = e
            # Keep trying the next candidate
            continue

    # If nothing worked, raise the last error we saw
    raise FileNotFoundError(f"CPKC RTM file not found with any candidate name. Last error: {last_error}")

# =========================
# CSX Excel (Historical_Data only)
# =========================
def discover_csx_historical(max_back_weeks: int = 12) -> str:
    """
    Find the most recent CSX Historical_Data_Week file by checking backward
    from the current ISO week up to max_back_weeks.
    """
    today = dt.date.today()
    iso_year, iso_week, _ = today.isocalendar()

    tried = []
    for delta in range(max_back_weeks):
        week = iso_week - delta
        year = iso_year
        # if we roll back before week 1, adjust year
        while week <= 0:
            year -= 1
            last_dec = dt.date(year, 12, 28)  # ISO week 52 or 53
            week = last_dec.isocalendar()[1] + week

        # Try every day folder of the last 14 days (posting lag possible)
        for day_delta in range(0, 14):
            d = today - dt.timedelta(days=day_delta)
            folder = d.strftime("%Y/%m/%d")
            fname = f"Historical_Data_Week_{week}_{year}.xlsx"
            url = f"{CSX_CDN_BASE}/{folder}/{fname}"
            tried.append(url)
            if http_head_ok(url):
                print(f"✅ Found CSX Historical Data: {url}")
                return url

    raise FileNotFoundError(f"❌ Could not find CSX Historical_Data file. Tried: {tried[-5:]} (and more)")

def download_csx() -> str:
    url = discover_csx_historical()
    resp = http_get(url)
    fname = sanitize_filename(f"CSX_{os.path.basename(url)}")
    return save_bytes(resp.content, fname)

# =========================
# CSX AAR (PDF)
# =========================
def download_csx_aar(max_back_weeks: int = 12) -> str:
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

    raise FileNotFoundError(f"❌ No CSX AAR PDF found in last {max_back_weeks} weeks. Tried {tried_urls[-5:]}")

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
    """
    Downloads the latest BNSF Weekly Surface Transportation Board Update Excel file
    from https://www.bnsf.com/news-media/customer-notifications/notification.page?notId=weekly-surface-transportation-board-update
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
