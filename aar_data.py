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
TIMEOUT_UP = 60
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
# NS (fixed)
# =========================
def download_ns() -> List[str]:
    """
    Download Norfolk Southern's Weekly Performance Report (PDF)
    and the current month's Weekly Carloading Report (PDF).
    """
    r = http_get(NS_REPORTS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    saved = []

    # --- Weekly Performance Report (look for 2025_NS_Monthly_AAR_Performance.pdf) ---
    perf_link = None
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if href.endswith("2025_ns_monthly_aar_performance.pdf"):
            perf_link = normalize_url(NS_REPORTS_PAGE, a["href"])
            break
    if perf_link:
        print(f"⬇️ NS Performance PDF: {perf_link}")
        resp = http_get(perf_link, referer=NS_REPORTS_PAGE)
        saved.append(save_bytes(resp.content, f"NS_Performance_{datestamp()}.pdf"))
    else:
        print("⚠️ No Performance Report PDF found")

    # --- Weekly Carloading Reports (look for investor-weekly-carloads-<month>-2025.pdf) ---
    carload_link = None
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if "investor-weekly-carloads-" in href and href.endswith("2025.pdf"):
            carload_link = normalize_url(NS_REPORTS_PAGE, a["href"])
            break
    if carload_link:
        print(f"⬇️ NS Carloading PDF: {carload_link}")
        resp = http_get(carload_link, referer=NS_REPORTS_PAGE)
        saved.append(save_bytes(resp.content, f"NS_Carloads_{datestamp()}.pdf"))
    else:
        print("⚠️ No Carloading Report PDF found")

    if not saved:
        raise FileNotFoundError("NS reports not found")
    return saved

# =========================
# (Other railroads unchanged – CN, CPKC, CSX, UP, BNSF)
# =========================
# ... your existing functions for download_ep724, download_cn_perf,
# download_cn_rtm, download_cpkc_53week, download_cpkc_rtm, download_csx,
# download_up, download_bnsf remain here ...

# =========================
# Main
# =========================
def download_all():
    print(f"📂 Download folder: {DOWNLOAD_FOLDER}")
    fetched: List[str] = []
    tasks = [
        ("NS", download_ns),
        # add the other carriers here if you want them to run as well
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
