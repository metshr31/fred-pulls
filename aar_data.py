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
# NS (corrected)
# =========================
def download_ns() -> List[str]:
    """
    Download Norfolk Southern's Weekly Performance Report (PDF)
    and the most recent Weekly Carloading Report (PDF).
    """
    r = http_get(NS_REPORTS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    saved = []

    # --- Weekly Performance Report ---
    perf_link = None
    for a in soup.find_all("a", href=True):
        txt = (a.get_text() or "").strip().lower()
        href = a["href"].lower()
        if "weekly performance report" in txt and href.endswith(".pdf"):
            perf_link = normalize_url(NS_REPORTS_PAGE, a["href"])
            break
    if perf_link:
        print(f"⬇️ NS Performance PDF: {perf_link}")
        resp = http_get(perf_link, referer=NS_REPORTS_PAGE)
        saved.append(save_bytes(resp.content, f"NS_Performance_{datestamp()}.pdf"))
    else:
        print("⚠️ No Weekly Performance Report link found")

    # --- Weekly Carloading Reports ---
    carload_page = None
    for a in soup.find_all("a", href=True):
        if "weekly carloading reports" in (a.get_text() or "").lower():
            carload_page = normalize_url(NS_REPORTS_PAGE, a["href"])
            break
    if carload_page:
        sub = http_get(carload_page)
        subsoup = BeautifulSoup(sub.text, "html.parser")
        pdfs = [normalize_url(carload_page, a["href"])
                for a in subsoup.find_all("a", href=True)
                if a["href"].lower().endswith(".pdf")]
        if pdfs:
            latest_carload = sorted(pdfs)[-1]
            print(f"⬇️ NS Carload PDF: {latest_carload}")
            resp = http_get(latest_carload, referer=carload_page)
            saved.append(save_bytes(resp.content, f"NS_Carloads_{datestamp()}.pdf"))
        else:
            print("⚠️ No carloading PDFs found")
    else:
        print("⚠️ No Weekly Carloading Reports section found")

    if not saved:
        raise FileNotFoundError("NS reports not found")
    return saved

# =========================
# (Other railroads unchanged – CN, CPKC, CSX, UP, BNSF)
# =========================
# ... keep your existing download_ep724, download_cn_perf, download_cn_rtm,
# download_cpkc_53week, download_cpkc_rtm, download_csx, download_up, download_bnsf here ...

# =========================
# Main
# =========================
def download_all():
    print(f"📂 Download folder: {DOWNLOAD_FOLDER}")
    fetched: List[str] = []
    tasks = [
        # Add back your other tasks here
        ("NS", download_ns),
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
