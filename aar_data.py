# aar_data.py
import os
import re
import time
import datetime as dt
import requests
from bs4 import BeautifulSoup

# =========================
# Config
# =========================
STB_URL = "https://www.stb.gov/reports-data/rail-service-data/"
CN_PERF_URL = "https://www.cn.ca/-/media/files/investors/investor-performance-measures/perf_measures_en.xlsx"
CN_METRICS_PAGE = "https://www.cn.ca/en/investors/key-weekly-metrics/"

CSX_METRICS_PAGE = "https://investors.csx.com/metrics/default.aspx"
CPKC_CDN_BASE = "https://s21.q4cdn.com/736796105/files/doc_downloads"
CPKC_53WEEK_FILENAME = "CPKC-53-Week-Railway-Performance-Report.xlsx"

UP_METRICS_PAGE = "https://investor.unionpacific.com/key-performance-metrics"
NS_REPORTS_PAGE = "https://norfolksouthern.investorroom.com/weekly-performance-reports"
BNSF_REPORTS_PAGE = "https://www.bnsf.com/about-bnsf/financial-information/weekly-carload-reports/"

DOWNLOAD_FOLDER = os.getenv("STB_LOG_DIR", os.getcwd())

TIMEOUT_DEFAULT = 20
TIMEOUT_UP = 60  # UP is slow

UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

LOG_FILE = os.path.join(DOWNLOAD_FOLDER, "download_log.txt")


# =========================
# Utilities
# =========================
def ensure_dir(path): os.makedirs(path, exist_ok=True)
def datestamp(): return dt.date.today().strftime("%Y-%m-%d")
def sanitize_filename(name): return re.sub(r"[^\w\-.]+", "_", name)

def log(message):
    print(message)
    ensure_dir(DOWNLOAD_FOLDER)
    with open(LOG_FILE, "a") as f:
        f.write(message + "\n")

def save_bytes(content, filename):
    ensure_dir(DOWNLOAD_FOLDER)
    full = os.path.join(DOWNLOAD_FOLDER, filename)
    with open(full, "wb") as f: f.write(content)
    log(f"✅ Saved: {full}")
    return full

def http_get(url, timeout=None, referer=None, retries=3, backoff=5):
    headers = dict(UA)
    if referer:
        headers["Referer"] = referer
    t = TIMEOUT_UP if "unionpacific" in url else (timeout or TIMEOUT_DEFAULT)

    for attempt in range(1, retries+1):
        try:
            r = requests.get(url, headers=headers, timeout=t, allow_redirects=True)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == retries:
                raise
            log(f"⚠️ Attempt {attempt} failed for {url}: {e} — retrying in {backoff}s")
            time.sleep(backoff)

def normalize_url(base, href):
    if not href: return None
    href = href.strip()
    if href.startswith("//"): return "https:" + href
    if href.startswith("http"): return href
    if href.startswith("/"): return base.rstrip("/") + href
    return base.rstrip("/") + "/" + href


# =========================
# STB
# =========================
def get_latest_ep724_url():
    r = http_get(STB_URL)
    soup = BeautifulSoup(r.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if "EP724" in a["href"] and a["href"].endswith(".xlsx")]
    if not links: raise FileNotFoundError("No EP724 Excel found")
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
    saved = []
    for href in links:
        url = normalize_url("https://www.cn.ca", href)
        fname = url.rsplit("/", 1)[-1]
        log(f"⬇️ CN RTM {fname}")
        resp = http_get(url)
        saved.append(save_bytes(resp.content, f"CN_RTM_{datestamp()}_{fname}"))
    return saved


# =========================
# CPKC
# =========================
def discover_cpkc_53week_url():
    today = dt.date.today()
    last_mon = today - dt.timedelta(days=(today.weekday() - 0) % 7)
    for d in (last_mon, last_mon - dt.timedelta(days=7)):
        url = f"{CPKC_CDN_BASE}/{d.strftime('%Y/%m/%d')}/{CPKC_53WEEK_FILENAME}"
        r = requests.head(url, headers=UA)
        if r.status_code == 200: return url
    raise FileNotFoundError("CPKC 53-week not found")

def download_cpkc_53week():
    resp = http_get(discover_cpkc_53week_url())
    return save_bytes(resp.content, f"CPKC_53_Week_{datestamp()}.xlsx")

def discover_cpkc_rtm_url():
    today = dt.date.today()
    for delta in range(14):
        d = today - dt.timedelta(days=delta)
        url = f"{CPKC_CDN_BASE}/{d.strftime('%Y/%m/%d')}/CPKC-Weekly-RTMs-and-Carloads-{d.year}.xlsx"
        r = requests.head(url, headers=UA)
        if r.status_code == 200: return url
    raise FileNotFoundError("CPKC Weekly RTM not found")

def download_cpkc_rtm():
    resp = http_get(discover_cpkc_rtm_url())
    return save_bytes(resp.content, f"CPKC_Weekly_RTM_{datestamp()}.xlsx")


# =========================
# CSX (Historical + Weekly Carload)
# =========================
def download_csx_files():
    r = http_get(CSX_METRICS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    saved = []
    for a in soup.find_all("a", href=True):
        if a["href"].endswith(".xlsx"):
            url = normalize_url("https://investors.csx.com", a["href"])
            fname = sanitize_filename(f"CSX_{datestamp()}_{url.split('/')[-1]}")
            resp = http_get(url)
            saved.append(save_bytes(resp.content, fname))
    if not saved:
        raise FileNotFoundError("CSX Excel files not found")
    return saved


# =========================
# UP
# =========================
def download_up_files():
    r = http_get(UP_METRICS_PAGE, timeout=TIMEOUT_UP)
    soup = BeautifulSoup(r.text, "html.parser")
    saved = []
    for a in soup.find_all("a", href=True):
        if a["href"].endswith(".xlsx"):
            url = normalize_url("https://investor.unionpacific.com", a["href"])
            label = url.split("/")[-1]
            resp = http_get(url, timeout=TIMEOUT_UP)
            saved.append(save_bytes(resp.content, f"UP_{datestamp()}_{label}"))
    if not saved:
        raise FileNotFoundError("UP Excel files not found")
    return saved


# =========================
# NS
# =========================
def download_ns_files():
    r = http_get(NS_REPORTS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    saved = []
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        url = normalize_url("https://norfolksouthern.investorroom.com", a["href"])
        if href.endswith(".xlsx") and "performance" in (a.get_text() or "").lower():
            resp = http_get(url)
            saved.append(save_bytes(resp.content, f"NS_Performance_{datestamp()}.xlsx"))
        if href.endswith(".pdf") and "carload" in (a.get_text() or "").lower():
            resp = http_get(url)
            saved.append(save_bytes(resp.content, f"NS_Carloads_{datestamp()}.pdf"))
    if not saved:
        raise FileNotFoundError("NS reports not found")
    return saved


# =========================
# BNSF
# =========================
def download_bnsf_file():
    r = http_get(BNSF_REPORTS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        if "carload" in (a.get_text() or "").lower() and a["href"].endswith(".pdf"):
            url = normalize_url("https://www.bnsf.com", a["href"])
            resp = http_get(url)
            return save_bytes(resp.content, f"BNSF_Carloads_{datestamp()}.pdf")
    raise FileNotFoundError("BNSF weekly carload not found")


# =========================
# Main
# =========================
def main():
    ensure_dir(DOWNLOAD_FOLDER)
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)

    log(f"📂 Download folder: {DOWNLOAD_FOLDER}")
    fetched = []
    tasks = [
        ("EP724", download_ep724),
        ("CN Perf", download_cn_perf),
        ("CN RTM", download_cn_rtm),
        ("CPKC 53W", download_cpkc_53week),
        ("CPKC RTM", download_cpkc_rtm),
        ("CSX", download_csx_files),
        ("UP", download_up_files),
        ("NS", download_ns_files),
        ("BNSF", download_bnsf_file),
    ]
    for name, fn in tasks:
        try:
            result = fn()
            if isinstance(result, list): fetched.extend(result)
            else: fetched.append(result)
        except Exception as e:
            log(f"❌ {name} failed: {e}")

    if fetched:
        log("✅ Files downloaded:")
        for f in fetched: log(" • " + f)
    else:
        log("❌ No files downloaded.")


if __name__ == "__main__":
    main()
