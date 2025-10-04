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
CSX_CDN_BASE = "https://s2.q4cdn.com/859568992/files/doc_downloads"

CPKC_CDN_BASE = "https://s21.q4cdn.com/736796105/files/doc_downloads"
CPKC_53WEEK_FILENAME = "CPKC-53-Week-Railway-Performance-Report.xlsx"

UP_FILES = {
    "RTM_Carloadings": "https://investor.unionpacific.com/static-files/42fe7816-51a0-4844-9e24-ab51fb378299",
    "Performance_Measures": "https://investor.unionpacific.com/static-files/cedd1572-83c5-49e4-9bc2-753e75ed6814",
}

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
            print(f"⚠️ Attempt {attempt} failed for {url}: {e} — retrying in {backoff}s")
            time.sleep(backoff)

def http_head_ok(url, timeout=None):
    try:
        r = requests.head(url, headers=UA, timeout=timeout or TIMEOUT_DEFAULT, allow_redirects=True)
        ctype = r.headers.get("Content-Type", "").lower()
        return r.status_code == 200 and "text/html" not in ctype
    except Exception:
        return False

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
        print(f"⬇️ CN RTM {fname}")
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
        if http_head_ok(url): return url
    raise FileNotFoundError("CPKC 53-week not found")

def download_cpkc_53week():
    resp = http_get(discover_cpkc_53week_url())
    return save_bytes(resp.content, f"CPKC_53_Week_{datestamp()}.xlsx")

def discover_cpkc_rtm_url():
    today = dt.date.today()
    for delta in range(14):
        d = today - dt.timedelta(days=delta)
        url = f"{CPKC_CDN_BASE}/{d.strftime('%Y/%m/%d')}/CPKC-Weekly-RTMs-and-Carloads-{d.year}.xlsx"
        if http_head_ok(url): return url
    raise FileNotFoundError("CPKC Weekly RTM not found")

def download_cpkc_rtm():
    resp = http_get(discover_cpkc_rtm_url())
    return save_bytes(resp.content, f"CPKC_Weekly_RTM_{datestamp()}.xlsx")

# =========================
# CSX (Historical + Weekly Carload)
# =========================
def discover_csx_historical_url(max_back_days=10):
    today = dt.date.today()
    last_week_end = today - dt.timedelta(days=today.weekday() + 2)
    year, week = last_week_end.isocalendar()[0], last_week_end.isocalendar()[1]
    for delta in range(max_back_days):
        d = today - dt.timedelta(days=delta)
        folder = d.strftime("%Y/%m/%d")
        for fname in [f"Historical_Data_Week_{week}_{year}.xlsx",
                      f"Combined-Intermodal-and-Carload-TPC-Week-1-2022-Week-{week}-{year}.xlsx"]:
            url = f"{CSX_CDN_BASE}/{folder}/{fname}"
            if http_head_ok(url): return url
    # fallback parse
    r = http_get(CSX_METRICS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        if a["href"].endswith(".xlsx"):
            u = normalize_url("https://investors.csx.com", a["href"])
            if http_head_ok(u): return u
    raise FileNotFoundError("CSX Historical Excel not found")

def download_csx_historical():
    url = discover_csx_historical_url()
    resp = http_get(url)
    fname = sanitize_filename(f"CSX_{datestamp()}_{url.split('/')[-1]}")
    return save_bytes(resp.content, fname)

def download_csx_weekly_carload():
    url = discover_csx_historical_url()
    resp = http_get(url)
    fname = sanitize_filename(f"CSX_WeeklyCarload_{datestamp()}_{url.split('/')[-1]}")
    return save_bytes(resp.content, fname)

# =========================
# UP
# =========================
def download_up():
    saved = []
    for label, url in UP_FILES.items():
        print(f"⬇️ UP {label}")
        resp = http_get(url, timeout=TIMEOUT_UP, retries=3)
        fname = f"UP_{label}_{datestamp()}.xlsx"
        saved.append(save_bytes(resp.content, fname))
        time.sleep(1)
    return saved

# =========================
# NS
# =========================
def download_ns():
    r = http_get(NS_REPORTS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    anchors = soup.find_all("a", href=True)

    saved = []
    for a in anchors:
        href = a["href"]
        text = (a.get_text() or "").lower()
        if href.lower().endswith(".xlsx") and "performance" in text:
            url = normalize_url("https://norfolksouthern.investorroom.com", href)
            resp = http_get(url, referer=NS_REPORTS_PAGE, retries=3)
            saved.append(save_bytes(resp.content, f"NS_Performance_{datestamp()}.xlsx"))
        if href.lower().endswith(".pdf") and "carload" in text:
            url = normalize_url("https://norfolksouthern.investorroom.com", href)
            resp = http_get(url, referer=NS_REPORTS_PAGE, retries=3)
            saved.append(save_bytes(resp.content, f"NS_Carloads_{datestamp()}.pdf"))
    if not saved: raise FileNotFoundError("NS reports not found")
    return saved

# =========================
# BNSF
# =========================
def download_bnsf():
    r = http_get(BNSF_REPORTS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        if "current weekly carload report" in (a.get_text() or "").lower():
            url = normalize_url("https://www.bnsf.com", a["href"])
            resp = http_get(url, retries=3)
            return save_bytes(resp.content, f"BNSF_Carloads_{datestamp()}.pdf")
    raise FileNotFoundError("BNSF weekly carload not found")

# =========================
# Main
# =========================
def main():
    print(f"📂 Download folder: {DOWNLOAD_FOLDER}")
    fetched = []
    tasks = [
        ("EP724", download_ep724),
        ("CN Perf", download_cn_perf),
        ("CN RTM", download_cn_rtm),
        ("CPKC 53W", download_cpkc_53week),
        ("CPKC RTM", download_cpkc_rtm),
        ("CSX Hist", download_csx_historical),
        ("CSX Weekly", download_csx_weekly_carload),
        ("UP", download_up),
        ("NS", download_ns),
        ("BNSF", download_bnsf),
    ]
    for name, fn in tasks:
        try:
            result = fn()
            if isinstance(result, list): fetched.extend(result)
            else: fetched.append(result)
        except Exception as e:
            print(f"❌ {name} failed: {e}")
    if fetched:
        print("✅ Files downloaded:")
        for f in fetched: print(" •", f)
    else:
        print("❌ No files downloaded.")

if __name__ == "__main__":
    main()
