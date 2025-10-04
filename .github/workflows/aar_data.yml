# aar_data.py
import os
import re
import datetime as dt
import requests
from bs4 import BeautifulSoup

# === CONFIG ===
STB_URL = "https://www.stb.gov/reports-data/rail-service-data/"
CN_URL = "https://www.cn.ca/-/media/files/investors/investor-performance-measures/perf_measures_en.xlsx"
DOWNLOAD_FOLDER = os.getenv("STB_LOG_DIR", os.getcwd())
TIMEOUT = 12
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) excel-fetcher"}

CSX_METRICS_PAGE = "https://investors.csx.com/metrics/default.aspx"
CSX_CDN_BASE = "https://s2.q4cdn.com/859568992/files/doc_downloads"
CPKC_CDN_BASE = "https://s21.q4cdn.com/736796105/files/doc_downloads"
CPKC_FILENAME = "CPKC-53-Week-Railway-Performance-Report.xlsx"

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
    try:
        r = requests.head(url, headers=UA, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code == 200 and "text/html" not in r.headers.get("Content-Type", "").lower():
            return True
    except requests.RequestException:
        return False
    return False

# === EP724 ===
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
    url = get_latest_ep724_url()
    resp = http_get(url)
    fname = f"EP724_{datestamp()}.xlsx"
    return save_bytes(resp.content, fname)

# === CN ===
def download_cn():
    resp = http_get(CN_URL)
    fname = f"CN_perf_measures_{datestamp()}.xlsx"
    return save_bytes(resp.content, fname)

# === CPKC ===
def discover_cpkc_url():
    today = dt.date.today()
    offset = (today.weekday() - 0) % 7
    last_monday = today - dt.timedelta(days=offset)
    candidates = [last_monday, last_monday - dt.timedelta(days=7)]
    for d in candidates:
        url = f"{CPKC_CDN_BASE}/{d.strftime('%Y/%m/%d')}/{CPKC_FILENAME}"
        if http_head_ok(url): return url
    raise FileNotFoundError("CPKC file not found for last two Mondays.")

def download_cpkc():
    url = discover_cpkc_url()
    resp = http_get(url)
    fname = f"CPKC_53_week_report_{datestamp()}.xlsx"
    return save_bytes(resp.content, fname)

# === CSX ===
def _iso_week_year(date_obj): return date_obj.isocalendar()[0], date_obj.isocalendar()[1]
def _csx_candidate_filenames(year, week):
    return [
        f"Historical_Data_Week_{week}_{year}.xlsx",
        f"Combined-Intermodal-and-Carload-TPC-Week-1-2022-Week-{week}-{year}.xlsx",
    ]

def discover_csx_url(max_back_days=10):
    today = dt.date.today()
    last_week_end = today - dt.timedelta(days=today.weekday() + 2)
    year, week = _iso_week_year(last_week_end)
    for delta in range(max_back_days):
        d = today - dt.timedelta(days=delta)
        folder = d.strftime("%Y/%m/%d")
        for fname in _csx_candidate_filenames(year, week):
            url = f"{CSX_CDN_BASE}/{folder}/{fname}"
            if http_head_ok(url): return url
    r = http_get(CSX_METRICS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".xlsx")]
    for u in links:
        if u.startswith("//"): u = "https:" + u
        elif u.startswith("/"): u = "https://investors.csx.com" + u
        if http_head_ok(u): return u
    raise FileNotFoundError("CSX Excel not found.")

def download_csx():
    url = discover_csx_url()
    resp = http_get(url)
    server_name = url.rstrip("/").rsplit("/", 1)[-1]
    fname = sanitize_filename(f"CSX_{datestamp()}_{server_name}")
    return save_bytes(resp.content, fname)

# === MAIN ===
def main():
    print(f"📂 Download folder: {DOWNLOAD_FOLDER}")
    fetched = []
    tasks = [("EP724", download_ep724), ("CN", download_cn), ("CPKC", download_cpkc), ("CSX", download_csx)]
    for name, fn in tasks:
        try:
            path = fn()
            fetched.append(path)
        except Exception as e:
            print(f"❌ {name} failed: {e}")
    if fetched:
        print("✅ Files downloaded:")
        for f in fetched: print(" •", f)
    else:
        print("❌ No files downloaded.")

if __name__ == "__main__":
    main()
