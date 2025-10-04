import os
import re
import datetime as dt
import requests
from bs4 import BeautifulSoup

# === CONFIG ===
STB_URL = "https://www.stb.gov/reports-data/rail-service-data/"
CN_PERF_URL = "https://www.cn.ca/-/media/files/investors/investor-performance-measures/perf_measures_en.xlsx"
CN_METRICS_PAGE = "https://www.cn.ca/en/investors/key-weekly-metrics/"
CSX_METRICS_PAGE = "https://investors.csx.com/metrics/default.aspx"
CPKC_CDN_BASE = "https://s21.q4cdn.com/736796105/files/doc_downloads"
CPKC_53WEEK_FILENAME = "CPKC-53-Week-Railway-Performance-Report.xlsx"

DOWNLOAD_FOLDER = os.getenv("STB_LOG_DIR", os.getcwd())
TIMEOUT = 15
UA = {"User-Agent": "Mozilla/5.0 (Python Excel downloader)"}

# --- Utility functions ---
def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def datestamp():
    return dt.date.today().strftime("%Y-%m-%d")

def sanitize_filename(name):
    return re.sub(r"[^\w\-.]+", "_", name)

def save_bytes(content, filename):
    ensure_dir(DOWNLOAD_FOLDER)
    full = os.path.join(DOWNLOAD_FOLDER, filename)
    with open(full, "wb") as f:
        f.write(content)
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

# --- EP724 ---
def get_latest_ep724_url():
    r = http_get(STB_URL)
    soup = BeautifulSoup(r.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if "EP724" in a["href"] and a["href"].endswith(".xlsx")]
    if not links:
        raise FileNotFoundError("No EP724 .xlsx link found")
    links.sort()
    url = links[-1]
    if not url.startswith("http"):
        url = "https://www.stb.gov" + url
    return url

def download_ep724():
    url = get_latest_ep724_url()
    resp = http_get(url)
    fname = f"EP724_{datestamp()}.xlsx"
    return save_bytes(resp.content, fname)

# --- CN performance measures ---
def download_cn_perf():
    resp = http_get(CN_PERF_URL)
    fname = f"CN_Performance_{datestamp()}.xlsx"
    return save_bytes(resp.content, fname)

# --- CN weekly RTM summary ---
def download_cn_rtm():
    r = http_get(CN_METRICS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".xlsx")]
    if not links:
        raise FileNotFoundError("No CN RTM .xlsx link found on metrics page")
    saved = []
    for url in links:
        if url.startswith("//"):
            url = "https:" + url
        elif url.startswith("/"):
            url = "https://www.cn.ca" + url
        fname = url.split("/")[-1]
        print(f"⬇️ Downloading CN RTM {fname}")
        resp = http_get(url)
        custom_name = f"CN_RTM_{datestamp()}.xlsx"
        saved.append(save_bytes(resp.content, custom_name))
    return saved

# --- CPKC 53-week report ---
def discover_cpkc_53week_url():
    today = dt.date.today()
    offset = (today.weekday() - 0) % 7  # Monday
    last_monday = today - dt.timedelta(days=offset)
    candidates = [last_monday, last_monday - dt.timedelta(days=7)]
    for d in candidates:
        url = f"{CPKC_CDN_BASE}/{d.strftime('%Y/%m/%d')}/{CPKC_53WEEK_FILENAME}"
        if http_head_ok(url):
            return url
    raise FileNotFoundError("CPKC 53-week file not found for last two Mondays.")

def download_cpkc_53week():
    url = discover_cpkc_53week_url()
    resp = http_get(url)
    fname = f"CPKC_53_Week_{datestamp()}.xlsx"
    return save_bytes(resp.content, fname)

# --- CPKC Weekly RTMs & Carloads ---
def discover_cpkc_rtm_url():
    today = dt.date.today()
    for delta in range(0, 14):  # probe last 14 days
        d = today - dt.timedelta(days=delta)
        url = f"{CPKC_CDN_BASE}/{d.strftime('%Y/%m/%d')}/CPKC-Weekly-RTMs-and-Carloads-{d.year}.xlsx"
        if http_head_ok(url):
            return url
    raise FileNotFoundError("CPKC Weekly RTM/Carloads file not found in last 14 days.")

def download_cpkc_rtm():
    url = discover_cpkc_rtm_url()
    resp = http_get(url)
    fname = f"CPKC_Weekly_RTM_{datestamp()}.xlsx"
    return save_bytes(resp.content, fname)

# --- CSX (both files) ---
def download_csx_files():
    r = http_get(CSX_METRICS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".xlsx")]
    if not links:
        raise FileNotFoundError("No CSX .xlsx links found")
    saved = []
    for url in links:
        if url.startswith("//"):
            url = "https:" + url
        elif url.startswith("/"):
            url = "https://investors.csx.com" + url
        fname = url.split("/")[-1]
        print(f"⬇️ Downloading CSX {fname}")
        resp = http_get(url)
        # Standardize names: CSX_Historical / CSX_Week1
        custom_name = f"CSX_{datestamp()}_{fname}"
        saved.append(save_bytes(resp.content, custom_name))
    return saved

# --- MAIN ---
def main():
    print(f"📂 Download folder: {DOWNLOAD_FOLDER}")
    fetched = []

    # EP724 + CN
    for name, fn in [
        ("EP724", download_ep724),
        ("CN Performance", download_cn_perf),
    ]:
        try:
            path = fn()
            fetched.append(path)
        except Exception as e:
            print(f"❌ {name} failed: {e}")

    # CN RTM
    try:
        cn_rtm_files = download_cn_rtm()
        fetched.extend(cn_rtm_files)
    except Exception as e:
        print(f"❌ CN RTM summary failed: {e}")

    # CPKC 53-week
    try:
        f = download_cpkc_53week()
        fetched.append(f)
    except Exception as e:
        print(f"❌ CPKC 53-week failed: {e}")

    # CPKC Weekly RTM
    try:
        f = download_cpkc_rtm()
        fetched.append(f)
    except Exception as e:
        print(f"❌ CPKC Weekly RTM failed: {e}")

    # CSX files
    try:
        csx_files = download_csx_files()
        fetched.extend(csx_files)
    except Exception as e:
        print(f"❌ CSX files failed: {e}")

    if fetched:
        print("✅ Files downloaded:")
        for f in fetched:
            print(" •", f)
    else:
        print("❌ No files downloaded.")

if __name__ == "__main__":
    main()
