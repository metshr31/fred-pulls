import os
import re
import datetime as dt
import requests
from bs4 import BeautifulSoup
import pandas as pd

# === CONFIG ===
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
TIMEOUT = 20
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) excel-fetcher"}

# --- Utility functions ---
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

# --- PDF to Excel converter ---
def pdf_to_excel(pdf_path, xlsx_path):
    try:
        import camelot
        tables = camelot.read_pdf(pdf_path, pages="all")
        if not tables:
            print(f"⚠️ No tables found in {pdf_path}")
            return None
        writer = pd.ExcelWriter(xlsx_path, engine="xlsxwriter")
        for i, t in enumerate(tables):
            t.df.to_excel(writer, sheet_name=f"Table{i+1}", index=False)
        writer.close()
        print(f"✅ Converted {pdf_path} → {xlsx_path}")
        return xlsx_path
    except Exception as e:
        print(f"⚠️ Could not convert {pdf_path} to Excel: {e}")
        return None

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
def download_cn_perf():
    resp = http_get(CN_PERF_URL)
    fname = f"CN_Performance_{datestamp()}.xlsx"
    return save_bytes(resp.content, fname)

def download_cn_rtm():
    r = http_get(CN_METRICS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".xlsx")]
    if not links: raise FileNotFoundError("No CN RTM .xlsx link found")
    saved = []
    for url in links:
        if url.startswith("//"): url = "https:" + url
        elif url.startswith("/"): url = "https://www.cn.ca" + url
        fname = url.split("/")[-1]
        print(f"⬇️ Downloading CN RTM {fname}")
        resp = http_get(url)
        custom_name = f"CN_RTM_{datestamp()}_{fname}"
        saved.append(save_bytes(resp.content, custom_name))
    return saved

# === CPKC ===
def discover_cpkc_53week_url():
    today = dt.date.today()
    offset = (today.weekday() - 0) % 7
    last_monday = today - dt.timedelta(days=offset)
    candidates = [last_monday, last_monday - dt.timedelta(days=7)]
    for d in candidates:
        url = f"{CPKC_CDN_BASE}/{d.strftime('%Y/%m/%d')}/{CPKC_53WEEK_FILENAME}"
        if http_head_ok(url): return url
    raise FileNotFoundError("CPKC 53-week file not found for last two Mondays.")

def download_cpkc_53week():
    url = discover_cpkc_53week_url()
    resp = http_get(url)
    fname = f"CPKC_53_Week_{datestamp()}.xlsx"
    return save_bytes(resp.content, fname)

def discover_cpkc_rtm_url():
    today = dt.date.today()
    for delta in range(0, 14):
        d = today - dt.timedelta(days=delta)
        url = f"{CPKC_CDN_BASE}/{d.strftime('%Y/%m/%d')}/CPKC-Weekly-RTMs-and-Carloads-{d.year}.xlsx"
        if http_head_ok(url): return url
    raise FileNotFoundError("CPKC Weekly RTM/Carloads not found in last 14 days.")

def download_cpkc_rtm():
    url = discover_cpkc_rtm_url()
    resp = http_get(url)
    fname = f"CPKC_Weekly_RTM_{datestamp()}.xlsx"
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

def download_csx_weekly_carload():
    print("🌐 Fetching CSX Weekly Carload Report …")
    r = http_get(CSX_METRICS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")

    links = [(a.text.strip(), a["href"]) for a in soup.find_all("a", href=True) if "Week" in a.text and a["href"].endswith(".xlsx")]
    if not links:
        raise FileNotFoundError("No CSX Weekly Carload Report link found.")

    latest_text, latest_href = links[0]

    if latest_href.startswith("//"):
        latest_href = "https:" + latest_href
    elif latest_href.startswith("/"):
        latest_href = "https://investors.csx.com" + latest_href

    week_match = re.search(r"Week\s+(\d+)", latest_text)
    week_str = f"Week{week_match.group(1)}" if week_match else "Week_Unknown"

    resp = http_get(latest_href)
    server_name = latest_href.rstrip("/").rsplit("/", 1)[-1]

    fname = sanitize_filename(f"CSX_WeeklyCarload_{datestamp()}_{week_str}_{server_name}")
    return save_bytes(resp.content, fname)

# === UP ===
def download_up():
    saved = []
    for label, url in UP_FILES.items():
        print(f"⬇️ Downloading UP {label}")
        resp = http_get(url)
        fname = f"UP_{label}_{datestamp()}.xlsx"
        saved.append(save_bytes(resp.content, fname))
    return saved

# === NS ===
def download_ns():
    r = http_get(NS_REPORTS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith((".pdf", ".xlsx"))]

    perf_links = [l for l in links if "performance" in l.lower()]
    carload_links = [l for l in links if "carload" in l.lower()]

    saved = []

    if perf_links:
        latest_perf = perf_links[0]
        if latest_perf.startswith("/"):
            latest_perf = "https://norfolksouthern.investorroom.com" + latest_perf
        resp = http_get(latest_perf)
        pdf_name = f"NS_PerformanceReport_{datestamp()}.pdf"
        pdf_path = save_bytes(resp.content, pdf_name)
        saved.append(pdf_path)

        xlsx_name = pdf_name.replace(".pdf", ".xlsx")
        pdf_to_excel(pdf_path, xlsx_name)
        if os.path.isfile(xlsx_name):
            saved.append(os.path.join(DOWNLOAD_FOLDER, xlsx_name))

    if carload_links:
        latest_carload = carload_links[0]
        if latest_carload.startswith("/"):
            latest_carload = "https://norfolksouthern.investorroom.com" + latest_carload
        resp = http_get(latest_carload)
        ext = ".xlsx" if latest_carload.endswith(".xlsx") else ".pdf"
        fname = f"NS_Carloading_{datestamp()}{ext}"
        path = save_bytes(resp.content, fname)
        saved.append(path)

        if ext == ".pdf":
            xlsx_name = fname.replace(".pdf", ".xlsx")
            pdf_to_excel(path, xlsx_name)
            if os.path.isfile(xlsx_name):
                saved.append(os.path.join(DOWNLOAD_FOLDER, xlsx_name))

    return saved

# === BNSF ===
def download_bnsf():
    print("🌐 Fetching BNSF Current Weekly Carload Report …")
    r = http_get(BNSF_REPORTS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")

    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".pdf")]
    if not links:
        raise FileNotFoundError("No BNSF Carload PDF link found.")

    latest_pdf = links[0]
    if latest_pdf.startswith("/"):
        latest_pdf = "https://www.bnsf.com" + latest_pdf

    resp = http_get(latest_pdf)
    pdf_name = f"BNSF_Carloads_{datestamp()}.pdf"
    pdf_path = save_bytes(resp.content, pdf_name)

    saved = [pdf_path]

    xlsx_name = pdf_name.replace(".pdf", ".xlsx")
    pdf_to_excel(pdf_path, xlsx_name)
    if os.path.isfile(xlsx_name):
        saved.append(os.path.join(DOWNLOAD_FOLDER, xlsx_name))

    return saved

# === MAIN ===
def main():
    print(f"📂 Download folder: {DOWNLOAD_FOLDER}")
    fetched = []
    tasks = [
        ("EP724", download_ep724),
        ("CN Performance", download_cn_perf),
        ("CN RTM", download_cn_rtm),
        ("CPKC 53-week", download_cpkc_53week),
        ("CPKC Weekly RTM", download_cpkc_rtm),
        ("CSX", download_csx),
        ("CSX Weekly Carload", download_csx_weekly_carload),
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
