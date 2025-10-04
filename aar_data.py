import os
import re
import time
import datetime as dt
import requests
from bs4 import BeautifulSoup
import pandas as pd

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

# UP static file endpoints (these are stable "static-files/<uuid>" links behind the page)
UP_FILES = {
    # Revenue Ton Miles (RTMs)/Carloadings YTD By Key Market Segments
    "RTM_Carloadings": "https://investor.unionpacific.com/static-files/42fe7816-51a0-4844-9e24-ab51fb378299",
    # Historical Weekly Performance Measures
    "Performance_Measures": "https://investor.unionpacific.com/static-files/cedd1572-83c5-49e4-9bc2-753e75ed6814",
}

NS_REPORTS_PAGE = "https://norfolksouthern.investorroom.com/weekly-performance-reports"
BNSF_REPORTS_PAGE = "https://www.bnsf.com/about-bnsf/financial-information/weekly-carload-reports/"

DOWNLOAD_FOLDER = os.getenv("STB_LOG_DIR", os.getcwd())

# Default timeout; some domains override below
TIMEOUT_DEFAULT = 20
TIMEOUT_UP = 60  # UP can be slow; bump timeout

UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
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
    with open(full, "wb") as f:
        f.write(content)
    print(f"✅ Saved: {full}")
    return full

def _requests_session(timeout=TIMEOUT_DEFAULT):
    s = requests.Session()
    s.headers.update(UA)
    try:
        # add retries
        from urllib3.util.retry import Retry
        from requests.adapters import HTTPAdapter
        retry = Retry(total=3, connect=3, read=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.mount("http://", HTTPAdapter(max_retries=retry))
    except Exception:
        pass
    s.request = _with_timeout(s.request, timeout)
    return s

def _with_timeout(fn, timeout):
    def wrapper(method, url, **kwargs):
        kwargs.setdefault("timeout", timeout)
        kwargs.setdefault("allow_redirects", True)
        return fn(method, url, **kwargs)
    return wrapper

def http_get(url, timeout=None):
    if "investor.unionpacific.com" in url:
        s = _requests_session(timeout=TIMEOUT_UP)
    else:
        s = _requests_session(timeout or TIMEOUT_DEFAULT)
    r = s.get(url)
    r.raise_for_status()
    return r

def http_head_ok(url, timeout=None):
    try:
        if "investor.unionpacific.com" in url:
            s = _requests_session(timeout=TIMEOUT_UP)
        else:
            s = _requests_session(timeout or TIMEOUT_DEFAULT)
        r = s.head(url)
        ctype = r.headers.get("Content-Type", "").lower()
        return (r.status_code == 200 and "text/html" not in ctype)
    except requests.RequestException:
        return False

def normalize_url(base, href):
    """Turn relative or protocol-relative href into absolute based on base domain."""
    if not href:
        return None
    href = href.strip()
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        # base like https://host/path → keep scheme+host
        from urllib.parse import urlparse
        p = urlparse(base)
        return f"{p.scheme}://{p.netloc}{href}"
    # bare relative like 'image/foo.pdf'
    if not href.startswith("http"):
        # ensure one slash
        if not base.endswith("/") and not href.startswith("/"):
            return base + "/" + href
        return base + href
    return href

# Optional PDF -> Excel helper (kept because it worked for BNSF/NS)
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

# =========================
# STB EP724
# =========================
def get_latest_ep724_url():
    r = http_get(STB_URL)
    soup = BeautifulSoup(r.text, "html.parser")
    links = [a.get("href") for a in soup.find_all("a", href=True)
             if "EP724" in a["href"] and a["href"].endswith(".xlsx")]
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

# =========================
# CN
# =========================
def download_cn_perf():
    resp = http_get(CN_PERF_URL)
    fname = f"CN_Performance_{datestamp()}.xlsx"
    return save_bytes(resp.content, fname)

def download_cn_rtm():
    r = http_get(CN_METRICS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".xlsx")]
    if not links:
        raise FileNotFoundError("No CN RTM .xlsx link found")
    saved = []
    for href in links:
        url = normalize_url("https://www.cn.ca", href)
        fname = url.rsplit("/", 1)[-1]
        print(f"⬇️ Downloading CN RTM {fname}")
        resp = http_get(url)
        custom_name = f"CN_RTM_{datestamp()}_{fname}"
        saved.append(save_bytes(resp.content, custom_name))
    return saved

# =========================
# CPKC
# =========================
def discover_cpkc_53week_url():
    today = dt.date.today()
    offset = (today.weekday() - 0) % 7  # Monday index 0
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

def discover_cpkc_rtm_url():
    today = dt.date.today()
    # try last two weeks of folders
    for delta in range(0, 14):
        d = today - dt.timedelta(days=delta)
        url = f"{CPKC_CDN_BASE}/{d.strftime('%Y/%m/%d')}/CPKC-Weekly-RTMs-and-Carloads-{d.year}.xlsx"
        if http_head_ok(url):
            return url
    raise FileNotFoundError("CPKC Weekly RTM/Carloads not found in last 14 days.")

def download_cpkc_rtm():
    url = discover_cpkc_rtm_url()
    resp = http_get(url)
    fname = f"CPKC_Weekly_RTM_{datestamp()}.xlsx"
    return save_bytes(resp.content, fname)

# =========================
# CSX (Historical + Weekly Carload as separate saves)
# =========================
def _iso_week_year(date_obj):
    iso = date_obj.isocalendar()
    return iso[0], iso[1]

def _csx_candidate_filenames(year, week):
    return [
        f"Historical_Data_Week_{week}_{year}.xlsx",
        f"Combined-Intermodal-and-Carload-TPC-Week-1-2022-Week-{week}-{year}.xlsx",
    ]

def discover_csx_historical_url(max_back_days=10):
    # Try CDN folders for the most recent calendar days with the historical filename format
    today = dt.date.today()
    # Use last week's end (Saturday-ish) to determine AAR week
    last_week_end = today - dt.timedelta(days=today.weekday() + 2)
    year, week = _iso_week_year(last_week_end)
    for delta in range(max_back_days):
        d = today - dt.timedelta(days=delta)
        folder = d.strftime("%Y/%m/%d")
        for fname in _csx_candidate_filenames(year, week):
            url = f"{CSX_CDN_BASE}/{folder}/{fname}"
            if http_head_ok(url):
                return url
    # Fallback: parse page for any direct .xlsx links (may 403 but worth a last try)
    try:
        r = http_get(CSX_METRICS_PAGE)
        soup = BeautifulSoup(r.text, "html.parser")
        links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".xlsx")]
        for u in links:
            u = normalize_url("https://investors.csx.com", u)
            if http_head_ok(u):
                return u
    except Exception:
        pass
    raise FileNotFoundError("CSX Historical Excel not found.")

def download_csx_historical():
    url = discover_csx_historical_url()
    resp = http_get(url)
    server_name = url.rstrip("/").rsplit("/", 1)[-1]
    fname = sanitize_filename(f"CSX_{datestamp()}_{server_name}")
    return save_bytes(resp.content, fname)

def download_csx_weekly_carload():
    """
    CSX Weekly Carload section often links to the same historical-week Excel.
    To avoid 403 on the landing page, reuse the discovered CDN URL and save
    a second copy named with WeekXX.
    """
    url = discover_csx_historical_url()
    # try to parse week from filename
    fn = url.rsplit("/", 1)[-1]
    m = re.search(r"[Ww]eek[_\- ](\d{1,2}).*?(\d{4})", fn)
    week_str = f"Week{m.group(1)}" if m else "Week_Unknown"
    resp = http_get(url)
    out = sanitize_filename(f"CSX_WeeklyCarload_{datestamp()}_{week_str}_{fn}")
    return save_bytes(resp.content, out)

# =========================
# UP (with longer timeout + retries)
# =========================
def download_up():
    saved = []
    for label, url in UP_FILES.items():
        print(f"⬇️ Downloading UP {label}")
        resp = http_get(url, )
        ext = ".xlsx" if ".xlsx" in resp.headers.get("Content-Disposition", "").lower() or url.endswith(".xlsx") else ".xlsx"
        # We’ll still save with .xlsx; UP static files are Excel behind generic path
        fname = f"UP_{label}_{datestamp()}{ext}"
        saved.append(save_bytes(resp.content, fname))
        # small pause between large files
        time.sleep(0.5)
    return saved

# =========================
# NS (normalize relative links; convert PDF to Excel if needed)
# =========================
def download_ns():
    r = http_get(NS_REPORTS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    anchors = soup.find_all("a", href=True)

    perf_links = []
    carload_links = []

    for a in anchors:
        href = a["href"]
        text = (a.get_text() or "").lower()
        # candidate file types
        if not (href.lower().endswith(".pdf") or href.lower().endswith(".xlsx")):
            continue
        # bucket by text hints
        if "performance" in text or "weekly performance" in text:
            perf_links.append(href)
        if "carload" in text or "carloading" in text or "weekly aar" in text:
            carload_links.append(href)

    saved = []

    # Performance report (often PDF)
    if perf_links:
        latest = normalize_url(NS_REPORTS_PAGE, perf_links[0])
        resp = http_get(latest)
        perf_pdf = f"NS_PerformanceReport_{datestamp()}.pdf" if latest.lower().endswith(".pdf") else f"NS_PerformanceReport_{datestamp()}.xlsx"
        path = save_bytes(resp.content, perf_pdf)
        saved.append(path)
        # convert if PDF
        if perf_pdf.endswith(".pdf"):
            xlsx = perf_pdf.replace(".pdf", ".xlsx")
            if pdf_to_excel(path, xlsx):
                saved.append(os.path.join(DOWNLOAD_FOLDER, xlsx))

    # Weekly carloading report (PDF or Excel)
    if carload_links:
        latest = normalize_url(NS_REPORTS_PAGE, carload_links[0])
        ext = ".pdf" if latest.lower().endswith(".pdf") else ".xlsx"
        resp = http_get(latest)
        out = f"NS_Carloading_{datestamp()}{ext}"
        path = save_bytes(resp.content, out)
        saved.append(path)
        if ext == ".pdf":
            xlsx = out.replace(".pdf", ".xlsx")
            if pdf_to_excel(path, xlsx):
                saved.append(os.path.join(DOWNLOAD_FOLDER, xlsx))

    if not saved:
        raise FileNotFoundError("No NS weekly performance or carloading links found.")
    return saved

# =========================
# BNSF (PDF + convert to Excel)
# =========================
def download_bnsf():
    print("🌐 Fetching BNSF Current Weekly Carload Report …")
    r = http_get(BNSF_REPORTS_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    # collect PDFs; typically first is current week
    pdfs = [normalize_url("https://www.bnsf.com", a["href"])
            for a in soup.find_all("a", href=True) if a["href"].lower().endswith(".pdf")]
    if not pdfs:
        raise FileNotFoundError("No BNSF Carload PDF link found.")
    latest_pdf = pdfs[0]
    resp = http_get(latest_pdf)
    pdf_name = f"BNSF_Carloads_{datestamp()}.pdf"
    pdf_path = save_bytes(resp.content, pdf_name)

    saved = [pdf_path]
    xlsx_name = pdf_name.replace(".pdf", ".xlsx")
    if pdf_to_excel(pdf_path, xlsx_name):
        saved.append(os.path.join(DOWNLOAD_FOLDER, xlsx_name))
    return saved

# =========================
# Main
# =========================
def main():
    print(f"📂 Download folder: {DOWNLOAD_FOLDER}")
    fetched = []
    tasks = [
        ("EP724", download_ep724),
        ("CN Performance", download_cn_perf),
        ("CN RTM", download_cn_rtm),
        ("CPKC 53-week", download_cpkc_53week),
        ("CPKC Weekly RTM", download_cpkc_rtm),
        ("CSX Historical", download_csx_historical),
        ("CSX Weekly Carload", download_csx_weekly_carload),
        ("UP", download_up),
        ("NS", download_ns),
        ("BNSF", download_bnsf),
    ]
    for name, fn in tasks:
        try:
            result = fn()
            if isinstance(result, list):
                fetched.extend(result)
            else:
                fetched.append(result)
        except Exception as e:
            print(f"❌ {name} failed: {e}")

    if fetched:
        print("✅ Files downloaded:")
        for f in fetched:
            print(" •", f)
    else:
        print("❌ No files downloaded.")

if __name__ == "__main__":
    main()
