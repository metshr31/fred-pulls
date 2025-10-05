import os
import re
import datetime as dt
import requests
import logging
import argparse
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

# === CONFIG ===
# Increased timeout to 90 seconds to handle slow servers (Union Pacific issue)
TIMEOUT = 90
UA = {"User-Agent": "Mozilla/5.0 excel-fetcher"}
# Environment variable to control the download location
DOWNLOAD_FOLDER = os.getenv("RAIL_METRICS_DIR", os.getcwd()) 

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === UTILS ===
def ensure_dir(path): os.makedirs(path, exist_ok=True)
def datestamp(): return dt.date.today().strftime("%Y-%m-%d")
# Replaces problematic characters with an underscore
def sanitize_filename(name): return re.sub(r"[^\w\-.]+", "_", name) 

def save_bytes(content, filename):
    clean_filename = sanitize_filename(filename) 
    ensure_dir(DOWNLOAD_FOLDER)
    full = os.path.join(DOWNLOAD_FOLDER, clean_filename)
    with open(full, "wb") as f: f.write(content)
    logging.info(f"✅ Saved: {full}")
    return full

def http_get(url):
    r = requests.get(url, headers=UA, timeout=TIMEOUT)
    r.raise_for_status()
    return r

def http_head_ok(url):
    try:
        r = requests.head(url, headers=UA, timeout=TIMEOUT, allow_redirects=True)
        return r.status_code == 200 and "text/html" not in r.headers.get("Content-Type", "").lower()
    except requests.RequestException:
        return False

# === BASE CLASS ===
class RailDownloader:
    def __init__(self, name): self.name = name
    def log(self, msg): logging.info(f"[{self.name}] {msg}")
    def fail(self, msg): logging.error(f"[{self.name}] ❌ {msg}")

# === STB ===
class STBDownloader(RailDownloader):
    URL = "https://www.stb.gov/reports-data/rail-service-data/"
    def download(self):
        self.log("Scraping EP724")
        r = http_get(self.URL)
        soup = BeautifulSoup(r.text, "html.parser")
        links = [a["href"] for a in soup.find_all("a", href=True) if "EP724" in a["href"] and a["href"].endswith(".xlsx")]
        if not links: raise FileNotFoundError("No EP724 .xlsx link found.")
        url = links[-1]
        if not url.startswith("http"): url = "https://www.stb.gov" + url
        self.log(f"Found: {url}")
        resp = http_get(url)
        return save_bytes(resp.content, f"EP724_{datestamp()}.xlsx")

# === CN ===
class CNDownloader(RailDownloader):
    PERF_URL = "https://www.cn.ca/-/media/files/investors/investor-performance-measures/perf_measures_en.xlsx"
    METRICS_PAGE = "https://www.cn.ca/en/investors/key-weekly-metrics/"
    def download_perf(self):
        self.log("Downloading CN Performance")
        resp = http_get(self.PERF_URL)
        return save_bytes(resp.content, f"CN_Performance_{datestamp()}.xlsx")
    def download_rtm(self):
        self.log("Scraping CN RTM")
        r = http_get(self.METRICS_PAGE)
        soup = BeautifulSoup(r.text, "html.parser")
        links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".xlsx")]
        if not links: raise FileNotFoundError("No CN RTM .xlsx links found.")
        saved = []
        for url in links:
            if url.startswith("//"): url = "https:" + url
            elif url.startswith("/"): url = "https://www.cn.ca" + url
            
            fname_part = url.split('/')[-1]
            fname = f"CN_RTM_{datestamp()}_{fname_part}" 
            
            try:
                resp = http_get(url)
                saved.append(save_bytes(resp.content, fname))
            except Exception as e:
                self.fail(f"Failed to download {url}: {e}")
        if not saved: raise Exception("CN RTM links found but none downloaded.")
        return saved

# === CPKC ===
class CPKCDownloader(RailDownloader):
    CDN_BASE = "https://s21.q4cdn.com/736796105/files/doc_downloads"
    FILENAME_53WEEK = "CPKC-53-Week-Railway-Performance-Report.xlsx"
    def discover_53week_url(self):
        today = dt.date.today()
        mondays = [today - dt.timedelta(days=(today.weekday() - i) % 7) for i in [0, 7]] 
        for d in mondays:
            url = f"{self.CDN_BASE}/{d.strftime('%Y/%m/%d')}/{self.FILENAME_53WEEK}"
            if http_head_ok(url): return url
        raise FileNotFoundError("CPKC 53-week file not found.")
    def download_53week(self):
        self.log("Downloading CPKC 53-week")
        url = self.discover_53week_url()
        self.log(f"Found: {url}")
        resp = http_get(url)
        return save_bytes(resp.content, f"CPKC_53_Week_{datestamp()}.xlsx")
    def discover_rtm_url(self):
        today = dt.date.today()
        for delta in range(14):
            d = today - dt.timedelta(days=delta)
            url = f"{self.CDN_BASE}/{d.strftime('%Y/%m/%d')}/CPKC-Weekly-RTMs-and-Carloads-{d.year}.xlsx"
            if http_head_ok(url): return url
        raise FileNotFoundError("CPKC RTM file not found.")
    def download_rtm(self):
        self.log("Downloading CPKC RTM")
        url = self.discover_rtm_url()
        self.log(f"Found: {url}")
        resp = http_get(url)
        return save_bytes(resp.content, f"CPKC_Weekly_RTM_{datestamp()}.xlsx")

# === CSX ===
class CSXDownloader(RailDownloader):
    METRICS_PAGE = "https://investors.csx.com/metrics/default.aspx"
    CDN_BASE = "https://s2.q4cdn.com/859568992/files/doc_downloads"
    def _iso_week_year(self, date_obj): return date_obj.isocalendar()[0], date_obj.isocalendar()[1]
    def _candidate_filenames(self, year, week):
        return [
            f"Historical_Data_Week_{week}_{year}.xlsx",
            f"Combined-Intermodal-and-Carload-TPC-Week-1-{year}-Week-{week}-{year}.xlsx",
        ]
    def discover_url(self):
        today = dt.date.today()
        year, week = self._iso_week_year(today - dt.timedelta(days=today.weekday() + 2)) 
        for delta in range(10):
            d = today - dt.timedelta(days=delta)
            folder = d.strftime("%Y/%m/%d")
            for fname in self._candidate_filenames(year, week):
                url = f"{self.CDN_BASE}/{folder}/{fname}"
                if http_head_ok(url): return url
        self.log("CDN guess failed. Scraping metrics page.")
        r = http_get(self.METRICS_PAGE)
        soup = BeautifulSoup(r.text, "html.parser")
        links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".xlsx")]
        for u in links:
            if u.startswith("//"): u = "https:" + u
            elif u.startswith("/"): u = "https://investors.csx.com" + u
            if http_head_ok(u): return u
        raise FileNotFoundError("CSX Excel not found.")
    def download(self):
        self.log("Downloading CSX")
        url = self.discover_url()
        self.log(f"Found: {url}")
        resp = http_get(url)
        # Standardized CSX Filename
        fname = f"CSX_Weekly_Metrics_{datestamp()}.xlsx" 
        return save_bytes(resp.content, fname)

# === UP (Union Pacific) ===
class UPDownloader(RailDownloader):
    # Main metrics page used to find the latest file link
    METRICS_PAGE = "https://investors.unionpacific.com/performance/performance-data/default.aspx"
    
    def download(self):
        self.log("Scraping Weekly UP Metrics")
        r = http_get(self.METRICS_PAGE)
        soup = BeautifulSoup(r.text, "html.parser")

        # Find all .xlsx links, common links are under 'static-files'
        links = [
            a["href"] 
            for a in soup.find_all("a", href=True) 
            if a["href"].endswith(".xlsx") and "static-files" in a["href"]
        ]
        
        if not links:
            raise FileNotFoundError("No UP .xlsx link found.")
        
        # Use the most recently posted link (often the last one)
        url = links[-1] 
        # UP links are usually absolute, but check for safety
        if url.startswith("/"): url = "https://investors.unionpacific.com" + url

        self.log(f"Found: {url}")
        
        # Note: The retry logic is implicitly handled by the increased global TIMEOUT
        resp = http_get(url) 
        
        fname = f"UP_Weekly_Metrics_{datestamp()}.xlsx" 
        return save_bytes(resp.content, fname)

# === MAIN EXECUTION ===
def run_tasks(args):
    
    # 1. Validation for DOWNLOAD_FOLDER writability
    if not os.path.exists(DOWNLOAD_FOLDER) or not os.access(DOWNLOAD_FOLDER, os.W_OK):
        logging.error(f"❌ Download folder '{DOWNLOAD_FOLDER}' is not accessible or writable. Aborting.")
        return

    tasks = []
    # Collect all tasks based on CLI arguments
    if args.all or args.stb: tasks.append(STBDownloader("STB").download)
    if args.all or args.cn:
        cn = CNDownloader("CN")
        tasks.extend([cn.download_perf, cn.download_rtm])
    if args.all or args.cpkc:
        cp = CPKCDownloader("CPKC")
        tasks.extend([cp.download_53week, cp.download_rtm])
    if args.all or args.csx: tasks.append(CSXDownloader("CSX").download)
    if args.all or args.up: tasks.append(UPDownloader("UP").download) # ADDED UP DOWNLOADER

    fetched = []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(fn): fn.__name__ for fn in tasks} 
        for future in futures:
            try:
                result = future.result()
                if isinstance(result, list): fetched.extend(result)
                else: fetched.append(result)
            
            # Enhanced HTTP Error Logging
            except Exception as e:
                error_msg = f"❌ {futures[future]} failed: {e}"
                if isinstance(e, requests.HTTPError) and e.response is not None:
                     error_msg += f" (HTTP {e.response.status_code})"
                logging.error(error_msg) 

    # Final Summary Logging
    logging.info("\n--------------------------------")
    if fetched:
        logging.info(f"✅ SCRAPING COMPLETE: Successfully downloaded {len(fetched)} files.")
        for f in fetched:
            logging.info(f" • {f}")
    else:
        logging.error("❌ SCRAPING COMPLETE: No files downloaded.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Rail Metrics Scraper. Downloads performance spreadsheets from STB, CN, CPKC, CSX, and UP. "
                    "Set the RAIL_METRICS_DIR environment variable to specify the download folder (defaults to current directory)."
    )
    parser.add_argument("--all", action="store_true", help="Download all reports (STB, CN, CPKC, CSX, UP)")
    parser.add_argument("--stb", action="store_true", help="Download STB EP724")
    parser.add_argument("--cn", action="store_true", help="Download CN Performance and RTM")
    parser.add_argument("--cpkc", action="store_true", help="Download CPKC 53-week and RTM")
    parser.add_argument("--csx", action="store_true", help="Download CSX report")
    parser.add_argument("--up", action="store_true", help="Download Union Pacific report") # ADDED UP ARGUMENT
    args = parser.parse_args()
    
    if not (args.stb or args.cn or args.cpkc or args.csx or args.up or args.all):
        logging.error("❌ No rail source selected. Use --all or one or more source flags (e.g., --stb --up).")
        parser.print_help()
    else:
        logging.info(f"📂 Download folder: {DOWNLOAD_FOLDER}")
        run_tasks(args)