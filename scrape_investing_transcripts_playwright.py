import argparse, time, re, sys
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE_URL = "https://www.investing.com/news/transcripts"
SEP = "===FREIGHT PULSE NEWS BREAK==="

# This selector is no longer actively awaited, but kept for clarity in the links function.
ARTICLE_LIST_SELECTOR = 'div[data-test="news-container"]'

# Flexible "Published ..." capture (handles "Nov 4, 2025 09:36AM ET" & "11/04/2025, 09:36 AM")
PUBLISH_RE = re.compile(r"Published\s+([A-Za-z0-9:,\s/]+)(?:\s*(?:ET|UTC|GMT))?\b", re.I)
KEY_TAKEAWAYS_RE = re.compile(r"\bkey\s*take\s*aways?\b[:\s]*", re.I)
STOP_GUARDS = [
    re.compile(r"\brelated articles?\b", re.I),
    re.compile(r"\bread next\b", re.I),
    re.compile(r"\bcomments?\b", re.I),
    re.compile(r"\bsponsored\b", re.I),
    re.compile(r"\bmost (?:read|popular)\b", re.I),
    re.compile(r"\blatest\b", re.I),
    re.compile(r"\bdisclaimer\b", re.I),
    re.compile(r"\bprivacy\b", re.I),
    re.compile(r"\bshare this article\b", re.I),
]

def parse_publish(text: str):
    m = PUBLISH_RE.search(text)
    if not m:
        return None, None
    raw = m.group(1).strip().strip(",")
    try:
        dt = dtparser.parse(raw, fuzzy=True)
        return raw, dt.isoformat()
    except Exception:
        return raw, None

def extract_key_takeaways_to_end(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    article = soup.find("article") or soup

    # Find heading-like "Key Takeaways"
    start = None
    for tag in article.find_all(["h1","h2","h3","h4","h5","h6","strong","b"]):
        if KEY_TAKEAWAYS_RE.search(tag.get_text(" ", strip=True) or ""):
            start = tag
            break
    if not start:
        for p in article.find_all("p"):
            t = p.get_text(" ", strip=True)
            if t and len(t) <= 120 and KEY_TAKEAWAYS_RE.search(t):
                start = p
                break
    if not start:
        return None

    chunks = []
    for el in start.next_elements:
        if el is article:
            break
        if hasattr(el, "name") and el.name in ("footer","nav","aside"):
            break
        if hasattr(el, "get"):
            klass = " ".join(el.get("class", [])).lower() if el.get("class") else ""
            eid = (el.get("id") or "").lower()
            if any(k in klass for k in ("comments","related","most-read","share","social","sponsored","tags")):
                break
            if any(k in eid for k in ("comments","related","most","share","sponsored","tags")):
                break
        if hasattr(el, "get_text"):
            t = el.get_text(" ", strip=True)
            if t:
                if any(r.search(t) for r in STOP_GUARDS):
                    break
                if getattr(el, "name","") in ("p","li"):
                    if not chunks or chunks[-1] != t:
                        chunks.append(t)
    return "\n".join(chunks) if chunks else None

def same_day(dt_iso, target_date):
    return datetime.fromisoformat(dt_iso).date() == target_date

def older_than(dt_iso, target_date):
    return datetime.fromisoformat(dt_iso).date() < target_date

def get_listing_links(page) -> list[str]:
    """
    Collects article links using a precise selector for transcript pages.
    Guarantees scroll to load lazy content before selection.
    """
    # Pre-scroll down to trigger lazy loading of pagination/links
    for frac in (0.25, 0.5, 0.9, 1.0):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight*%s)" % frac)
        time.sleep(0.6)

    # Use a specific XPath to filter for only high-confidence transcript links
    xpath_selector = "//a[contains(@href, '/news/transcripts/') and (.//h2 or .//h3)]"
    
    urls = []
    # Use Playwright's locator method which is efficient
    locator = page.locator(xpath_selector)
    count = locator.count()
    
    for i in range(min(count, 2000)):
        try:
            href = locator.nth(i).get_attribute("href") or ""
            if href:
                # Ensure absolute URL
                if href.startswith("/"):
                    href = urljoin(BASE_URL, href)
                urls.append(href)
        except Exception:
            continue

    # de-dup while preserving order
    seen, out = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def click_next(page) -> bool:
    """
    Attempts to click the 'Next' pagination link.
    Relies on the <a> tag containing a <span> with the text "Next" and NOT being disabled.
    """
    # XPATH for the visible 'Next' button that is NOT disabled (check for cursor-not-allowed class)
    next_link_xpath = "//a[.//span[normalize-space(translate(text(),'NEXT','next'))='next'] and not(contains(@class, 'cursor-not-allowed'))]"
    
    el = page.locator(next_link_xpath)
    if el.count():
        try:
            el.first.scroll_into_view_if_needed()
            el.first.click(timeout=5000)
            return True
        except PWTimeout:
            pass
    return False

def parse_article(page, url: str, delay: float):
    page.goto(url, wait_until="domcontentloaded")
    time.sleep(1.0)
    # try generic cookie accept (best effort)
    for text in ["Accept", "AGREE", "I Accept"]:
        btn = page.get_by_role("button", name=re.compile(text, re.I))
        if btn.count():
            try:
                # Use Playwright's native clicking for stability
                btn.first.click(timeout=500)
                time.sleep(0.5)
                break
            except Exception:
                pass
    
    # Wait for the main article content (a common parent)
    try:
        page.wait_for_selector('div.news-analysis-v2_content__z0iLP', timeout=10000)
    except PWTimeout:
        pass

    html = page.content()
    soup = BeautifulSoup(html, "html.parser")
    title = ""
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        title = h1.get_text(strip=True)
    else:
        h2 = soup.find("h2")
        title = h2.get_text(strip=True) if h2 else ""
    page_text = soup.get_text(" ", strip=True)
    published_str, published_iso = parse_publish(page_text)

    content = extract_key_takeaways_to_end(html)
    if not content:
        article = soup.find("article") or soup
        parts = []
        for node in article.find_all(["p","li"]):
            t = node.get_text(" ", strip=True)
            if t:
                parts.append(t)
        content = "\n".join(parts)
    time.sleep(delay)
    return {
        "url": url,
        "title": title,
        "published_str": published_str,
        "published_iso": published_iso,
        "content": content,
    }

def write_txt(results, out_path):
    with open(out_path, "w", encoding="utf-8") as f:
        first = True
        for r in results:
            if not first:
                f.write("\n" + SEP + "\n\n")
            first = False
            title = r.get("title") or ""
            pub = r.get("published_str") or ""
            url = r.get("url") or ""
            content = r.get("content") or ""

            if title: f.write(title.strip() + "\n")
            if pub:   f.write(f"Published: {pub}\n")
            if url:   f.write(f"URL: {url}\n")
            if title or pub or url: f.write("\n")
            f.write((content or "").strip() + "\n")

def scrape_playwright(target_date_str: str, out_path: str, delay: float = 5.0, max_pages: int = 10, headless: bool = True):
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    results, got_any_for_target = [], False

    with sync_playwright() as pw:
        # Use chromium which is generally good in cloud/headless environments
        browser = pw.chromium.launch(headless=headless, args=["--lang=en-US"])
        context = browser.new_context(locale="en-US", viewport={"width":1280,"height":1600})
        page = context.new_page()
        
        # --- FIX 1: Maximize Navigation Stability Timeout (Overall) ---
        page.set_default_navigation_timeout(120000) # Set overall navigation timeout to 120s
        
        # --- FIX 2: Critical change from "networkidle" to "domcontentloaded" to prevent infinite security checks ---
        page.goto(BASE_URL, wait_until="domcontentloaded") 
        time.sleep(delay)

        # --- FIX 3: Attempt to dismiss broad security/overlay banners ---
        try:
            # Look for common blocking elements and press escape or click agree
            page.locator('button:has-text("Continue"), button:has-text("I Agree")').first.click(timeout=5000)
            page.locator('div[aria-label*="Privacy"]').first.press("Escape", timeout=500)
            time.sleep(2)
        except Exception:
            pass
        
        # --- FINAL CRITICAL FIX: Replace conditional selector wait with a fixed wait ---
        # This addresses the persistent 0-byte output by forcing the script 
        # to wait a reasonable time before attempting to scrape links.
        time.sleep(15.0) 
        # print("Final fixed wait completed. Attempting to gather links...")
        
        page_idx = 0
        while page_idx < max_pages:
            page_idx += 1
            links = get_listing_links(page)
            
            # Re-check for links after scrolling, vital for lazy loading sites
            if not links and page_idx == 1:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(3.0) # Longer wait on initial scroll retry
                links = get_listing_links(page)
            
            if not links:
                break

            for url in links:
                art = parse_article(page, url, delay)
                
                if art["published_iso"]:
                    if same_day(art["published_iso"], target_date):
                        got_any_for_target = True
                        results.append(art)
                    elif older_than(art["published_iso"], target_date):
                        if got_any_for_target:
                            write_txt(results, out_path)
                            context.close(); browser.close()
                            return # Stop and exit function immediately
                else:
                    # keep unknowns for visibility / debugging
                    results.append(art)

            if not click_next(page):
                break # Break if no next button found
            time.sleep(delay)

        write_txt(results, out_path)
        context.close(); browser.close()

def main():
    ap = argparse.ArgumentParser(description="Scrape Investing.com Transcripts via Playwright (Key Takeaways → end).")
    ap.add_argument("--date", required=True, help="Target date in YYYY-MM-DD (site local time).")
    ap.add_argument("--out", default="transcripts.txt", help="Output .txt file")
    ap.add_argument("--delay", type=float, default=5.0, help="Seconds between actions (default 5.0)")
    ap.add_argument("--max-pages", type=int, default=10, help="Max listing pages (default 10)")
    ap.add_argument("--no-headless", action="store_true", help="Show the browser")
    args = ap.parse_args()
    
    # We remove the manual playwright install check here, as GitHub Actions handles it.

    scrape_playwright(args.date, args.out, args.delay, args.max_pages, headless=not args.no_headless)
    print(f"Wrote transcripts to {args.out}")

if __name__ == "__main__":
    main()
