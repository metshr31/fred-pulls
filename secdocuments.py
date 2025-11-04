import datetime
from datetime import timezone, timedelta
import os
import re
import csv
import textwrap
import hashlib
import sys

# IMPORTANT: the import is "edgar", not "edgartools"
try:
    from edgar import get_current_filings
except Exception as e:
    print("ERROR: Could not import 'edgar'. "
          "Make sure requirements.txt includes 'edgartools' and it installed successfully.\n"
          f"Underlying import error: {e}", file=sys.stderr)
    raise

###############################################################################
# CONFIGURATION
###############################################################################

# Forms to monitor. Core US forms + foreign equivalents + "skeptical" capital raise docs
FORMS_WE_CARE_ABOUT = {
    # Core US / domestic ops & strategy
    "8-K", "10-Q", "10-K", "S-4", "S-4/A",
    # High-value foreign filers (CN, CPKC, Maersk, etc.)
    "6-K", "20-F",
    # Skeptical but allowed (only surface if they talk freight explicitly)
    "424B", "424B1", "424B2", "424B3", "424B4", "424B5",
    "FWP",
    "S-1", "S-1/A",
    "S-3", "S-3/A",
}

# Direct freight / logistics / network language
DIRECT_KEYWORDS = {
    r"\bintermodal\b": 3,
    r"\brail\b": 2,
    r"\brailroad\b": 3,
    r"\bdrayage\b": 4,
    r"\bchassis\b": 4,
    r"\bcontainer(s)?\b": 2,
    r"\bport(s)?\b": 3,
    r"\bterminal(s)?\b": 2,
    r"\btransload(ing)?\b": 3,
    r"\bwarehouse(s|ing)?\b": 2,
    r"\bdistribution center\b": 2,
    r"\blogistics\b": 2,
    r"\bsupply[- ]chain\b": 2,
    r"\bfreight\b": 3,
    r"\blinehaul\b": 3,
    r"\btrucking\b": 3,
    r"\btruckload\b": 4,
    r"\bTL\b": 2,
    r"\bLTL\b": 4,
    r"\bless[- ]than[- ]truckload\b": 4,
    r"\bparcel\b": 3,
    r"\bair freight\b": 3,
    r"\bcross[- ]border\b": 3,
    r"\bMexico\b": 2,
    r"\bUS[- ]Mexico\b": 3,
    r"\bnearshor(e|ing)\b": 3,
    r"\bcorridor(s)?\b": 2,
    r"\blane(s)?\b": 2,
    r"\bbackhaul\b": 3,
    r"\bdwell\b": 3,
    r"\bvelocity\b": 2,
    r"\bturn time\b": 2,
    r"\bchassis pool\b": 4,
    r"\binterchange\b": 2,
}

# Indirect freight demand / cost / capacity signals
CONTEXT_KEYWORDS = {
    # Industrial / project cargo / flatbed / bulk rail
    r"\bmanufactur(ing|er|ed)\b": 2,
    r"\bplant\b": 2,
    r"\bfactory\b": 2,
    r"\bproduction\b": 2,
    r"\bbacklog\b": 2,
    r"\bcapital project\b": 2,
    r"\bCapEx\b": 2,
    r"\binfrastructure\b": 1,
    r"\bconstruction\b": 2,
    r"\bsteel\b": 2,
    r"\bstructural steel\b": 3,
    r"\bfabricated metal\b": 2,
    r"\bpipe\b": 1,
    r"\bcoil\b": 1,
    r"\bplate\b": 1,
    r"\boilfield\b": 2,
    r"\bdrill(ing)? rig\b": 2,
    r"\bfrac sand\b": 3,
    r"\bchemical plant\b": 3,
    r"\brefinery turnaround\b": 3,
    r"\boil ?&? gas\b": 2,

    # Retail / replenishment / DC network / parcel
    r"\binventor(y|ies)\b": 2,
    r"\bstockout(s)?\b": 2,
    r"\brestock(ing)?\b": 2,
    r"\bSKU (rationalization|reduction)\b": 2,
    r"\bSKU rationalization\b": 2,
    r"\bSKU reduction\b": 2,
    r"\bfulfillment\b": 2,
    r"\bDC network\b": 3,
    r"\bdistribution network\b": 3,
    r"\blast[- ]mile\b": 2,
    r"\be[- ]commerce\b": 2,
    r"\bcorrugated box\b": 3,
    r"\bpackaging resin\b": 2,
    r"\bplastics resin\b": 2,
    r"\bholiday build\b": 2,

    # Food / reefer
    r"\bprotein processing\b": 3,
    r"\bmeatpacking\b": 3,
    r"\bdairy processing\b": 2,
    r"\bcold storage\b": 3,
    r"\btemperature[- ]controlled\b": 3,
    r"\bperishable\b": 2,

    # Transportation cost / service stress
    r"\bfuel surcharge\b": 4,
    r"\bdiesel\b": 2,
    r"\blinehaul cost\b": 3,
    r"\btransportation cost(s)?\b": 2,
    r"\bfreight cost(s)?\b": 3,
    r"\bcarrier rates\b": 2,
    r"\bcapacity constraint(s)?\b": 3,
    r"\bdriver shortage\b": 4,
    r"\blabor disruption\b": 3,
    r"\bstrike\b": 2,
    r"\bwork stoppage\b": 3,
    r"\bshutdown\b": 2,
    r"\bservice interruption\b": 2,

    # Border / nearshoring / Laredo corridor
    r"\btariff(s)?\b": 3,
    r"\bcustoms\b": 2,
    r"\bborder\b": 2,
    r"\brelocation of production\b": 4,
    r"\bshift(ed|ing)? production\b": 3,
    r"\bnearshore(d|ing)?\b": 4,
    r"\bMonterrey\b": 3,
    r"\bJu[aá]rez\b": 3,
    r"\bLaredo\b": 3,
}

# Paired concepts that say "this is a freight capacity / flow story"
PAIR_RULES = [
    # Industrial output + transport stress
    (r"\bmanufactur(ing|er|ed)\b", r"\btransportation cost(s)?\b", 3),
    (r"\bMexico\b", r"\bcapacity constraint(s)?\b", 3),
    (r"\bnearshor(e|ing)\b", r"\bcross[- ]border\b", 3),
    (r"\bMexico\b", r"\bdriver shortage\b", 3),

    # Inventory + DC flow
    (r"\binventor(y|ies)\b", r"\bdistribution center\b", 2),
    (r"\binventor(y|ies)\b", r"\bwarehouse(s|ing)?\b", 2),
    (r"\bfulfillment\b", r"\bDC network\b", 3),

    # Port/terminal congestion + trade
    (r"\bport congestion\b", r"\bimport(s|ed|ing)\b", 3),
    (r"\bterminal congestion\b", r"\bexport(s|ed|ing)?\b", 3),

    # Truck pricing pressure
    (r"\bfuel surcharge\b", r"\blinehaul\b", 4),
    (r"\bfuel surcharge\b", r"\blinehaul cost\b", 4),

    # Labor / node shutdown
    (r"\bwork stoppage\b", r"\bterminal(s)?\b", 3),
    (r"\bstrike\b", r"\bwarehouse(s|ing)?\b", 2),
    (r"\bstrike\b", r"\bplant\b", 2),
]

# High-priority companies whose disclosures ALWAYS matter to freight
CORE_FREIGHT_WATCHLIST = [
    "Union Pacific",
    "Norfolk Southern",
    "CSX",
    "Canadian Pacific Kansas City",
    "Canadian Pacific Kansas City Limited",
    "BNSF",
    "J.B. Hunt",
    "J B Hunt",
    "Schneider National",
    "Hub Group",
    "Knight-Swift",
    "Knight Swift",
    "Werner Enterprises",
    "Old Dominion Freight Line",
    "Saia",
    "XPO",
    "GXO",
    "FedEx",
    "United Parcel Service",
    "UPS",
    "Ryder System",
    "ArcBest",
    "TFI International",
    "Landstar System",
    "Matson",
    "Kirby Corporation",
    "Kirby Corp",
    "C.H. Robinson",
    "CH Robinson",
    "C H Robinson",
]

# Mode "lenses" so we can say who should care
MODE_TAGS = [
    ("Rail / Intermodal", [
        r"\brail\b", r"\brailroad\b", r"\bintermodal\b", r"\bchassis\b",
        r"\bcontainer(s)?\b", r"\bport(s)?\b", r"\bterminal(s)?\b",
        r"\bdwell\b", r"\bvelocity\b", r"\bcross[- ]border\b",
        r"\bMexico\b", r"\bnearshor(e|ing)\b"
    ]),
    ("Truckload / Dry Van / LTL", [
        r"\btrucking\b", r"\btruckload\b", r"\bTL\b", r"\bLTL\b",
        r"\bless[- ]than[- ]truckload\b", r"\blinehaul\b",
        r"\bfuel surcharge\b", r"\bdriver shortage\b",
        r"\bdistribution center\b", r"\bDC network\b",
        r"\bSKU (rationalization|reduction)\b",
        r"\bcorrugated box\b", r"\brestock(ing)?\b",
        r"\binventor(y|ies)\b", r"\bholiday build\b"
    ]),
    ("Reefer / Food Cargo", [
        r"\bcold storage\b", r"\btemperature[- ]controlled\b",
        r"\bprotein processing\b", r"\bmeatpacking\b",
        r"\bdairy processing\b", r"\bperishable\b"
    ]),
    ("Industrial / Flatbed / Project Cargo", [
        r"\bsteel\b", r"\bstructural steel\b", r"\bfabricated metal\b",
        r"\bpipe\b", r"\bfrac sand\b", r"\bdrill(ing)? rig\b",
        r"\bchemical plant\b", r"\brefinery turnaround\b",
        r"\bconstruction\b", r"\bcapital project\b", r"\bCapEx\b",
        r"\bmanufactur(ing|er|ed)\b", r"\bfactory\b", r"\bplant\b",
        r"\bproduction\b", r"\boilfield\b"
    ]),
    ("Parcel / Air / E-commerce", [
        r"\bparcel\b", r"\bair freight\b", r"\be[- ]commerce\b",
        r"\bfulfillment\b", r"\blast[- ]mile\b"
    ]),
]

# Threshold for "include in summary bullets"
SCORE_THRESHOLD = 4

# Threshold for "also dump the full filing text to its own file"
FULLTEXT_THRESHOLD = 7


###############################################################################
# SCORING HELPERS
###############################################################################

def weighted_keyword_score(text: str, keyword_dict: dict) -> int:
    if not text:
        return 0
    total = 0
    for pattern, weight in keyword_dict.items():
        if re.search(pattern, text, flags=re.IGNORECASE):
            total += weight
    return total

def pair_score(text: str, pairs) -> int:
    if not text:
        return 0
    bonus = 0
    for pat_a, pat_b, weight in pairs:
        if re.search(pat_a, text, flags=re.IGNORECASE) and re.search(pat_b, text, flags=re.IGNORECASE):
            bonus += weight
    return bonus

def is_core_freight_company(name: str) -> bool:
    if not name:
        return False
    lower = name.lower()
    for watch in CORE_FREIGHT_WATCHLIST:
        if watch.lower() in lower:
            return True
    return False

def guess_mode_tags(text: str):
    tags = []
    if not text:
        return tags
    for label, patterns in MODE_TAGS:
        for p in patterns:
            if re.search(p, text, flags=re.IGNORECASE):
                tags.append(label)
                break
    # de-dupe while preserving order
    final = []
    for t in tags:
        if t not in final:
            final.append(t)
    return final

def form_signal_adjustment(form_type: str) -> int:
    """
    Bump or penalize certain forms:
    - 8-K / 6-K: often real-time operational/guidance events -> +1
    - 10-Q / 10-K / 20-F: baseline, rich in logistics content -> 0
    - S-4: neutral
    - 424B*, FWP, S-1/S-3: very noisy, only surface if keywords are strong -> -1
    """
    if form_type in ("8-K", "6-K"):
        return 1
    if form_type in ("10-Q", "10-K", "20-F"):
        return 0
    if form_type in ("S-4", "S-4/A"):
        return 0
    if (form_type.startswith("424B")
        or form_type in ("FWP", "S-1", "S-1/A", "S-3", "S-3/A")):
        return -1
    return 0

def find_relevant_snippet(text: str, patterns: list[str], window: int = 220) -> str:
    """
    Extract 1-2 sentence 'evidence' around the first interesting match.
    This is what you'll quote in Freight Pulse.
    """
    if not text:
        return ""
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            start = max(m.start() - window//2, 0)
            end = min(m.end() + window//2, len(text))
            snippet = text[start:end]

            # normalize whitespace
            snippet = re.sub(r"\s+", " ", snippet).strip()

            # try to end cleanly at nearest period
            period_pos = snippet.find(". ")
            if period_pos != -1 and period_pos < len(snippet) - 20:
                snippet = snippet[:period_pos+1]

            snippet = snippet.strip().strip('"').strip("'")
            return snippet
    return ""

def safe_slug(s: str) -> str:
    """
    Make a filename-friendly slug.
    """
    if not s:
        return "na"
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")[:80]

def tiny_hash(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:10]

def summarize_for_newsletter(company, ticker, form, filed_at, url, rationale, tags, snippet, fulltext_path_if_any):
    """
    Build the bullet block that goes into freight_pulse_sec_raw.txt.
    """
    if isinstance(filed_at, datetime.datetime):
        ts_str = filed_at.strftime("%Y-%m-%d %H:%M UTC")
    else:
        ts_str = str(filed_at)

    tag_str = ", ".join(tags) if tags else "General Freight / Supply Chain Impact"

    lines = []
    lines.append(f"• {company} ({ticker if ticker else 'no ticker'}) filed a {form} on {ts_str}.")
    lines.append(f"  Why it matters: {rationale}")
    lines.append(f"  Mode lens: {tag_str}")
    if snippet:
        lines.append(f'  Excerpt: "{snippet}"')
    lines.append(f"  Source: {url}")

    if fulltext_path_if_any:
        lines.append(f"  Full text saved: {fulltext_path_if_any}")

    return "\n".join(lines) + "\n"


###############################################################################
# MAIN
###############################################################################

def main():
    now = datetime.datetime.now(timezone.utc)
    # get_current_filings() already represents the most recent batch (≈ last 24h window)
    filings = get_current_filings()

    hits = []
    bullet_blocks = []

    # Ensure output dirs exist
    os.makedirs("output", exist_ok=True)
    os.makedirs("output/full_text", exist_ok=True)

    for f in filings:
        # Expected attributes from edgar filings objects:
        #   f.form_type, f.company_name, f.ticker, f.filed, f.primary_document_url, f.text()
        form = getattr(f, "form_type", "") or ""
        if form not in FORMS_WE_CARE_ABOUT:
            continue

        company_name = getattr(f, "company_name", "") or ""
        ticker = getattr(f, "ticker", "") or ""
        filed_at = getattr(f, "filed", "")
        url = getattr(f, "primary_document_url", "") or ""

        # Pull full filing text
        try:
            body_text = f.text()
        except Exception:
            body_text = ""

        # --- scoring ---
        direct_pts  = weighted_keyword_score(company_name, DIRECT_KEYWORDS) \
                    + weighted_keyword_score(body_text, DIRECT_KEYWORDS)
        context_pts = weighted_keyword_score(body_text, CONTEXT_KEYWORDS)
        combo_pts   = pair_score(body_text, PAIR_RULES)
        boost_pts   = 5 if is_core_freight_company(company_name) else 0
        form_adj    = form_signal_adjustment(form)

        score = direct_pts + context_pts + combo_pts + boost_pts + form_adj

        # --- rationale text for "Why it matters" ---
        rationale_bits = []
        if boost_pts:
            rationale_bits.append("core transport operator")
        if direct_pts:
            rationale_bits.append("direct freight/transport language (rail, trucking, port, chassis, etc.)")
        if context_pts:
            rationale_bits.append("macro driver (inventory, industrial output, steel/chemicals, cold storage, Mexico)")
        if combo_pts:
            rationale_bits.append("paired signal (production + transport stress, border + capacity, etc.)")
        rationale = "; ".join(rationale_bits) if rationale_bits else "logistics-adjacent operational signal"

        # --- mode tagging ---
        modes = guess_mode_tags(body_text)

        # --- snippet extraction ---
        snippet_patterns = list(DIRECT_KEYWORDS.keys()) + list(CONTEXT_KEYWORDS.keys())
        for a, b, _w in PAIR_RULES:
            snippet_patterns.append(a)
            snippet_patterns.append(b)
        snippet_patterns = list(dict.fromkeys(snippet_patterns))
        snippet = find_relevant_snippet(body_text, snippet_patterns)

        # --- thresholds ---
        should_surface = score >= SCORE_THRESHOLD
        if not should_surface:
            continue

        should_dump_fulltext = score >= FULLTEXT_THRESHOLD

        fulltext_path = None
        if should_dump_fulltext:
            base_pieces = [
                now.date().isoformat(),
                form,
                ticker if ticker else safe_slug(company_name)[:20],
            ]
            base_name = "_".join(safe_slug(p) for p in base_pieces if p)
            base_name = base_name + "_" + tiny_hash(url or company_name or "") + ".txt"

            fulltext_path = os.path.join("output", "full_text", base_name)

            with open(fulltext_path, "w", encoding="utf-8") as ffull:
                ffull.write(f"Company: {company_name}\n")
                ffull.write(f"Ticker: {ticker}\n")
                ffull.write(f"Form: {form}\n")
                ffull.write(f"Filed At: {filed_at}\n")
                ffull.write(f"URL: {url}\n")
                ffull.write(f"Score: {score}\n")
                ffull.write("\n=== BEGIN FILING TEXT ===\n\n")
                ffull.write(body_text)

        hits.append({
            "date_run": now.date().isoformat(),
            "company": company_name.strip(),
            "ticker": ticker.strip(),
            "form": form,
            "filed_at": filed_at,
            "url": url.strip(),
            "rationale": rationale,
            "tags": modes,
            "score": score,
            "snippet": snippet,
            "fulltext_file": fulltext_path if should_dump_fulltext else "",
        })

    # Sort for readability
    form_rank = {
        "8-K": 1, "6-K": 1,
        "10-Q": 2, "10-K": 2, "20-F": 2,
        "S-4": 3, "S-4/A": 3,
        "424B": 4, "424B1": 4, "424B2": 4, "424B3": 4, "424B4": 4, "424B5": 4,
        "FWP": 4, "S-1": 4, "S-1/A": 4, "S-3": 4, "S-3/A": 4,
    }

    def sort_key(item):
        if isinstance(item["filed_at"], datetime.datetime):
            filed_str = item["filed_at"].strftime("%Y-%m-%d %H:%M:%S")
        else:
            filed_str = str(item["filed_at"])
        return (
            -item["score"],
            form_rank.get(item["form"], 99),
            filed_str[::-1],
        )

    hits.sort(key=sort_key)

    # Human-readable report
    bullet_blocks.append("🔎 SEC Filings With Freight / Supply Chain Impact (recent feed)\n")

    if not hits:
        bullet_blocks.append(
            "• No new 8-K / 6-K / 10-Q / 10-K / 20-F / S-4 / capital-market filings that materially touch freight demand, capacity, cost, labor, nearshoring, industrial build, cold chain, or inventory positioning.\n"
        )
    else:
        for h in hits:
            bullet_blocks.append(
                summarize_for_newsletter(
                    company=h["company"],
                    ticker=h["ticker"],
                    form=h["form"],
                    filed_at=h["filed_at"],
                    url=h["url"],
                    rationale=h["rationale"],
                    tags=h["tags"],
                    snippet=h["snippet"],
                    fulltext_path_if_any=h["fulltext_file"],
                )
            )

    bullet_blocks.append(
        f"[internal note: surfaced {len(hits)}; SCORE_THRESHOLD={SCORE_THRESHOLD}; FULLTEXT_THRESHOLD={FULLTEXT_THRESHOLD}]"
    )

    # Write outputs
    with open("output/freight_pulse_sec_raw.txt", "w", encoding="utf-8") as ftxt:
        ftxt.write("\n".join(bullet_blocks))

    csv_path = "output/freight_pulse_sec_full.csv"
    new_file = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as fcsv:
        writer = csv.writer(fcsv)
        if new_file:
            writer.writerow([
                "date_run",
                "company",
                "ticker",
                "form",
                "filed_at",
                "score",
                "rationale",
                "mode_tags",
                "snippet",
                "url",
                "fulltext_file",
            ])
        for h in hits:
            writer.writerow([
                h["date_run"],
                h["company"],
                h["ticker"],
                h["form"],
                h["filed_at"],
                h["score"],
                h["rationale"],
                "; ".join(h["tags"]),
                h["snippet"],
                h["url"],
                h["fulltext_file"],
            ])

    # Log to stdout for Actions
    print("\n".join(bullet_blocks))


if __name__ == "__main__":
    main()
