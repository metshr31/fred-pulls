import datetime
from datetime import timezone
import os
import re
import csv
import hashlib
import sys

# edgartools installs as "edgar"
try:
    from edgar import get_current_filings, set_identity
except Exception as e:
    print("ERROR: Could not import 'edgar'. "
          "Ensure requirements.txt includes 'edgartools' (import name is 'edgar').\n"
          f"Import error: {e}", file=sys.stderr)
    raise

# Apply identity from env (works locally and in GitHub Actions)
identity = os.getenv("EDGAR_IDENTITY") or os.getenv("EDGAR_USER_AGENT")
if identity:
    try:
        set_identity(identity)
    except Exception:
        pass

###############################################################################
# CONFIGURATION
###############################################################################

# Core (surface) forms to show in bullets if they pass the score bar
CORE_FORMS = {
    # Core US / domestic ops & strategy
    "8-K", "10-Q", "10-K", "S-4",
    # High-value foreign filers
    "6-K", "20-F",
    # Amended variants (often material)
    "8-K/A", "10-Q/A", "10-K/A", "6-K/A", "20-F/A", "S-4/A",
    # Capital-markets docs allowed if they truly talk freight (kept, but penalized below)
    "424B", "424B1", "424B2", "424B3", "424B4", "424B5",
    "FWP", "S-1", "S-1/A", "S-3", "S-3/A",
    # Optional: 425 (M&A comms) — can matter for networks (penalized below)
    "425",
}

# Direct freight / logistics / network language
DIRECT_KEYWORDS = {
    r"\bintermodal\b": 3, r"\brail\b": 2, r"\brailroad\b": 3, r"\bdrayage\b": 4,
    r"\bchassis\b": 4, r"\bcontainer(s)?\b": 2, r"\bport(s)?\b": 3, r"\bterminal(s)?\b": 2,
    r"\btransload(ing)?\b": 3, r"\bwarehouse(s|ing)?\b": 2, r"\bdistribution center\b": 2,
    r"\blogistics\b": 2, r"\bsupply[- ]chain\b": 2, r"\bfreight\b": 3, r"\blinehaul\b": 3,
    r"\btrucking\b": 3, r"\btruckload\b": 4, r"\bTL\b": 2, r"\bLTL\b": 4,
    r"\bless[- ]than[- ]truckload\b": 4, r"\bparcel\b": 3, r"\bair freight\b": 3,
    r"\bcross[- ]border\b": 3, r"\bMexico\b": 2, r"\bUS[- ]Mexico\b": 3,
    r"\bnearshor(e|ing)\b": 3, r"\bcorridor(s)?\b": 2, r"\blane(s)?\b": 2,
    r"\bbackhaul\b": 3, r"\bdwell\b": 3, r"\bvelocity\b": 2, r"\bturn time\b": 2,
    r"\bchassis pool\b": 4, r"\binterchange\b": 2,
}

# Indirect US-economy / sector signals (expanded)
CONTEXT_KEYWORDS = {
    # Industrial / manufacturing cycle
    r"\bindustrial production\b": 3, r"\bcapacity utilization\b": 2,
    r"\bPMI\b": 2, r"\bISM\b": 2, r"\bbacklog\b": 2, r"\bnew orders\b": 2,
    r"\bproduction\b": 2, r"\bfactory\b": 2, r"\bplant\b": 2,
    r"\bCapEx\b": 2, r"\bcapital project\b": 2, r"\bmaintenance turnaround\b": 2,

    # Construction / building (flatbed-heavy)
    r"\bconstruction spending\b": 2, r"\bhousing starts?\b": 2,
    r"\bbuilding permits?\b": 2, r"\bnonresidential\b": 1,
    r"\bcement\b": 2, r"\bconcrete\b": 2, r"\basphalt\b": 2,
    r"\blumber\b": 2, r"\bOSB\b": 2, r"\bgypsum\b": 2, r"\brebar\b": 2,

    # Metals / industrial inputs
    r"\bsteel\b": 2, r"\bstructural steel\b": 3, r"\bfabricated metal\b": 2,
    r"\bcoil steel\b": 3, r"\baluminum\b": 2, r"\bcopper\b": 2, r"\bnickel\b": 2, r"\bzinc\b": 2,

    # Energy / chemicals
    r"\boilfield\b": 2, r"\brefinery turnaround\b": 3, r"\bchemical plant\b": 3,
    r"\bammonia\b": 2, r"\bfertilizer\b": 2, r"\bpolyethylene\b": 3, r"\bpolypropylene\b": 3,
    r"\bresin prices?\b": 3, r"\bPVC\b": 2,

    # Retail / inventory / parcel
    r"\bretail sales?\b": 2, r"\bcomp sales?\b": 2, r"\binventor(y|ies)\b": 2,
    r"\bstockout(s)?\b": 2, r"\brestock(ing)?\b": 2, r"\bSKU (rationalization|reduction)\b": 2,
    r"\bfulfillment\b": 2, r"\bDC network\b": 3, r"\bdistribution network\b": 3,
    r"\bDC consolidation\b": 3, r"\bnetwork optimization\b": 2, r"\bmicro[- ]fulfillment\b": 2,
    r"\blast[- ]mile\b": 2, r"\be[- ]commerce\b": 2, r"\bcorrugated box\b": 3,
    r"\bpackaging resin\b": 2, r"\bplastics resin\b": 2, r"\bholiday build\b": 2,

    # Food chain / reefer
    r"\bprotein processing\b": 3, r"\bmeatpacking\b": 3, r"\bdairy processing\b": 2,
    r"\bcold storage\b": 3, r"\btemperature[- ]controlled\b": 3, r"\bperishable\b": 2,
    r"\bproduce season\b": 2, r"\bharvest\b": 2, r"\bgrain exports?\b": 2,
    r"\bsoy(beans)?\b": 2, r"\bcorn\b": 2, r"\bwheat\b": 2,

    # Transportation cost / service stress
    r"\bfuel surcharge\b": 4, r"\bdiesel\b": 2, r"\blinehaul cost\b": 3,
    r"\btransportation cost(s)?\b": 2, r"\bfreight cost(s)?\b": 3, r"\bcarrier rates\b": 2,
    r"\bcapacity constraint(s)?\b": 3, r"\bdriver shortage\b": 4,
    r"\blabor disruption\b": 3, r"\bstrike\b": 2, r"\bwork stoppage\b": 3,
    r"\bshutdown\b": 2, r"\bservice interruption\b": 2,

    # Ports / corridors (named nodes)
    r"\bSavannah\b": 3, r"\bCharleston\b": 3, r"\bLA[- ]?Long Beach\b": 3,
    r"\bPort of Los Angeles\b": 3, r"\bPort of Long Beach\b": 3, r"\bHouston\b": 2,
    r"\bLaredo\b": 3, r"\bNogales\b": 2, r"\bOtay Mesa\b": 2,

    # Border / nearshoring / trade
    r"\btariff(s)?\b": 3, r"\bcustoms\b": 2, r"\bborder\b": 2,
    r"\brelocation of production\b": 4, r"\bshift(ed|ing)? production\b": 3,
    r"\bnearshore(d|ing)?\b": 4, r"\bMonterrey\b": 3, r"\bJu[aá]rez\b": 3,
}

# Paired concepts that say "this is a freight capacity / flow story"
PAIR_RULES = [
    (r"\bmanufactur(ing|er|ed)\b", r"\btransportation cost(s)?\b", 3),
    (r"\bMexico\b", r"\bcapacity constraint(s)?\b", 3),
    (r"\bnearshor(e|ing)\b", r"\bcross[- ]border\b", 3),
    (r"\bMexico\b", r"\bdriver shortage\b", 3),
    (r"\binventor(y|ies)\b", r"\bdistribution center\b", 2),
    (r"\binventor(y|ies)\b", r"\bwarehouse(s|ing)?\b", 2),
    (r"\bfulfillment\b", r"\bDC network\b", 3),
    (r"\bport congestion\b", r"\bimport(s|ed|ing)\b", 3),
    (r"\bterminal congestion\b", r"\bexport(s|ed|ing)?\b", 3),
    (r"\bfuel surcharge\b", r"\blinehaul\b", 4),
    (r"\bfuel surcharge\b", r"\blinehaul cost\b", 4),
    (r"\bwork stoppage\b", r"\bterminal(s)?\b", 3),
    (r"\bstrike\b", r"\bwarehouse(s|ing)?\b", 2),
    (r"\bstrike\b", r"\bplant\b", 2),
]

# High-priority companies whose disclosures ALWAYS matter to freight
CORE_FREIGHT_WATCHLIST = [
    "Union Pacific","Norfolk Southern","CSX","Canadian Pacific Kansas City",
    "Canadian Pacific Kansas City Limited","BNSF","J.B. Hunt","J B Hunt",
    "Schneider National","Hub Group","Knight-Swift","Knight Swift","Werner Enterprises",
    "Old Dominion Freight Line","Saia","XPO","GXO","FedEx","United Parcel Service","UPS",
    "Ryder System","ArcBest","TFI International","Landstar System","Matson",
    "Kirby Corporation","Kirby Corp","C.H. Robinson","CH Robinson","C H Robinson",
]

# Mode lenses
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
        r"\bSKU (rationalization|reduction)\b", r"\bcorrugated box\b",
        r"\brestock(ing)?\b", r"\binventor(y|ies)\b", r"\bholiday build\b"
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

# Thresholds
SCORE_THRESHOLD = 2
FULLTEXT_THRESHOLD = 5

###############################################################################
# HELPERS
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
                tags.append(label); break
    # de-dupe, preserve order
    final = []
    for t in tags:
        if t not in final:
            final.append(t)
    return final

def form_signal_adjustment(form_type: str) -> int:
    # 8-K/6-K (+ amended): +1; 10-Q/10-K/20-F (+ amended): 0; S-4: 0; capital-raise & 425: -1
    if form_type in ("8-K", "6-K", "8-K/A", "6-K/A"):
        return 1
    if form_type in ("10-Q", "10-K", "20-F", "10-Q/A", "10-K/A", "20-F/A", "S-4", "S-4/A"):
        return 0
    if (form_type.startswith("424B")
        or form_type in ("FWP", "S-1", "S-1/A", "S-3", "S-3/A", "425")):
        return -1
    return 0

def find_relevant_snippet(text: str, patterns: list[str], window: int = 220) -> str:
    if not text:
        return ""
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            start = max(m.start() - window//2, 0)
            end = min(m.end() + window//2, len(text))
            snippet = re.sub(r"\s+", " ", text[start:end]).strip()
            # try to end cleanly at nearest period
            period_pos = snippet.find(". ")
            if period_pos != -1 and period_pos < len(snippet) - 20:
                snippet = snippet[:period_pos+1]
            return snippet.strip().strip('"').strip("'")
    return ""

def safe_slug(s: str) -> str:
    if not s: return "na"
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")[:80]

def tiny_hash(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:10]

def summarize_for_newsletter(company, ticker, form, filed_at, url, rationale, tags, snippet, fulltext_path_if_any):
    ts_str = str(filed_at) if filed_at else "unknown date"
    tag_str = ", ".join(tags) if tags else "General Freight / Supply Chain Impact"
    lines = []
    lines.append(f"• {company if company else '(unknown issuer)'} ({ticker if ticker else 'no ticker'}) filed a {form if form else '(unknown form)'} on {ts_str}.")
    lines.append(f"  Why it matters: {rationale}")
    lines.append(f"  Mode lens: {tag_str}")
    if snippet:
        lines.append(f'  Excerpt: "{snippet}"')
    lines.append(f"  Source: {url if url else '(no link)'}")
    if fulltext_path_if_any:
        lines.append(f"  Full text saved: {fulltext_path_if_any}")
    return "\n".join(lines) + "\n"

###############################################################################
# MAIN
###############################################################################

def main():
    now = datetime.datetime.now(timezone.utc)
    filings = get_current_filings()  # keep tight recency window

    hits = []
    candidates = []
    bullet_blocks = []

    os.makedirs("output", exist_ok=True)
    os.makedirs("output/full_text", exist_ok=True)

    # snippet patterns (build once)
    snippet_patterns = list(DIRECT_KEYWORDS.keys()) + list(CONTEXT_KEYWORDS.keys())
    for a, b, _w in PAIR_RULES:
        snippet_patterns.append(a); snippet_patterns.append(b)
    snippet_patterns = list(dict.fromkeys(snippet_patterns))

    total_seen = 0
    total_core_form = 0

    for f in filings:
        total_seen += 1

        # ✅ Correct attribute names from edgartools Filing API
        form = (getattr(f, "form", "") or "").strip()
        company_name = (getattr(f, "company", "") or "").strip()
        filed_at = (getattr(f, "filing_date", "") or "").strip()   # "YYYY-MM-DD"
        url = (getattr(f, "filing_url", None) or getattr(f, "url", "") or "").strip()

        # edgar's Filing object typically doesn't have ticker; leave blank (optional lookup is slower)
        ticker = ""

        try:
            body_text = f.text()
        except Exception:
            body_text = ""

        # --- scoring (for ALL filings, even off-form) ---
        direct_pts  = weighted_keyword_score(company_name, DIRECT_KEYWORDS) \
                    + weighted_keyword_score(body_text, DIRECT_KEYWORDS)
        context_pts = weighted_keyword_score(body_text, CONTEXT_KEYWORDS)
        combo_pts   = pair_score(body_text, PAIR_RULES)
        boost_pts   = 5 if is_core_freight_company(company_name) else 0
        form_adj    = form_signal_adjustment(form)
        score       = direct_pts + context_pts + combo_pts + boost_pts + form_adj

        # rationale & tags
        rationale_bits = []
        if boost_pts: rationale_bits.append("core transport operator")
        if direct_pts: rationale_bits.append("direct transport language")
        if context_pts: rationale_bits.append("macro/sector signal (IP, retail, construction, ports, inputs)")
        if combo_pts: rationale_bits.append("paired signal (output + transport stress, border + capacity)")
        rationale = "; ".join(rationale_bits) if rationale_bits else "logistics-adjacent operational signal"
        modes = guess_mode_tags(body_text)
        snippet = find_relevant_snippet(body_text, snippet_patterns)

        cand = {
            "date_run": now.date().isoformat(),
            "company": company_name,
            "ticker": ticker,
            "form": form,
            "filed_at": filed_at,
            "url": url,
            "rationale": rationale,
            "tags": modes,
            "score": score,
            "snippet": snippet,
            "direct_pts": direct_pts,
            "context_pts": context_pts,
            "combo_pts": combo_pts,
            "boost_pts": boost_pts,
            "form_adj": form_adj,
        }
        candidates.append(cand)

        # Only surface "core" forms if they pass the score bar
        if form in CORE_FORMS:
            total_core_form += 1
            if score >= SCORE_THRESHOLD:
                fulltext_path = None
                if score >= FULLTEXT_THRESHOLD:
                    base_pieces = [now.date().isoformat(), form, safe_slug(company_name)[:20] or "issuer"]
                    base_name = "_".join(safe_slug(p) for p in base_pieces if p) + "_" + tiny_hash(url or company_name or "") + ".txt"
                    fulltext_path = os.path.join("output", "full_text", base_name)
                    with open(fulltext_path, "w", encoding="utf-8") as ffull:
                        ffull.write(f"Company: {company_name}\nTicker: {ticker}\nForm: {form}\nFiled At: {filed_at}\nURL: {url}\nScore: {score}\n")
                        ffull.write("\n=== BEGIN FILING TEXT ===\n\n")
                        ffull.write(body_text)

                cand_hit = dict(cand)
                cand_hit["fulltext_file"] = fulltext_path if fulltext_path else ""
                hits.append(cand_hit)

    # Sort surfaced hits (filing_date is already ISO; lexical sort works)
    form_rank = {
        "8-K": 1, "6-K": 1, "8-K/A": 1, "6-K/A": 1,
        "10-Q": 2, "10-K": 2, "20-F": 2, "10-Q/A": 2, "10-K/A": 2, "20-F/A": 2,
        "S-4": 3, "S-4/A": 3,
        "424B": 4, "424B1": 4, "424B2": 4, "424B3": 4, "424B4": 4, "424B5": 4,
        "FWP": 4, "S-1": 4, "S-1/A": 4, "S-3": 4, "S-3/A": 4, "425": 4,
    }
    def sort_key(item):
        filed_str = str(item.get("filed_at", ""))
        return (-item["score"], form_rank.get(item["form"], 99), filed_str)
    hits.sort(key=sort_key)

    # Report
    bullet_blocks.append("🔎 SEC Filings With Freight / Supply Chain Impact (recent feed)\n")

    if not hits:
        bullet_blocks.append("• No high-signal core forms matched the freight/macro criteria above.\n")

        # === Recall Floor: Top-5 near-misses from ALL forms ===
        generic_pat = re.compile(r"\b(supply|inventory|production|capacity|port|logistics|warehouse|CapEx|construction|PMI|ISM|retail)\b", re.I)
        near = []
        for c in candidates:
            text_for_check = " ".join([
                c.get("snippet") or "",
                c.get("rationale") or "",
                c.get("company") or ""
            ])
            if c["score"] >= 1 or generic_pat.search(text_for_check):
                near.append(c)
        near.sort(key=lambda x: -x["score"])
        near = near[:5]
        if near:
            bullet_blocks.append("🔁 Recall floor — notable near-misses (manual review suggested):\n")
            for c in near:
                off_form_tag = "" if c["form"] in CORE_FORMS else " (off-form)"
                bullet_blocks.append(
                    summarize_for_newsletter(
                        company=c["company"], ticker=c["ticker"], form=(c["form"] or "(unknown form)") + off_form_tag,
                        filed_at=c["filed_at"], url=c["url"],
                        rationale=f"(near-miss) score={c['score']} [direct={c['direct_pts']} context={c['context_pts']} pairs={c['combo_pts']} form={c['form_adj']} boost={c['boost_pts']}] — {c['rationale']}",
                        tags=c["tags"], snippet=c["snippet"], fulltext_path_if_any=""
                    )
                )
        else:
            bullet_blocks.append("• (Recall floor found no near-misses to surface.)\n")
    else:
        for h in hits:
            bullet_blocks.append(
                summarize_for_newsletter(
                    company=h["company"], ticker=h["ticker"], form=h["form"],
                    filed_at=h["filed_at"], url=h["url"],
                    rationale=h["rationale"] + f" [score={h['score']} direct={h['direct_pts']} context={h['context_pts']} pairs={h['combo_pts']} form={h['form_adj']} boost={h['boost_pts']}]",
                    tags=h["tags"], snippet=h["snippet"], fulltext_path_if_any=h["fulltext_file"],
                )
            )

    bullet_blocks.append(
        f"[internal note: surfaced {len(hits)}; total_seen={total_seen}; total_core_form={total_core_form}; "
        f"SCORE_THRESHOLD={SCORE_THRESHOLD}; FULLTEXT_THRESHOLD={FULLTEXT_THRESHOLD}]"
    )

    # Quick debug: forms we actually saw
    unique_forms = sorted({c["form"] for c in candidates if c["form"]})
    print(f"[debug] forms_seen={len(unique_forms)} sample={unique_forms[:12]}")

    # Write outputs
    with open("output/freight_pulse_sec_raw.txt", "w", encoding="utf-8") as ftxt:
        ftxt.write("\n".join(bullet_blocks))

    csv_path = "output/freight_pulse_sec_full.csv"
    new_file = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as fcsv:
        writer = csv.writer(fcsv)
        if new_file:
            writer.writerow([
                "date_run","company","ticker","form","filed_at","score",
                "rationale","mode_tags","snippet","url","fulltext_file",
                "direct_pts","context_pts","combo_pts","boost_pts","form_adj"
            ])
        # Always log surfaced hits; if none, log recall-floor near-misses
        rows = hits if hits else (near if 'near' in locals() else [])
        for h in rows:
            writer.writerow([
                now.date().isoformat(), h["company"], h["ticker"], h["form"], h["filed_at"], h["score"],
                h["rationale"], "; ".join(h["tags"]), h["snippet"], h["url"], h.get("fulltext_file",""),
                h["direct_pts"], h["context_pts"], h["combo_pts"], h["boost_pts"], h["form_adj"]
            ])

    # Echo to Actions logs
    print("\n".join(bullet_blocks))

if __name__ == "__main__":
    main()
