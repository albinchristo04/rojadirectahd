#!/usr/bin/env python3
"""
Rojadirecta HD – Match Details & iFrame Scraper
Targets the exact HTML structure of rojadirectahd.pl:

Main page table #my-table:
  <tr>
    <td><span class='t'>HH:MM</span></td>
    <td align='center'><span class='evento XX'/></td>   ← XX = flag/sport CSS class
    <td align='left'>COMPETITION <a href='ver/canal-N.php'><b>Team A vs Team B en Vivo</b></a></td>
  </tr>

Stream pages (ver/*.php) contain an <iframe> inside #player or similar.

Groups duplicate rows (same match, different channels) into one match entry
with multiple stream_links, each carrying its own iframes.
"""

import json
import logging
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────
BASE_URL    = "https://www.rojadirectahd.pl/"
OUTPUT_DIR  = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "matches.json"

REQUEST_TIMEOUT = 20    # seconds per HTTP call
STREAM_DELAY    = 1.5   # polite delay between stream-page visits (seconds)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": BASE_URL,
}

# CSS class → human-readable flag/sport label
# Sourced from the actual stylesheet on the page
FLAG_MAP = {
    # Countries
    "ad":"Andorra","ae":"UAE","arg":"Argentina","at":"Austria","au":"Australia",
    "be":"Belgium","bg":"Bulgaria","bo":"Bolivia","by":"Belarus","br":"Brazil",
    "ca":"Canada","cl":"Chile","cn":"China","co":"Colombia","cr":"Costa Rica",
    "cz":"Czech Rep.","de":"Germany","dk":"Denmark","do":"Dominican Rep.",
    "dz":"Algeria","ec":"Ecuador","eg":"Egypt","en":"England","es":"Spain",
    "fi":"Finland","fr":"France","gb":"Great Britain","gr":"Greece","gt":"Guatemala",
    "hn":"Honduras","hr":"Croatia","hu":"Hungary","ie":"Ireland","il":"Israel",
    "in":"India","is":"Iceland","it":"Italy","jp":"Japan","kr":"South Korea",
    "lu":"Luxembourg","mx":"Mexico","ni":"Nicaragua","nl":"Netherlands","no":"Norway",
    "nz":"New Zealand","pa":"Panama","pe":"Peru","pl":"Poland","pr":"Puerto Rico",
    "pt":"Portugal","py":"Paraguay","qa":"Qatar","ro":"Romania","rs":"Serbia",
    "ru":"Russia","sa":"Saudi Arabia","se":"Sweden","sk":"Slovakia","sv":"El Salvador",
    "th":"Thailand","tr":"Turkey","us":"USA","uy":"Uruguay","ve":"Venezuela",
    # Competitions / sports
    "soccer":"Football","bkb":"Basketball","voley":"Volleyball","tenis":"Tennis",
    "golf":"Golf","snooker":"Snooker","mlb":"MLB Baseball","el":"Europa League",
    "ch":"Champions League","uefa":"UEFA","suda":"Copa Sudamericana",
    "concacaf":"CONCACAF","caf":"CAF","eu21":"U-21 Euro","eu19":"U-19 Euro",
    "am":"Copa América","nfl":"NFL","cfl":"CFL","rugby":"Rugby","wwe":"WWE",
    "boxeo":"Boxing","dardos":"Darts","mundial":"World Cup","euro":"Euro Cup",
    "america":"América","rfef":"RFEF","otros":"Others","nba":"NBA","nhl":"NHL",
    "ci":"CI","csuda":"Copa Sudamericana","lib":"Copa Libertadores",
    "uefacup":"UEFA Cup","ms21":"U-21","oro":"Gold Cup","icc":"ICC Cricket",
    "f1":"Formula 1","beisbol":"Baseball","eurobkb":"EuroBasket",
    "wimbledon":"Wimbledon","rg":"Roland Garros","qa22":"Qatar 2022",
    "uefanl":"UEFA Nations League","mun":"Manchester Utd","fifa":"FIFA",
    "oli":"Olympics","toro":"Bull Running","ufc":"UFC","nazcar":"NASCAR",
    "motogp":"MotoGP",
}


# ─────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────
# HTTP helper
# ─────────────────────────────────────────────────────
session = requests.Session()
session.headers.update(HEADERS)


def fetch(url: str, retries: int = 3) -> BeautifulSoup | None:
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "lxml")
        except requests.HTTPError as e:
            log.warning("HTTP %s → %s", e.response.status_code, url)
            break
        except requests.RequestException as e:
            log.warning("Attempt %d failed for %s: %s", attempt, url, e)
            if attempt < retries:
                time.sleep(2 ** attempt)
    return None


# ─────────────────────────────────────────────────────
# Flag-class extractor
# ─────────────────────────────────────────────────────

def get_flag_class(span_tag) -> tuple[str, str]:
    """
    Given a <span class='evento XX'/> tag return (css_class, human_label).
    The second class after 'evento' is the actual country/competition code.
    """
    classes = span_tag.get("class", [])
    # remove 'evento', take the first remaining class
    code = next((c for c in classes if c != "evento"), "")
    label = FLAG_MAP.get(code, code.upper() if code else "Unknown")
    return code, label


# ─────────────────────────────────────────────────────
# Main-page parser
# ─────────────────────────────────────────────────────

def parse_main_page(soup: BeautifulSoup) -> list[dict]:
    """
    Parse #my-table rows into raw match records.

    Each row yields ONE stream link. Multiple rows sharing the same
    (time, competition, match_title) will be grouped later.

    Row structure (3 <td>s):
      0 → <span class='t'>HH:MM</span>
      1 → <span class='evento XX'/>        ← flag/competition icon
      2 → COMPETITION TEXT <a href=...><b>MATCH TITLE</b></a>
    """
    records = []

    table = soup.find("table", id="my-table")
    if not table:
        log.warning("Could not find #my-table – falling back to any table rows")
        table = soup

    tbody = table.find("tbody") or table
    rows  = tbody.find_all("tr")
    log.info("Found %d <tr> elements in match table", len(rows))

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue  # header or empty row

        # ── Cell 0: time ──────────────────────────────────────────────────
        time_span = cells[0].find("span", class_="t")
        match_time = time_span.get_text(strip=True) if time_span else cells[0].get_text(strip=True)

        # ── Cell 1: flag / competition icon ───────────────────────────────
        evento_span = cells[1].find("span", class_="evento")
        flag_code, flag_label = ("", "Unknown")
        if evento_span:
            flag_code, flag_label = get_flag_class(evento_span)

        # ── Cell 2: competition name + stream link + match title ──────────
        cell3 = cells[2]

        # Stream link  →  <a href='ver/canal-N.php' target='_blank'><b>...</b></a>
        link_tag  = cell3.find("a", href=True)
        match_title = ""
        stream_url  = ""
        channel_name = ""

        if link_tag:
            raw_href = link_tag.get("href", "")
            # Fix self-closing <a/> siblings – BeautifulSoup handles this fine
            stream_url  = urljoin(BASE_URL, raw_href)
            channel_name = _channel_name_from_url(raw_href)
            bold = link_tag.find("b")
            match_title = bold.get_text(strip=True) if bold else link_tag.get_text(strip=True)
            # Clean up trailing "en Vivo" for a cleaner title
            match_title = re.sub(r"\s+en\s+Vivo\s*$", "", match_title, flags=re.I).strip()

        # Competition text = everything in cell3 before the <a> tag
        # We grab all direct text nodes that precede the link
        comp_parts = []
        for node in cell3.children:
            if node == link_tag:
                break
            if isinstance(node, str):
                comp_parts.append(node.strip())
        competition = " ".join(p for p in comp_parts if p).strip()

        if not match_title and not stream_url:
            continue  # completely empty row

        records.append({
            "time":         match_time,
            "flag_code":    flag_code,
            "flag_label":   flag_label,
            "competition":  competition,
            "match_title":  match_title,
            "stream_url":   stream_url,
            "channel_name": channel_name,
        })

    log.info("Parsed %d raw stream-row records", len(records))
    return records


def _channel_name_from_url(href: str) -> str:
    """Extract a readable channel name from a stream href like 'ver/espn-2.php'."""
    name = re.sub(r"^ver/|\.php$", "", href)
    name = re.sub(r"canal-\d+", "canal", name)  # keep 'canal' generic
    return name.replace("-", " ").title()


# ─────────────────────────────────────────────────────
# Grouping
# ─────────────────────────────────────────────────────

def group_records(records: list[dict]) -> list[dict]:
    """
    Collapse multiple rows for the same match into one entry with a
    list of stream links.

    Grouping key: (time, competition, match_title)
    """
    groups: dict[tuple, dict] = {}

    for r in records:
        key = (r["time"], r["competition"], r["match_title"])
        if key not in groups:
            groups[key] = {
                "time":        r["time"],
                "flag_code":   r["flag_code"],
                "flag_label":  r["flag_label"],
                "competition": r["competition"],
                "match_title": r["match_title"],
                "stream_links": [],
                "iframes":     [],  # filled in later
            }
        if r["stream_url"]:
            groups[key]["stream_links"].append({
                "channel":  r["channel_name"],
                "url":      r["stream_url"],
                "iframes":  [],   # filled in later
            })

    matches = list(groups.values())
    log.info("Grouped into %d unique matches", len(matches))
    return matches


# ─────────────────────────────────────────────────────
# iFrame extraction
# ─────────────────────────────────────────────────────

def extract_iframes(soup: BeautifulSoup, page_url: str) -> list[dict]:
    """Return all <iframe> tags found in a stream page."""
    iframes = []
    for tag in soup.find_all("iframe"):
        src = tag.get("src", "").strip()
        if not src:
            continue
        if src.startswith("//"):
            src = "https:" + src
        elif src.startswith("/"):
            src = urljoin(page_url, src)
        iframes.append({
            "src":            src,
            "width":          tag.get("width"),
            "height":         tag.get("height"),
            "allowfullscreen": tag.has_attr("allowfullscreen"),
            "domain":         urlparse(src).netloc,
        })
    return iframes


def enrich_with_iframes(matches: list[dict]) -> list[dict]:
    """
    Visit every unique stream URL, extract iframes, attach to links + match.
    Uses a simple URL cache to avoid fetching the same page twice.
    """
    cache: dict[str, list[dict]] = {}

    for match in matches:
        all_iframes: list[dict] = []

        for link in match["stream_links"]:
            url = link["url"]
            if url not in cache:
                soup = fetch(url)
                if soup:
                    iframes = extract_iframes(soup, url)
                    log.info("  %-55s → %d iframe(s)", url, len(iframes))
                else:
                    iframes = []
                    log.warning("  FAILED: %s", url)
                cache[url] = iframes
                time.sleep(STREAM_DELAY)

            link["iframes"] = cache[url]
            all_iframes.extend(cache[url])

        # Deduplicate the flat iframe list by src
        seen: set[str] = set()
        match["iframes"] = [
            i for i in all_iframes
            if i["src"] not in seen and not seen.add(i["src"])  # type: ignore[func-returns-value]
        ]

    return matches


# ─────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────

def run() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Fetching main page → %s", BASE_URL)
    main_soup = fetch(BASE_URL)
    if not main_soup:
        log.error("Failed to load main page – aborting.")
        raise SystemExit(1)

    page_title = (
        main_soup.title.string.strip()
        if main_soup.title else "Rojadirecta HD"
    )

    raw      = parse_main_page(main_soup)
    matches  = group_records(raw)
    matches  = enrich_with_iframes(matches)

    payload = {
        "scraped_at":    datetime.now(timezone.utc).isoformat(),
        "source_url":    BASE_URL,
        "page_title":    page_title,
        "total_matches": len(matches),
        "matches":       matches,
    }

    OUTPUT_FILE.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info("✓ Saved %d match(es) to %s", len(matches), OUTPUT_FILE)


if __name__ == "__main__":
    run()
