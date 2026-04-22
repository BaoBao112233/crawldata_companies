"""Pipeline run for 2026-04-22.

1) Load all old CSVs, dedup, filter companies without email.
2) Re-enrich them via website scrape + Hunter.io.
3) Save to output/22042026/enriched_existing.csv.
4) Run SERP discovery for 5 target countries.
5) Enrich new companies and save to output/22042026/new_companies.csv.
"""
import asyncio
import csv
import os
import re
from glob import glob
from pathlib import Path
from urllib.parse import urlparse

from crawlers.serp import SerpCrawler
from enrichers import hunter, website
from exporters import csv_export
from models import Company
from pipeline import dedup

OUT_DIR = Path("output/22042026")
OUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_COUNTRIES = ["thailand", "indonesia", "malaysia", "philippines", "kazakhstan"]

# Skip URLs/domains that clearly aren't distributor company sites
NON_COMPANY_DOMAINS = {
    "stylekorean.com", "bfuturist.com", "cosmeticindex.com",
    "eximpedia.app", "importgenius.com", "volza.com", "panjiva.com",
    "kompass.com", "indonetwork.co.id", "exportersindia.com",
    "tradekey.com", "made-in-china.com", "alibaba.com",
    "smegbbiblekalotsavam.com", "borobudur.ac.id", "dspace.uii.ac.id",
    "korika.id", "itpcdubai.id", "standar-otskk.pom.go.id",
    "naeema-permit.com", "cekindo.com", "infojasa.co.id",
    "shanhaimap.co.id", "hsh.co.id", "importer.co.id",
    "insightof.id", "triloker.com", "kormesicglobal.com",
    "id.linkedin.com", "linkedin.com", "wikipedia.org",
}

NON_COMPANY_URL_PATTERNS = [
    "/blog/", "/news/", "/article/", "/wp-content/",
    ".pdf", "/research/", "/forum/", "/question",
]


def is_real_company_candidate(c: Company) -> bool:
    """Heuristic: keep only URLs that look like a company's own site."""
    url = (c.source_url or c.website or "").lower()
    if not url:
        return False
    if any(p in url for p in NON_COMPANY_URL_PATTERNS):
        return False
    host = urlparse(url).netloc.lower().lstrip("www.")
    if host in NON_COMPANY_DOMAINS:
        return False
    # Generic nouns that indicate it's a regulation/news page, not a company
    name = c.company_name.lower()
    bad_keywords = [
        "question", "news", "article", "conference", "proceedings",
        "strategi", "intelijen", "case study", "guide",
        "pendaftaran", "jasa pengurusan", "about us",
    ]
    if any(kw in name for kw in bad_keywords) and len(name) > 20:
        return False
    return True


def load_old_companies() -> list[Company]:
    files = sorted(glob("output/old/*.csv"))
    all_rows: list[Company] = []
    for f in files:
        with open(f, encoding="utf-8-sig") as fh:
            for row in csv.DictReader(fh):
                row = {k: v for k, v in row.items() if k in Company.__dataclass_fields__}
                if "company_name" in row and "country" in row and "source" in row:
                    all_rows.append(Company(**row))
    print(f"[LOAD] {len(all_rows)} raw rows across {len(files)} files")
    return all_rows


PHONE_JUNK_RE = re.compile(r"^[\s\d\-]{1,6}$")  # drop too short / date-like phones


def clean_phone(phone: str) -> str:
    phone = (phone or "").strip()
    if not phone:
        return ""
    if PHONE_JUNK_RE.match(phone):
        return ""
    # Must have at least 7 digits
    digits = re.sub(r"\D", "", phone)
    if len(digits) < 7:
        return ""
    return phone


def clean_company(c: Company) -> Company:
    c.phone = clean_phone(c.phone)
    return c


async def main() -> None:
    # --- Part 1: re-enrich existing companies without email ---
    all_old = load_old_companies()
    uniq = dedup(all_old)
    print(f"[DEDUP] {len(uniq)} unique (name,country)")

    # Filter for target countries and valid candidates
    filtered = [
        c for c in uniq
        if c.country.lower() in TARGET_COUNTRIES and is_real_company_candidate(c)
    ]
    print(f"[FILTER] {len(filtered)} real-company candidates in target countries")

    # Keep only those without email (or email clearly invalid)
    no_email = [c for c in filtered if not c.email.strip()]
    print(f"[FILTER] {len(no_email)} without email → will enrich")

    # Also keep companies that already have email (for the merged output)
    have_email = [clean_company(c) for c in filtered if c.email.strip()]

    print("\n[ENRICH] website scrape for no-email subset…")
    enriched = await website.enrich_all(no_email, concurrency=6)

    if os.getenv("HUNTER_API_KEY"):
        print(f"[ENRICH] Hunter.io fallback for still-empty ({sum(1 for c in enriched if not c.email)})…")
        out = []
        for c in enriched:
            if not c.email:
                c = await hunter.enrich_hunter(c)
            out.append(c)
        enriched = out

    enriched = [clean_company(c) for c in enriched]
    merged_existing = have_email + enriched
    gained = sum(1 for c in enriched if c.email)
    print(f"[RESULT] gained email on {gained}/{len(enriched)} enriched companies")

    csv_export.export(merged_existing, str(OUT_DIR / "enriched_existing.csv"))
    csv_export.export(
        [c for c in enriched if c.email],
        str(OUT_DIR / "enriched_existing_emails_only.csv"),
    )

    # --- Part 2: discover new companies via SERP ---
    print("\n[DISCOVER] running SERP crawl for target countries…")
    serp = SerpCrawler()
    new_companies: list[Company] = []
    for country in TARGET_COUNTRIES:
        try:
            new_companies.extend(await serp.crawl(country))
        except Exception as e:
            print(f"[SERP][{country.upper()}] failed: {e}")

    new_companies = dedup(new_companies)
    # Drop duplicates that already exist in our merged set
    existing_keys = {c.dedup_key() for c in merged_existing}
    new_companies = [c for c in new_companies if c.dedup_key() not in existing_keys]
    new_companies = [c for c in new_companies if is_real_company_candidate(c)]
    print(f"[DISCOVER] {len(new_companies)} new unique candidates")

    print("[ENRICH] website scrape for new companies…")
    new_enriched = await website.enrich_all(new_companies, concurrency=6)

    if os.getenv("HUNTER_API_KEY"):
        print("[ENRICH] Hunter.io fallback…")
        out = []
        for c in new_enriched:
            if not c.email:
                c = await hunter.enrich_hunter(c)
            out.append(c)
        new_enriched = out

    new_enriched = [clean_company(c) for c in new_enriched]
    new_with_email = [c for c in new_enriched if c.email]
    print(f"[RESULT] {len(new_with_email)}/{len(new_enriched)} new companies with email")

    csv_export.export(new_enriched, str(OUT_DIR / "new_companies.csv"))
    csv_export.export(new_with_email, str(OUT_DIR / "new_companies_emails_only.csv"))

    # Combined final
    combined = merged_existing + new_enriched
    csv_export.export(combined, str(OUT_DIR / "combined_all.csv"))
    csv_export.export([c for c in combined if c.email], str(OUT_DIR / "combined_emails_only.csv"))

    print(f"\n[DONE] wrote files to {OUT_DIR}/")
    for p in sorted(OUT_DIR.glob("*.csv")):
        print(f"  - {p}")


if __name__ == "__main__":
    asyncio.run(main())
