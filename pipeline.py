"""Main pipeline — orchestrates discovery, enrichment, dedup, CSV export."""
import argparse
import asyncio
import os
from pathlib import Path

from config import COUNTRIES
from crawlers.cosmoprof import CosmoprofCrawler
from crawlers.customs import CustomsCrawler
from crawlers.local_directories import LocalDirectoryCrawler
from crawlers.serp import SerpCrawler
from enrichers import hunter, website
from exporters import csv_export
from models import Company

ALL_CRAWLERS = {
    "serp": SerpCrawler,
    "localdir": LocalDirectoryCrawler,
    "cosmoprof": CosmoprofCrawler,
    "customs": CustomsCrawler,
}


async def run(
    countries: list[str],
    sources: list[str],
    output: str,
    enrich: bool,
    enrich_only: str | None,
) -> None:
    if enrich_only:
        companies = _load_csv(enrich_only)
        print(f"Loaded {len(companies)} companies for enrichment")
    else:
        companies = await discover(countries, sources)
        companies = dedup(companies)
        print(f"\n[PIPELINE] {len(companies)} unique companies after dedup")

    if enrich:
        print("[PIPELINE] enriching websites...")
        companies = list(await website.enrich_all(companies))
        if os.getenv("HUNTER_API_KEY"):
            print("[PIPELINE] enriching via Hunter.io...")
            companies = [await hunter.enrich_hunter(c) for c in companies]

    path = csv_export.export(companies, output)
    print(f"\n[DONE] {len(companies)} companies → {path}")


async def discover(countries: list[str], sources: list[str]) -> list[Company]:
    all_companies: list[Company] = []
    for source_name in sources:
        cls = ALL_CRAWLERS.get(source_name)
        if not cls:
            print(f"[PIPELINE] unknown source: {source_name}")
            continue
        crawler = cls()
        for country in countries:
            try:
                results = await crawler.crawl(country)
                all_companies.extend(results)
            except Exception as e:
                print(f"[{source_name.upper()}][{country.upper()}] failed: {e}")
    return all_companies


def dedup(companies: list[Company]) -> list[Company]:
    seen: dict[str, Company] = {}
    for c in companies:
        key = c.dedup_key()
        if key not in seen:
            seen[key] = c
        else:
            # Merge: prefer non-empty fields
            existing = seen[key]
            for field in ["website", "email", "phone", "address", "description",
                          "brands_carried", "scale", "import_frequency", "hs_codes"]:
                if not getattr(existing, field) and getattr(c, field):
                    setattr(existing, field, getattr(c, field))
            # Accumulate sources
            if c.source not in existing.source:
                existing.source += f",{c.source}"
    return list(seen.values())


def _load_csv(path: str) -> list[Company]:
    import csv
    companies = []
    with open(path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            companies.append(Company(**{k: v for k, v in row.items() if k in Company.__dataclass_fields__}))
    return companies


def main() -> None:
    parser = argparse.ArgumentParser(description="Cosmetics distributor crawler")
    parser.add_argument("--countries", default="all",
                        help="Comma-separated countries or 'all'")
    parser.add_argument("--sources", default="serp,kompass,cosmoprof,customs",
                        help="Comma-separated sources")
    parser.add_argument("--output", default=None,
                        help="Output CSV path (auto-generated if omitted)")
    parser.add_argument("--enrich", action="store_true",
                        help="Enrich companies after discovery")
    parser.add_argument("--enrich-only", default=None, metavar="CSV_PATH",
                        help="Skip discovery, enrich existing CSV file")
    args = parser.parse_args()

    countries = COUNTRIES if args.countries == "all" else [c.strip() for c in args.countries.split(",")]
    sources = [s.strip() for s in args.sources.split(",")]

    output = args.output or csv_export.default_output_path(countries, None if len(sources) > 1 else sources[0])
    Path(output).parent.mkdir(parents=True, exist_ok=True)

    asyncio.run(run(
        countries=countries,
        sources=sources,
        output=output,
        enrich=args.enrich or bool(args.enrich_only),
        enrich_only=args.enrich_only,
    ))


if __name__ == "__main__":
    main()
