"""Pipeline run for 2026-05-03.

Goal: discover ~1000 NEW cosmetics distributor companies across 6 target
countries, dedup against everything previously found, enrich emails via
website scraping (free) + Hunter.io (limited).

Resume-safe: each country's raw SERP candidates are checkpointed to JSON
under output/03052026/raw/{country}.json so re-runs skip SERP for already-
processed countries.
"""
import asyncio
import csv
import json
import os
import re
from dataclasses import asdict
from glob import glob
from pathlib import Path
from urllib.parse import urlparse

from crawlers.serp import SerpCrawler
from enrichers import hunter, website
from exporters import csv_export
from models import Company
from pipeline import dedup

OUT_DIR = Path("output/03052026")
RAW_DIR = OUT_DIR / "raw"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

TARGET_COUNTRIES = [
    "indonesia", "thailand", "malaysia",
    "philippines", "kazakhstan", "mongolia",
]

EXPANDED_QUERIES: dict[str, list[str]] = {
    "indonesia": [
        '"importir kosmetik" Indonesia "hubungi"',
        '"importir resmi" kosmetik Indonesia kontak',
        '"importir produk kecantikan" Indonesia',
        '"distributor resmi" kosmetik impor Indonesia',
        '"distributor tunggal" kosmetik Indonesia',
        '"agen tunggal" kosmetik impor Indonesia',
        '"sole distributor" imported cosmetics Indonesia',
        '"authorized importer" cosmetics Indonesia',
        '"authorized distributor" imported cosmetics Indonesia',
        '"distributor kosmetik korea" Indonesia kontak',
        '"distributor kosmetik jepang" Indonesia kontak',
        '"distributor kosmetik eropa" Indonesia kontak',
        '"distributor kosmetik amerika" Indonesia',
        '"distributor parfum impor" Indonesia',
        '"distributor skincare impor" Indonesia',
        '"importir parfum" Indonesia hubungi',
        '"importir skincare" Indonesia kontak',
        '"importir makeup" Indonesia kontak',
        '"PT" "importir" kosmetik Indonesia',
        '"PT" "distributor" "produk impor" kosmetik',
        '"cosmetics importer" Jakarta contact',
        '"imported cosmetics distributor" Indonesia contact',
        '"perusahaan importir kosmetik" Indonesia',
        '"penyalur resmi" kosmetik impor Indonesia',
        'site:.co.id "importir" "kosmetik" "hubungi"',
        'site:.co.id "distributor" "kosmetik impor"',
        '"izin edar" kosmetik importir Indonesia hubungi',
        '"BPOM" importir kosmetik Indonesia hubungi',
        '"luxury cosmetics importer" Indonesia',
        '"K-beauty importer" Indonesia contact',
    ],
    "thailand": [
        '"cosmetics importer" Thailand "contact us"',
        '"ผู้นำเข้าเครื่องสำอาง" ติดต่อ',
        '"นำเข้าเครื่องสำอาง" บริษัท ติดต่อ',
        '"ตัวแทนจำหน่ายแต่เพียงผู้เดียว" เครื่องสำอาง',
        '"ตัวแทนจำหน่ายอย่างเป็นทางการ" เครื่องสำอาง นำเข้า',
        '"sole importer" cosmetics Thailand contact',
        '"authorized importer" cosmetics Thailand',
        '"sole distributor" imported cosmetics Thailand',
        '"authorized distributor" imported cosmetics Thailand',
        '"distributor of imported cosmetics" Thailand',
        '"importer of cosmetics" Thailand "contact us"',
        '"นำเข้า" เครื่องสำอาง เกาหลี ติดต่อ',
        '"นำเข้า" เครื่องสำอาง ญี่ปุ่น ติดต่อ',
        '"นำเข้า" เครื่องสำอาง ฝรั่งเศส ติดต่อ',
        '"นำเข้า" "น้ำหอม" "บริษัท" ติดต่อ',
        '"K-beauty importer" Thailand contact',
        '"J-beauty importer" Thailand contact',
        '"european cosmetics importer" Thailand',
        '"luxury beauty importer" Thailand contact',
        '"perfume importer" Thailand contact us',
        '"skincare importer" Thailand contact',
        '"haircare importer" Thailand contact',
        '"co.,ltd" cosmetics importer Thailand contact',
        '"company limited" cosmetics importer Thailand',
        'site:.co.th "cosmetics" "importer"',
        'site:.co.th "ผู้นำเข้า" "เครื่องสำอาง"',
        '"FDA Thailand" cosmetics importer contact',
        '"นำเข้าและจัดจำหน่าย" เครื่องสำอาง',
        '"importer and distributor" cosmetics Thailand',
        '"นำเข้าเครื่องสำอางจากต่างประเทศ" บริษัท',
    ],
    "malaysia": [
        '"cosmetics importer" Malaysia "contact us"',
        '"pengimport kosmetik" Malaysia hubungi',
        '"pengimport produk kecantikan" Malaysia',
        '"sole importer" cosmetics Malaysia contact',
        '"authorized importer" cosmetics Malaysia',
        '"sole distributor" imported cosmetics Malaysia',
        '"authorized distributor" imported cosmetics Malaysia',
        '"importer of cosmetics" Malaysia "contact"',
        '"distributor of imported cosmetics" Malaysia',
        '"sdn bhd" cosmetics importer Malaysia',
        '"sdn bhd" cosmetics distributor imported Malaysia',
        '"K-beauty importer" Malaysia contact',
        '"J-beauty importer" Malaysia contact',
        '"european cosmetics importer" Malaysia',
        '"luxury cosmetics importer" Malaysia',
        '"perfume importer" Malaysia contact',
        '"skincare importer" Malaysia contact',
        '"haircare importer" Malaysia contact',
        '"makeup importer" Malaysia contact',
        '"fragrance importer" Malaysia contact',
        'site:.com.my "cosmetics" "importer"',
        'site:.com.my "pengimport" "kosmetik"',
        '"NPRA" cosmetics importer Malaysia contact',
        '"halal cosmetics importer" Malaysia',
        '"distributor of imported brand" cosmetics Malaysia',
        '"importer and distributor" cosmetics Malaysia',
        '"pengimport rasmi" kosmetik Malaysia',
        '"pengedar tunggal" kosmetik diimport Malaysia',
        '"agen tunggal" kosmetik impor Malaysia',
        '"penyalur" kosmetik diimport Malaysia hubungi',
    ],
    "philippines": [
        '"cosmetics importer" Philippines "contact us"',
        '"beauty importer" Philippines contact',
        '"sole importer" cosmetics Philippines contact',
        '"authorized importer" cosmetics Philippines',
        '"sole distributor" imported cosmetics Philippines',
        '"authorized distributor" imported cosmetics Philippines',
        '"importer of cosmetics" Philippines "contact us"',
        '"distributor of imported cosmetics" Philippines',
        '"K-beauty importer" Philippines contact',
        '"J-beauty importer" Philippines contact',
        '"european cosmetics importer" Philippines',
        '"luxury cosmetics importer" Philippines',
        '"perfume importer" Philippines contact',
        '"fragrance importer" Philippines contact',
        '"skincare importer" Philippines contact',
        '"haircare importer" Philippines contact',
        '"makeup importer" Philippines contact',
        '"derma cosmetics importer" Philippines',
        'site:.com.ph "cosmetics" "importer"',
        'site:.com.ph "imported" "cosmetics" "distributor"',
        '"FDA Philippines" cosmetics importer contact',
        '"FDA-licensed" cosmetics importer Philippines',
        '"corporation" cosmetics importer Philippines contact',
        '"inc" cosmetics importer Philippines contact',
        '"manila" cosmetics importer contact',
        '"cebu" cosmetics importer contact',
        '"importer and distributor" cosmetics Philippines',
        '"licensed cosmetics importer" Philippines',
        '"sole distributor" perfume Philippines contact',
        '"distributor of imported beauty" Philippines',
    ],
    "kazakhstan": [
        '"импортер косметики" Казахстан контакт',
        '"импорт косметики" Казахстан компания контакт',
        '"эксклюзивный дистрибьютор" косметика Казахстан',
        '"официальный дистрибьютор" косметика импорт Казахстан',
        '"официальный импортер" косметика Казахстан',
        '"sole importer" cosmetics Kazakhstan contact',
        '"authorized importer" cosmetics Kazakhstan',
        '"sole distributor" imported cosmetics Kazakhstan',
        '"authorized distributor" imported cosmetics Kazakhstan',
        '"импортер парфюмерии" Казахстан контакт',
        '"импортер корейской косметики" Казахстан',
        '"импортер японской косметики" Казахстан',
        '"импортер европейской косметики" Казахстан',
        '"K-beauty importer" Kazakhstan contact',
        '"luxury cosmetics importer" Kazakhstan',
        '"perfume importer" Kazakhstan contact',
        '"ТОО" импортер косметики Казахстан',
        '"ТОО" дистрибьютор косметики импорт Казахстан',
        'site:.kz "импортер" "косметика" контакты',
        'site:.kz "импорт" "косметика" "контакты"',
        'site:.kz "эксклюзивный" "дистрибьютор" "косметика"',
        '"Алматы" импортер косметики контакты',
        '"Астана" импортер косметики контакты',
        '"импорт и дистрибуция" косметика Казахстан',
        '"импортер и дистрибьютор" косметика Казахстан',
        '"professional cosmetics importer" Kazakhstan',
        '"haircare importer" Kazakhstan contact',
        '"makeup importer" Kazakhstan contact',
        '"skincare importer" Kazakhstan contact',
        '"derma cosmetics importer" Kazakhstan',
    ],
    "mongolia": [
        '"cosmetics importer" Mongolia contact',
        '"гоо сайхны импортлогч" Монгол холбоо',
        '"гадаадаас оруулдаг" гоо сайхан Монгол',
        '"sole importer" cosmetics Mongolia contact',
        '"authorized importer" cosmetics Mongolia',
        '"sole distributor" imported cosmetics Mongolia',
        '"authorized distributor" imported cosmetics Mongolia',
        '"importer of cosmetics" Mongolia "contact"',
        '"distributor of imported cosmetics" Mongolia',
        '"K-beauty importer" Mongolia contact',
        '"J-beauty importer" Mongolia contact',
        '"korean cosmetics importer" Mongolia',
        '"japanese cosmetics importer" Mongolia',
        '"european cosmetics importer" Mongolia',
        '"luxury cosmetics importer" Mongolia',
        '"perfume importer" Mongolia contact',
        '"fragrance importer" Mongolia contact',
        '"skincare importer" Mongolia contact',
        '"haircare importer" Mongolia contact',
        '"makeup importer" Mongolia contact',
        '"Ulaanbaatar" cosmetics importer contact',
        '"Ulaanbaatar" perfume importer contact',
        'site:.mn "cosmetics" "importer"',
        'site:.mn "гоо сайхан" "импорт" "холбоо"',
        '"гоо сайхны барааны импортлогч"',
        '"албан ёсны импортлогч" гоо сайхан Монгол',
        '"албан ёсны дистрибьютор" гоо сайхан импорт',
        '"LLC" cosmetics importer Mongolia',
        '"corporation" imported cosmetics distributor Mongolia',
        '"importer and distributor" cosmetics Mongolia',
    ],
}

IMPORT_KEYWORDS = [
    "import", "imported", "importer", "importing",
    "distributor", "distribution", "wholesale", "wholesaler",
    # Indonesian
    "impor", "importir", "distributor", "agen tunggal", "penyalur",
    # Thai
    "นำเข้า", "ผู้นำเข้า", "ตัวแทนจำหน่าย",
    # Malay
    "pengimport", "pengedar", "agen",
    # Russian
    "импорт", "импортер", "импортёр", "дистрибьютор", "дистрибутор",
    # Mongolian
    "импорт", "импортлогч", "дистрибьютор", "гадаад",
]

MANUFACTURER_KEYWORDS = [
    "manufacturer", "manufacturing", "produsen", "pabrik", "produsen kosmetik",
    "ผู้ผลิต", "โรงงาน", "pengilang", "kilang",
    "производитель", "завод", "үйлдвэрлэгч",
    "oem", "odm", "private label", "contract manufacturing",
    "salon", "spa", "beauty clinic", "klinik kecantikan",
]

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
    "aliexpress.com", "amazon.com", "ebay.com",
    "shopee.co.id", "shopee.com.my", "shopee.com.ph", "shopee.co.th",
    "tokopedia.com", "lazada.com", "lazada.com.ph", "lazada.com.my",
    "blibli.com", "bukalapak.com",
    "instagram.com", "facebook.com", "twitter.com", "tiktok.com",
    "youtube.com", "pinterest.com",
    "indeed.com", "jobstreet.com", "jobthai.com", "glints.com",
    "google.com", "google.co.id", "google.co.th",
    "github.com", "medium.com", "reddit.com",
    "scribd.com", "issuu.com", "slideshare.net",
    "researchgate.net", "academia.edu",
    "hktdc.com", "globalsources.com",
    "amazon.in", "ebay.co.uk",
    "tripadvisor.com", "booking.com",
    "fda.gov.ph", "bpom.go.id",
    "olx.co.id", "carousell.ph", "carousell.com.my",
}

NON_COMPANY_URL_PATTERNS = [
    "/blog/", "/news/", "/article/", "/wp-content/",
    ".pdf", "/research/", "/forum/", "/question",
    "/listing/", "/category/", "/tag/", "/author/",
    "/jobs/", "/job/", "/career/", "/lowongan-kerja",
]

PHONE_JUNK_RE = re.compile(r"^[\s\d\-]{1,6}$")


def is_real_company_candidate(c: Company) -> bool:
    url = (c.source_url or c.website or "").lower()
    if not url:
        return False
    if any(p in url for p in NON_COMPANY_URL_PATTERNS):
        return False
    host = urlparse(url).netloc.lower().lstrip("www.")
    host = host[4:] if host.startswith("www.") else host
    if host in NON_COMPANY_DOMAINS:
        return False
    name = c.company_name.lower()
    bad_keywords = [
        "question", "news", "article", "conference", "proceedings",
        "strategi", "intelijen", "case study", "guide",
        "pendaftaran", "jasa pengurusan", "about us",
        "wikipedia", "youtube", "facebook", "instagram",
        "lowongan", "loker", "jobs at", "career",
    ]
    if any(kw in name for kw in bad_keywords) and len(name) > 20:
        return False
    return True


def is_importer_focused(c: Company) -> bool:
    """Keep only candidates that look like importers/distributors of foreign
    cosmetics — drop pure manufacturers, salons, spas."""
    text = f"{c.company_name} {c.description}".lower()
    has_import_signal = any(kw in text for kw in IMPORT_KEYWORDS)
    if not has_import_signal:
        return False
    # If it's primarily a manufacturer/salon/spa, drop it
    manufacturer_hits = sum(1 for kw in MANUFACTURER_KEYWORDS if kw in text)
    import_hits = sum(1 for kw in IMPORT_KEYWORDS if kw in text)
    if manufacturer_hits > import_hits:
        return False
    return True


def clean_phone(phone: str) -> str:
    phone = (phone or "").strip()
    if not phone or PHONE_JUNK_RE.match(phone):
        return ""
    digits = re.sub(r"\D", "", phone)
    if len(digits) < 7:
        return ""
    return phone


def clean_company(c: Company) -> Company:
    c.phone = clean_phone(c.phone)
    return c


def load_all_existing() -> set[str]:
    """Load every company name|country key from prior outputs.
    Excludes the current run's own dir to avoid self-dedup."""
    keys: set[str] = set()
    files = [
        f for f in sorted(glob("output/**/*.csv", recursive=True))
        if "03052026" not in f
    ]
    for f in files:
        try:
            with open(f, encoding="utf-8-sig") as fh:
                for row in csv.DictReader(fh):
                    name = (row.get("company_name") or "").strip().lower()
                    country = (row.get("country") or "").strip().lower()
                    if name and country:
                        keys.add(f"{name}|{country}")
        except Exception as e:
            print(f"[LOAD] skip {f}: {e}")
    print(f"[LOAD] {len(keys)} existing (name,country) keys across {len(files)} files")
    return keys


async def discover_country(serp: SerpCrawler, country: str) -> list[Company]:
    """Run all expanded queries for one country and aggregate results.
    Checkpoints raw results to RAW_DIR/{country}.json — if checkpoint exists,
    loads from disk and skips all SerpAPI calls."""
    import httpx
    from config import settings

    ckpt = RAW_DIR / f"{country}.json"
    if ckpt.exists():
        with open(ckpt, encoding="utf-8") as fh:
            data = json.load(fh)
        companies = [Company(**d) for d in data]
        print(f"[SERP][{country.upper()}] loaded {len(companies)} from checkpoint")
        return companies

    companies: list[Company] = []
    seen_urls: set[str] = set()
    queries = EXPANDED_QUERIES[country]

    from crawlers.serp import _is_blocked
    async with httpx.AsyncClient(timeout=30) as client:
        for q in queries:
            print(f"[SERP][{country.upper()}] query: {q}")
            results = []
            for attempt in range(3):
                try:
                    resp = await client.get(
                        "https://serpapi.com/search",
                        params={
                            "q": q, "api_key": settings.serpapi_key,
                            "num": 20, "hl": "en",
                        },
                    )
                    resp.raise_for_status()
                    results = resp.json().get("organic_results", [])
                    break
                except Exception as e:
                    print(f"[SERP][{country.upper()}] err (try {attempt+1}/3): {e}")
                    await asyncio.sleep(2 * (attempt + 1))

            for r in results:
                url = r.get("link", "")
                if not url or url in seen_urls or _is_blocked(url):
                    continue
                seen_urls.add(url)
                companies.append(Company(
                    company_name=r.get("title", "").split(" - ")[0].split(" | ")[0].strip(),
                    country=country,
                    source="serp",
                    source_url=url,
                    website=f"{urlparse(url).scheme}://{urlparse(url).netloc}",
                    description=r.get("snippet", ""),
                ))
            await asyncio.sleep(0.4)

    # Persist checkpoint immediately
    with open(ckpt, "w", encoding="utf-8") as fh:
        json.dump([asdict(c) for c in companies], fh, ensure_ascii=False, indent=2)
    print(f"[SERP][{country.upper()}] {len(companies)} candidates → {ckpt}")
    return companies


QUERY_BUDGET_PER_COUNTRY = {
    # indonesia already checkpointed → 0 cost on resume
    "indonesia": 30,
    # remaining 5 countries: trim to fit 76-credit SerpAPI budget
    "thailand": 14,
    "malaysia": 14,
    "philippines": 14,
    "kazakhstan": 14,
    "mongolia": 14,
}


async def main() -> None:
    existing_keys = load_all_existing()

    # Apply per-country query budget (only matters when not loading from cache)
    for country, n in QUERY_BUDGET_PER_COUNTRY.items():
        EXPANDED_QUERIES[country] = EXPANDED_QUERIES[country][:n]

    # ---- Discovery ----
    print(f"\n[DISCOVER] expanded SERP across {len(TARGET_COUNTRIES)} countries")
    serp = SerpCrawler()
    raw: list[Company] = []
    for country in TARGET_COUNTRIES:
        try:
            raw.extend(await discover_country(serp, country))
        except Exception as e:
            print(f"[SERP][{country.upper()}] failed: {e}")

    print(f"[DISCOVER] total raw across countries: {len(raw)}")

    # Snapshot raw BEFORE dedup/filter — defensive against any later bug
    csv_export.export(raw, str(OUT_DIR / "raw_all_pre_dedup.csv"))

    # ---- Dedup against current batch + against history ----
    raw = dedup(raw)
    print(f"[DEDUP] {len(raw)} unique after dedup")
    new_companies = [c for c in raw if c.dedup_key() not in existing_keys]
    print(f"[DEDUP] {len(new_companies)} after removing existing")
    new_companies = [c for c in new_companies if is_real_company_candidate(c)]
    print(f"[FILTER] {len(new_companies)} after generic filter")
    new_companies = [c for c in new_companies if is_importer_focused(c)]
    print(f"[FILTER] {len(new_companies)} after importer-focus filter")

    # Save raw-after-filter discovery before enrichment as a safety net
    csv_export.export(new_companies, str(OUT_DIR / "new_raw.csv"))

    # ---- Website enrichment (free) ----
    print("\n[ENRICH] website scrape …")
    enriched = await website.enrich_all(new_companies, concurrency=8)
    enriched = [clean_company(c) for c in enriched]

    # Snapshot intermediate
    csv_export.export(enriched, str(OUT_DIR / "new_enriched_websites.csv"))

    # ---- Hunter.io fallback (very limited quota) ----
    if os.getenv("HUNTER_API_KEY"):
        no_email = [c for c in enriched if not c.email]
        # Keep budget very small — Hunter free has only 21 left
        budget = 18
        target = no_email[:budget]
        print(f"[ENRICH] Hunter.io for {len(target)} top candidates "
              f"(budget {budget}, total no-email {len(no_email)})")
        for c in target:
            try:
                await hunter.enrich_hunter(c)
            except Exception as e:
                print(f"[HUNTER] {c.website}: {e}")

    enriched = [clean_company(c) for c in enriched]
    with_email = [c for c in enriched if c.email]
    print(f"\n[RESULT] {len(enriched)} new total, {len(with_email)} with email")

    csv_export.export(enriched, str(OUT_DIR / "new_companies_all.csv"))
    csv_export.export(with_email, str(OUT_DIR / "new_companies_with_email.csv"))

    print(f"\n[DONE] outputs in {OUT_DIR}/")
    for p in sorted(OUT_DIR.glob("*.csv")):
        print(f"  - {p}")


if __name__ == "__main__":
    asyncio.run(main())
