"""Tighten filters on the 03052026 batch — drop listing/consulting/tradeshow
noise and keep only true importer/distributor company sites."""
import csv
from pathlib import Path
from urllib.parse import urlparse

from exporters import csv_export
from models import Company

OUT_DIR = Path("output/03052026")

# Hosts that are clearly NOT distributor companies
JUNK_HOSTS = {
    # marketplaces / aggregators / listing sites
    "tradekey.com", "importer.tradekey.com", "tradewheel.com",
    "go4worldbusiness.com", "rocketreach.co", "exportgenius.in",
    "exportgenius.com", "indotrading.com", "ladinaayuindonesia.web.indotrading.com",
    "indonetwork.co.id", "alibaba.com", "indonesian.alibaba.com",
    "lazada.co.id", "lazada.com.ph", "lazada.com.my",
    "tokopedia.com", "shopee.com", "shopee.co.id", "shopee.co.th",
    "amazon.com", "ebay.com", "olx.co.id",
    "businesslist.ph", "yellowpages.co.th", "thaiwebsearch.com",
    "kompass.com", "europages.com",
    # social / publishing / forums
    "linkedin.com", "id.linkedin.com", "nz.linkedin.com", "sg.linkedin.com",
    "facebook.com", "instagram.com", "tiktok.com", "youtube.com",
    "pantip.com", "scribd.com", "id.scribd.com", "issuu.com",
    "linktr.ee", "wordpress.com", "produsenkosmetikindonesia.wordpress.com",
    "broadcastmagz.com", "globenewswire.com", "prestigeonline.com",
    "businesshubasia.com", "business-indonesia.org",
    "dokumen.pub", "academia.edu", "researchgate.net",
    # consulting / regulatory / shipping / accounting
    "konradlegal.com", "enterslice.com", "evershinecpa.com",
    "izinkosmetik.com", "amarc.co.th", "tonlexing.com",
    "atpserve.com", "sls2017law.com", "softaps.com",
    "dhl.com", "fedex.com", "cargo.com",
    "pwcargologistics.com", "masterimportir.com",
    "importlicensing.wto.org", "wto.org",
    "govinfo.gov", "gov.uk", "gov.ph", "fda.gov.ph", "bpom.go.id",
    "cs.nyu.edu",
    # generic news / blogs
    "wordpress.com", "blogspot.com", "medium.com",
    "edencolorsthailand.com",  # blog review
    "brilitas.com",  # blog
    "businessasia.com",
}

JUNK_HOST_SUFFIXES = (
    ".wordpress.com", ".blogspot.com", ".scribd.com",
    ".linkedin.com", ".facebook.com", ".indotrading.com",
    ".alibaba.com", ".gov", ".gov.uk", ".gov.ph",
    ".wto.org", ".academia.edu", ".tradekey.com",
)

# Description keywords that indicate non-distributor (consulting, regulation, blogs)
NEGATIVE_KEYWORDS = [
    "consulting", "consultant", "konsultan",
    "law firm", "lawyer", "legal advisor",
    "license", "licensing", "izin", "registration",
    "guide", "tutorial", "how to", "tips",
    "regulation", "regulatory",
    "shipping", "logistic", "freight forwarder",
    "training", "course",
    "review article", "เปิดร้าน", "ราคาส่ง",
    "blog", "artikel",
    "trade show", "exhibition",
    "manufacturer", "produsen", "pabrik",
    "factory", "OEM", "ODM", "private label",
    "salon", "spa", "klinik",
    "pwc cargo", "law office",
]


def load(path: Path) -> list[Company]:
    out = []
    with open(path, encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            row = {k: v for k, v in row.items()
                   if k in Company.__dataclass_fields__}
            out.append(Company(**row))
    return out


def is_junk_host(host: str) -> bool:
    host = host.lower().lstrip("www.")
    if host.startswith("www."):
        host = host[4:]
    if host in JUNK_HOSTS:
        return True
    if any(host.endswith(s) for s in JUNK_HOST_SUFFIXES):
        return True
    return False


def is_distributor_company(c: Company) -> bool:
    url = c.source_url or c.website
    if not url:
        return False
    host = urlparse(url).netloc
    if is_junk_host(host):
        return False
    text = f"{c.company_name} {c.description}".lower()
    if any(kw in text for kw in NEGATIVE_KEYWORDS):
        return False
    return True


def main() -> None:
    companies = load(OUT_DIR / "new_companies_all.csv")
    print(f"[LOAD] {len(companies)} companies before clean")

    cleaned = [c for c in companies if is_distributor_company(c)]
    print(f"[CLEAN] {len(cleaned)} after dropping junk hosts + negative keywords")

    with_email = [c for c in cleaned if c.email]
    print(f"[CLEAN] {len(with_email)} with email")

    # Keep raw, write cleaned
    csv_export.export(cleaned, str(OUT_DIR / "new_companies_cleaned.csv"))
    csv_export.export(with_email,
                      str(OUT_DIR / "new_companies_cleaned_with_email.csv"))

    from collections import Counter
    by_country = Counter(c.country for c in cleaned)
    by_country_email = Counter(c.country for c in with_email)
    print("\nPer country (all / with email):")
    for c in sorted(by_country):
        print(f"  {c:12} {by_country[c]:4d} / {by_country_email[c]:4d}")


if __name__ == "__main__":
    main()
