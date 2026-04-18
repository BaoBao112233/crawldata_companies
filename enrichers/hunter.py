"""Hunter.io domain search — find decision maker emails."""
from urllib.parse import urlparse

import httpx

from config import settings
from models import Company

HUNTER_API = "https://api.hunter.io/v2/domain-search"

TARGET_TITLES = [
    "purchasing", "procurement", "sourcing",
    "partnership", "business development",
    "ceo", "founder", "director", "general manager",
    "sales", "commercial",
]


async def enrich_hunter(company: Company) -> Company:
    if not settings.hunter_api_key or not company.website:
        return company

    domain = _extract_domain(company.website)
    if not domain:
        return company

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(HUNTER_API, params={
                "domain": domain,
                "api_key": settings.hunter_api_key,
                "limit": 10,
            })
            resp.raise_for_status()
            data = resp.json().get("data", {})
    except Exception as e:
        print(f"[HUNTER] {domain}: {e}")
        return company

    emails = data.get("emails", [])
    best = _pick_best(emails)
    if best and not company.email:
        company.email = best["value"]

    return company


def _pick_best(emails: list[dict]) -> dict | None:
    for title_kw in TARGET_TITLES:
        for e in emails:
            position = (e.get("position") or "").lower()
            if title_kw in position:
                return e
    return emails[0] if emails else None


def _extract_domain(website: str) -> str:
    try:
        parsed = urlparse(website if "://" in website else f"https://{website}")
        return parsed.netloc.lstrip("www.")
    except Exception:
        return ""
