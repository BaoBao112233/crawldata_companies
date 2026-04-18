"""Scrape contact info directly from company websites."""
import asyncio
import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from crawlers.base import extract_emails, extract_phones, make_client
from models import Company

CONTACT_PATHS = ["/contact", "/contact-us", "/about", "/about-us", "/hubungi", "/kontak", "/liên-hệ"]


async def enrich_website(company: Company) -> Company:
    if not company.website:
        return company

    base = company.website.rstrip("/")
    urls_to_try = [base] + [base + path for path in CONTACT_PATHS]

    async with make_client(timeout=15) as client:
        for url in urls_to_try:
            text = await _fetch_text(client, url)
            if not text:
                continue

            emails = extract_emails(text)
            phones = extract_phones(text)

            if emails and not company.email:
                company.email = emails[0]
            if phones and not company.phone:
                company.phone = phones[0].strip()

            if company.email:
                break

    return company


async def enrich_all(companies: list[Company], concurrency: int = 5) -> list[Company]:
    sem = asyncio.Semaphore(concurrency)

    async def _bounded(c: Company) -> Company:
        async with sem:
            return await enrich_website(c)

    return await asyncio.gather(*[_bounded(c) for c in companies])


async def _fetch_text(client, url: str) -> str:
    try:
        resp = await client.get(url)
        soup = BeautifulSoup(resp.text, "lxml")
        # Remove scripts and styles
        for tag in soup(["script", "style"]):
            tag.decompose()
        return soup.get_text(" ", strip=True)
    except Exception:
        return ""
