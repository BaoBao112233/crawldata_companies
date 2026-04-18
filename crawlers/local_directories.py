"""Local B2B directories per country — publicly accessible without login.

Indonesia: indonetwork.co.id search
Thailand:  yellowpages.co.th
Malaysia:  bizcommunity / malaysia.exportersindia.com
Philippines: businesslist.ph
Kazakhstan: kaspi.kz business / kazakhstan.exportersindia.com
Mongolia:  mongolian-trade.mn / mn.kompass.com
"""
import asyncio
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from crawlers.base import BaseCrawler, extract_emails, extract_phones, make_client
from models import Company

SOURCES: dict[str, list[dict]] = {
    "indonesia": [
        {
            "url": "https://www.indonetwork.co.id/product/cosmetics?page={page}",
            "pages": 5,
            "card_sel": "div.product-item, div.company-item",
            "name_sel": "h2 a, h3 a, a.company-name",
            "link_sel": "a[href]",
            "addr_sel": "span.location, div.location",
        },
        {
            "url": "https://www.exportersindia.com/indonesia-cosmetic-distributor.htm?pg={page}",
            "pages": 3,
            "card_sel": "div.prd_dsc_col, div.company-item",
            "name_sel": "h3 a, h2 a",
            "link_sel": "a[href]",
            "addr_sel": "span.location",
        },
    ],
    "thailand": [
        {
            "url": "https://www.thaitradeshow.com/cosmetics?page={page}",
            "pages": 3,
            "card_sel": "div.company-card, div.exhibitor",
            "name_sel": "h3, h2",
            "link_sel": "a[href]",
            "addr_sel": "span.address",
        },
        {
            "url": "https://www.exportersindia.com/thailand-cosmetic-distributor.htm?pg={page}",
            "pages": 3,
            "card_sel": "div.prd_dsc_col",
            "name_sel": "h3 a",
            "link_sel": "a[href]",
            "addr_sel": "span.location",
        },
    ],
    "malaysia": [
        {
            "url": "https://www.exportersindia.com/malaysia-cosmetic-distributor.htm?pg={page}",
            "pages": 3,
            "card_sel": "div.prd_dsc_col",
            "name_sel": "h3 a",
            "link_sel": "a[href]",
            "addr_sel": "span.location",
        },
    ],
    "philippines": [
        {
            "url": "https://www.exportersindia.com/philippine-cosmetic-distributor.htm?pg={page}",
            "pages": 3,
            "card_sel": "div.prd_dsc_col",
            "name_sel": "h3 a",
            "link_sel": "a[href]",
            "addr_sel": "span.location",
        },
    ],
    "kazakhstan": [
        {
            "url": "https://www.exportersindia.com/kazakhstan-cosmetic-distributor.htm?pg={page}",
            "pages": 3,
            "card_sel": "div.prd_dsc_col",
            "name_sel": "h3 a",
            "link_sel": "a[href]",
            "addr_sel": "span.location",
        },
    ],
    "mongolia": [
        {
            "url": "https://www.exportersindia.com/mongolia-cosmetic-distributor.htm?pg={page}",
            "pages": 3,
            "card_sel": "div.prd_dsc_col",
            "name_sel": "h3 a",
            "link_sel": "a[href]",
            "addr_sel": "span.location",
        },
    ],
}


class LocalDirectoryCrawler(BaseCrawler):
    name = "localdir"
    rate_limit_seconds = 3.0

    async def crawl(self, country: str) -> list[Company]:
        source_configs = SOURCES.get(country, [])
        if not source_configs:
            self.log(country, "no local directory configured")
            return []

        companies: list[Company] = []
        async with make_client() as client:
            for cfg in source_configs:
                for page in range(1, cfg["pages"] + 1):
                    url = cfg["url"].format(page=page)
                    self.log(country, f"fetching {url}")
                    try:
                        resp = await self._get(client, url)
                    except Exception as e:
                        self.log(country, f"error: {e}")
                        break

                    soup = BeautifulSoup(resp.text, "lxml")
                    cards = soup.select(cfg["card_sel"])
                    if not cards:
                        self.log(country, f"no cards on page {page}")
                        break

                    for card in cards:
                        c = self._parse(card, cfg, country, resp.url)
                        if c:
                            companies.append(c)

        self.log(country, f"found {len(companies)} companies")
        return companies

    def _parse(self, card, cfg: dict, country: str, base_url) -> Company | None:
        name_el = card.select_one(cfg["name_sel"])
        if not name_el:
            return None
        name = name_el.get_text(strip=True)
        if not name or len(name) < 3:
            return None

        link_el = card.select_one(cfg["link_sel"])
        href = link_el.get("href", "") if link_el else ""
        if href and not href.startswith("http"):
            href = urljoin(str(base_url), href)

        addr_el = card.select_one(cfg["addr_sel"])
        address = addr_el.get_text(strip=True) if addr_el else ""

        text = card.get_text(" ")
        emails = extract_emails(text)
        phones = extract_phones(text)

        return Company(
            company_name=name,
            country=country,
            source=self.name,
            source_url=href,
            address=address,
            email=emails[0] if emails else "",
            phone=phones[0].strip() if phones else "",
        )
