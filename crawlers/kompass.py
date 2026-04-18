"""Kompass.com B2B directory scraper — NACE 4645 (cosmetics wholesale)."""
from bs4 import BeautifulSoup

from config import KOMPASS_COUNTRY_CODES
from crawlers.base import BaseCrawler, make_client
from models import Company

BASE_URL = "https://www.kompass.com"
# NACE 4645 = Wholesale of perfume and cosmetics
SEARCH_URL = BASE_URL + "/searchCompanies?text=cosmetics+distributor&activity=45053000&country={cc}&from={offset}"


class KompassCrawler(BaseCrawler):
    name = "kompass"
    rate_limit_seconds = 3.0
    max_pages = 5

    async def crawl(self, country: str) -> list[Company]:
        cc = KOMPASS_COUNTRY_CODES.get(country)
        if not cc:
            self.log(country, "no country code — skipping")
            return []

        companies: list[Company] = []
        async with make_client() as client:
            for page in range(self.max_pages):
                offset = page * 10
                url = SEARCH_URL.format(cc=cc.upper(), offset=offset)
                self.log(country, f"page {page + 1}: {url}")
                try:
                    resp = await self._get(client, url)
                except Exception as e:
                    self.log(country, f"error on page {page + 1}: {e}")
                    break

                soup = BeautifulSoup(resp.text, "lxml")
                cards = soup.select("div.companyCard, article.company-card, div[class*='company']")
                if not cards:
                    # Try alternate selector
                    cards = soup.select("li.result-item, div.result-item")
                if not cards:
                    self.log(country, f"no cards on page {page + 1}, stopping")
                    break

                for card in cards:
                    company = self._parse_card(card, country)
                    if company:
                        companies.append(company)

        self.log(country, f"found {len(companies)} companies")
        return companies

    def _parse_card(self, card, country: str) -> Company | None:
        name_el = card.select_one("a.company-name, h2.company-name, span.companyName, a[class*='name']")
        if not name_el:
            return None
        name = name_el.get_text(strip=True)
        if not name:
            return None

        link_el = card.select_one("a[href*='/a/']") or name_el
        href = link_el.get("href", "")
        source_url = BASE_URL + href if href.startswith("/") else href

        website_el = card.select_one("a[class*='website'], span[class*='website']")
        website = website_el.get_text(strip=True) if website_el else ""

        address_el = card.select_one("span[class*='address'], p[class*='address']")
        address = address_el.get_text(strip=True) if address_el else ""

        desc_el = card.select_one("p[class*='activit'], span[class*='activit'], p.description")
        description = desc_el.get_text(strip=True) if desc_el else ""

        return Company(
            company_name=name,
            country=country,
            source=self.name,
            source_url=source_url,
            website=website,
            address=address,
            description=description,
        )
