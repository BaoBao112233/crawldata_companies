"""Cosmoprof Asia exhibitor list scraper."""
from bs4 import BeautifulSoup

from config import COUNTRIES
from crawlers.base import BaseCrawler, make_client
from models import Company

EXHIBITOR_URL = "https://www.cosmoprof-asia.com/en/exhibitors/"

COUNTRY_NAME_MAP = {
    "indonesia": "Indonesia",
    "thailand": "Thailand",
    "malaysia": "Malaysia",
    "philippines": "Philippines",
    "kazakhstan": "Kazakhstan",
    "mongolia": "Mongolia",
}


class CosmoprofCrawler(BaseCrawler):
    name = "cosmoprof"
    rate_limit_seconds = 3.0

    async def crawl(self, country: str) -> list[Company]:
        country_display = COUNTRY_NAME_MAP.get(country, country.title())
        companies: list[Company] = []

        async with make_client() as client:
            try:
                resp = await self._get(client, EXHIBITOR_URL)
            except Exception as e:
                self.log(country, f"failed to fetch exhibitor list: {e}")
                return []

            soup = BeautifulSoup(resp.text, "lxml")
            companies = self._parse_exhibitors(soup, country, country_display)

            # Try paginated API endpoint used by many trade show sites
            if not companies:
                companies = await self._try_api(client, country, country_display)

        self.log(country, f"found {len(companies)} exhibitors")
        return companies

    def _parse_exhibitors(self, soup: BeautifulSoup, country: str, country_display: str) -> list[Company]:
        results = []
        # Common selectors for exhibitor pages
        cards = soup.select(
            "div.exhibitor-card, article.exhibitor, "
            "div[class*='exhibitor'], li[class*='exhibitor']"
        )
        for card in cards:
            country_el = card.select_one("[class*='country'], span.country, p.country")
            if country_el and country_display.lower() not in country_el.get_text().lower():
                continue

            name_el = card.select_one("h2, h3, a[class*='name'], span[class*='name']")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            if not name:
                continue

            link_el = card.select_one("a[href]")
            href = link_el.get("href", "") if link_el else ""

            website_el = card.select_one("a[class*='website']")
            website = website_el.get("href", "") if website_el else ""

            results.append(Company(
                company_name=name,
                country=country,
                source=self.name,
                source_url=href,
                website=website,
            ))
        return results

    async def _try_api(self, client, country: str, country_display: str) -> list[Company]:
        """Fallback: try JSON API endpoint some trade show sites use."""
        api_urls = [
            f"https://www.cosmoprof-asia.com/wp-json/exhibitors/v1/list?country={country_display}",
            f"https://www.cosmoprof-asia.com/en/exhibitors/?country={country_display}&format=json",
        ]
        for url in api_urls:
            try:
                resp = await self._get(client, url)
                data = resp.json()
                if isinstance(data, list) and data:
                    return [
                        Company(
                            company_name=item.get("name", item.get("title", "")),
                            country=country,
                            source=self.name,
                            source_url=item.get("url", ""),
                            website=item.get("website", ""),
                            description=item.get("description", item.get("profile", "")),
                        )
                        for item in data
                        if item.get("name") or item.get("title")
                    ]
            except Exception:
                continue
        return []
