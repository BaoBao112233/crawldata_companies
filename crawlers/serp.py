"""Google SERP crawler via SerpAPI — discovers companies from search results."""
import httpx

from config import SERP_QUERIES, settings
from crawlers.base import BaseCrawler, extract_emails
from models import Company

SERPAPI_ENDPOINT = "https://serpapi.com/search"

BLOCKED_DOMAINS = {
    "instagram.com", "facebook.com", "twitter.com", "tiktok.com",
    "youtube.com", "linkedin.com", "pinterest.com",
    "tokopedia.com", "shopee.co.id", "shopee.com", "lazada.com",
    "bukalapak.com", "blibli.com", "alibaba.com", "aliexpress.com",
    "amazon.com", "ebay.com",
    "wikipedia.org", "wikimedia.org",
    "indeed.com", "jobstreet.com",
    "indonetwork.co.id", "tradekey.com", "made-in-china.com",
}

BLOCKED_DOMAIN_PATTERNS = [".go.id", ".gov.", ".mil.", "go.th", "go.ph", "gov.kz", "gov.mn"]


def _is_blocked(url: str) -> bool:
    from urllib.parse import urlparse
    try:
        host = urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return True
    if host in BLOCKED_DOMAINS:
        return True
    return any(p in host for p in BLOCKED_DOMAIN_PATTERNS)


class SerpCrawler(BaseCrawler):
    name = "serp"
    rate_limit_seconds = 1.5

    async def crawl(self, country: str) -> list[Company]:
        if not settings.serpapi_key:
            self.log(country, "SERPAPI_KEY not set — skipping")
            return []

        queries = SERP_QUERIES.get(country, [])
        companies: list[Company] = []
        seen_urls: set[str] = set()

        async with httpx.AsyncClient(timeout=20) as client:
            for query in queries:
                self.log(country, f"query: {query}")
                results = await self._search(client, query)
                for r in results:
                    url = r.get("link", "")
                    if not url or url in seen_urls or _is_blocked(url):
                        continue
                    seen_urls.add(url)
                    companies.append(Company(
                        company_name=r.get("title", "").split(" - ")[0].strip(),
                        country=country,
                        source=self.name,
                        source_url=url,
                        website=self._root_domain(url),
                        description=r.get("snippet", ""),
                    ))

        self.log(country, f"found {len(companies)} candidates")
        return companies

    async def _search(self, client: httpx.AsyncClient, query: str) -> list[dict]:
        try:
            resp = await client.get(SERPAPI_ENDPOINT, params={
                "q": query,
                "api_key": settings.serpapi_key,
                "num": 20,
                "hl": "en",
            })
            resp.raise_for_status()
            return resp.json().get("organic_results", [])
        except Exception as e:
            print(f"[SERP] error: {e}")
            return []

    def _root_domain(self, url: str) -> str:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
