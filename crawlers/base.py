import asyncio
import re
from abc import ABC, abstractmethod

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from config import settings
from models import Company

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def _build_proxy() -> str | None:
    u = settings.bright_data_username
    p = settings.bright_data_password
    h = settings.bright_data_host
    if u and p:
        return f"http://{u}:{p}@{h}"
    return None


def make_client(timeout: float = 20.0) -> httpx.AsyncClient:
    proxy = _build_proxy()
    return httpx.AsyncClient(
        headers=DEFAULT_HEADERS,
        timeout=timeout,
        follow_redirects=True,
        proxies={"all://": proxy} if proxy else None,
    )


def extract_emails(text: str) -> list[str]:
    pattern = r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
    found = re.findall(pattern, text)
    blocked = {"example.com", "domain.com", "email.com", "youremail.com"}
    return [e for e in found if e.split("@")[-1].lower() not in blocked]


def extract_phones(text: str) -> list[str]:
    pattern = r"[\+\(]?[\d\s\-\(\)]{7,20}"
    return re.findall(pattern, text)


class BaseCrawler(ABC):
    name: str = "base"
    rate_limit_seconds: float = 2.5

    @abstractmethod
    async def crawl(self, country: str) -> list[Company]:
        ...

    async def _get(self, client: httpx.AsyncClient, url: str, **kwargs) -> httpx.Response:
        await asyncio.sleep(self.rate_limit_seconds)
        return await self._get_with_retry(client, url, **kwargs)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _get_with_retry(self, client: httpx.AsyncClient, url: str, **kwargs) -> httpx.Response:
        resp = await client.get(url, **kwargs)
        resp.raise_for_status()
        return resp

    def log(self, country: str, msg: str) -> None:
        print(f"[{self.name.upper()}][{country.upper()}] {msg}")
