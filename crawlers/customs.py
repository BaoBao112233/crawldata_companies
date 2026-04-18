"""Customs/import-export data crawler — ImportGenius & Trademo APIs.

Finds companies that actually import cosmetics (HS 3304/3305/3307).
This is the highest-quality signal: real importers = real distributors.
"""
import httpx

from config import HS_CODES, settings
from crawlers.base import BaseCrawler
from models import Company

IMPORTGENIUS_API = "https://api.importgenius.com/v1"
TRADEMO_API = "https://api.trademo.com/v1"

# Countries served by each API
IMPORTGENIUS_COUNTRIES = {"indonesia", "thailand", "malaysia", "philippines"}
TRADEMO_COUNTRIES = {"kazakhstan", "mongolia"}


class CustomsCrawler(BaseCrawler):
    name = "customs"
    rate_limit_seconds = 1.0

    async def crawl(self, country: str) -> list[Company]:
        if country in IMPORTGENIUS_COUNTRIES:
            return await self._importgenius(country)
        if country in TRADEMO_COUNTRIES:
            return await self._trademo(country)
        self.log(country, "no customs API available for this country")
        return []

    async def _importgenius(self, country: str) -> list[Company]:
        if not settings.importgenius_api_key:
            self.log(country, "IMPORTGENIUS_API_KEY not set — skipping")
            return []

        companies: list[Company] = []
        async with httpx.AsyncClient(timeout=30) as client:
            for hs in HS_CODES:
                self.log(country, f"querying HS {hs}")
                try:
                    resp = await client.get(
                        f"{IMPORTGENIUS_API}/shipments",
                        headers={"Authorization": f"Bearer {settings.importgenius_api_key}"},
                        params={
                            "country": country,
                            "hs_code": hs,
                            "trade_type": "import",
                            "limit": 200,
                        },
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    companies.extend(self._parse_importgenius(data, country, hs))
                except Exception as e:
                    self.log(country, f"ImportGenius error (HS {hs}): {e}")

        deduped = self._dedup_by_buyer(companies)
        self.log(country, f"found {len(deduped)} unique importers")
        return deduped

    def _parse_importgenius(self, data: dict, country: str, hs: str) -> list[Company]:
        results = []
        for shipment in data.get("shipments", data.get("results", [])):
            buyer = shipment.get("consignee_name") or shipment.get("buyer_name", "")
            if not buyer:
                continue
            results.append(Company(
                company_name=buyer.strip().title(),
                country=country,
                source=self.name,
                source_url="https://importgenius.com",
                address=shipment.get("consignee_address", ""),
                description=shipment.get("description", ""),
                brands_carried=shipment.get("shipper_name", ""),
                hs_codes=hs,
                import_frequency=str(shipment.get("shipment_count", "")),
                scale=self._estimate_scale(shipment),
            ))
        return results

    async def _trademo(self, country: str) -> list[Company]:
        if not settings.trademo_api_key:
            self.log(country, "TRADEMO_API_KEY not set — skipping")
            return []

        companies: list[Company] = []
        async with httpx.AsyncClient(timeout=30) as client:
            for hs in HS_CODES:
                self.log(country, f"querying HS {hs} via Trademo")
                try:
                    resp = await client.post(
                        f"{TRADEMO_API}/trade-data/search",
                        headers={
                            "Authorization": f"Bearer {settings.trademo_api_key}",
                            "Content-Type": "application/json",
                        },
                        json={
                            "importer_country": country.upper(),
                            "hs_code": hs,
                            "limit": 200,
                        },
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    companies.extend(self._parse_trademo(data, country, hs))
                except Exception as e:
                    self.log(country, f"Trademo error (HS {hs}): {e}")

        deduped = self._dedup_by_buyer(companies)
        self.log(country, f"found {len(deduped)} unique importers")
        return deduped

    def _parse_trademo(self, data: dict, country: str, hs: str) -> list[Company]:
        results = []
        for record in data.get("data", data.get("records", [])):
            buyer = record.get("buyer_name") or record.get("importer_name", "")
            if not buyer:
                continue
            results.append(Company(
                company_name=buyer.strip().title(),
                country=country,
                source=self.name,
                source_url="https://trademo.com",
                address=record.get("buyer_address", ""),
                brands_carried=record.get("seller_name", ""),
                hs_codes=hs,
                import_frequency=str(record.get("shipment_count", "")),
                scale=self._estimate_scale(record),
            ))
        return results

    def _estimate_scale(self, record: dict) -> str:
        value = record.get("total_value_usd") or record.get("shipment_value", 0)
        try:
            v = float(value)
        except (TypeError, ValueError):
            return ""
        if v > 500_000:
            return "Large"
        if v > 50_000:
            return "Medium"
        return "Small"

    def _dedup_by_buyer(self, companies: list[Company]) -> list[Company]:
        seen: dict[str, Company] = {}
        for c in companies:
            key = c.company_name.lower().strip()
            if key not in seen:
                seen[key] = c
            else:
                # Merge HS codes
                existing = seen[key]
                existing_hs = set(existing.hs_codes.split(",")) if existing.hs_codes else set()
                existing_hs.add(c.hs_codes)
                existing.hs_codes = ",".join(sorted(existing_hs))
        return list(seen.values())
