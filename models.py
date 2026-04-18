from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Company:
    company_name: str
    country: str
    source: str
    source_url: str = ""
    website: str = ""
    email: str = ""
    phone: str = ""
    address: str = ""
    description: str = ""
    brands_carried: str = ""
    scale: str = ""
    import_frequency: str = ""
    hs_codes: str = ""
    found_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))

    def dedup_key(self) -> str:
        return f"{self.company_name.lower().strip()}|{self.country.lower()}"

    def to_dict(self) -> dict:
        return {
            "company_name": self.company_name,
            "country": self.country,
            "website": self.website,
            "email": self.email,
            "phone": self.phone,
            "address": self.address,
            "description": self.description,
            "brands_carried": self.brands_carried,
            "source": self.source,
            "source_url": self.source_url,
            "scale": self.scale,
            "import_frequency": self.import_frequency,
            "hs_codes": self.hs_codes,
            "found_at": self.found_at,
        }
