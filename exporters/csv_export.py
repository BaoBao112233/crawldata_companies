import csv
import os
from datetime import datetime
from pathlib import Path

from models import Company

FIELDNAMES = [
    "company_name", "country", "website", "email", "phone",
    "address", "description", "brands_carried",
    "source", "source_url", "scale", "import_frequency",
    "hs_codes", "found_at",
]


def export(companies: list[Company], output_path: str) -> str:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        for c in companies:
            writer.writerow(c.to_dict())
    return output_path


def default_output_path(countries: list[str], source: str | None) -> str:
    base = os.getenv("OUTPUT_DIR", "./output")
    tag = source or "all"
    country_tag = "_".join(countries) if len(countries) <= 3 else f"{len(countries)}countries"
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    return f"{base}/{tag}_{country_tag}_{ts}.csv"
