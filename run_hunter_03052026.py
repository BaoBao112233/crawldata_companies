"""Apply Hunter.io to companies in 03052026 batch lacking email."""
import asyncio
import csv
from pathlib import Path

from config import settings
from enrichers import hunter
from exporters import csv_export
from models import Company

OUT_DIR = Path("output/03052026")
INPUT = OUT_DIR / "new_companies_all.csv"


def load(path: Path) -> list[Company]:
    out = []
    with open(path, encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            row = {k: v for k, v in row.items()
                   if k in Company.__dataclass_fields__}
            out.append(Company(**row))
    return out


async def main() -> None:
    if not settings.hunter_api_key:
        print("[HUNTER] no API key in settings — abort")
        return

    companies = load(INPUT)
    no_email = [c for c in companies if not c.email and c.website]
    print(f"[LOAD] {len(companies)} total, {len(no_email)} need email")

    # Hunter free has ~21 search credits left; cap at 18
    BUDGET = 18
    targets = no_email[:BUDGET]
    print(f"[HUNTER] querying {len(targets)} domains (budget {BUDGET})")

    gained = 0
    for i, c in enumerate(targets, 1):
        try:
            await hunter.enrich_hunter(c)
            mark = "✓" if c.email else "·"
            print(f"  [{i:02d}/{len(targets)}] {mark} {c.website[:50]:50} → {c.email}")
            if c.email:
                gained += 1
        except Exception as e:
            print(f"  [{i:02d}/{len(targets)}] err {c.website}: {e}")

    print(f"\n[RESULT] +{gained} new emails via Hunter")

    # Re-export
    with_email = [c for c in companies if c.email]
    csv_export.export(companies, str(OUT_DIR / "new_companies_all.csv"))
    csv_export.export(with_email, str(OUT_DIR / "new_companies_with_email.csv"))
    print(f"[SAVE] {len(with_email)} companies with email total")


if __name__ == "__main__":
    asyncio.run(main())
