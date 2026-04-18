"""
Đọc cosmetics_distributors_final.csv, dịch cột description sang 3 thứ tiếng
(VN / EN / KO), sau đó tách thành 2 file: có email và không có email.
"""

from __future__ import annotations

import csv
import sys
import time
from pathlib import Path

from deep_translator import GoogleTranslator

ROOT = Path(__file__).resolve().parents[1]
INPUT_CSV = ROOT / "output" / "cosmetics_distributors_final.csv"
OUT_WITH_EMAIL = ROOT / "output" / "cosmetics_distributors_with_email.csv"
OUT_NO_EMAIL = ROOT / "output" / "cosmetics_distributors_no_email.csv"

TARGET_LANGS = {
    "description_vi": "vi",
    "description_en": "en",
    "description_ko": "ko",
}

# Cache để tránh dịch trùng cùng một chuỗi nhiều lần
_cache: dict[tuple[str, str], str] = {}


def translate(text: str, target: str) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    key = (target, text)
    if key in _cache:
        return _cache[key]

    # Google Translate giới hạn ~5000 ký tự/lần
    chunk = text[:4800]
    for attempt in range(3):
        try:
            result = GoogleTranslator(source="auto", target=target).translate(chunk)
            result = result or ""
            _cache[key] = result
            return result
        except Exception as e:
            print(f"  ! retry {attempt + 1} ({target}): {e}", file=sys.stderr)
            time.sleep(1.5 * (attempt + 1))
    _cache[key] = ""
    return ""


def main() -> None:
    with INPUT_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        base_fields = reader.fieldnames or []

    # Chèn 3 cột mới ngay sau 'description'
    new_fields: list[str] = []
    for col in base_fields:
        new_fields.append(col)
        if col == "description":
            new_fields.extend(TARGET_LANGS.keys())

    total = len(rows)
    print(f"[INFO] Đang dịch {total} công ty sang VI / EN / KO ...")

    for idx, row in enumerate(rows, start=1):
        desc = row.get("description", "") or ""
        for new_col, lang in TARGET_LANGS.items():
            row[new_col] = translate(desc, lang)
        if idx % 10 == 0 or idx == total:
            print(f"  [{idx}/{total}] {row.get('company_name', '')[:60]}")

    # Tách theo email
    with_email = [r for r in rows if (r.get("email") or "").strip()]
    no_email = [r for r in rows if not (r.get("email") or "").strip()]

    for path, data in [(OUT_WITH_EMAIL, with_email), (OUT_NO_EMAIL, no_email)]:
        with path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=new_fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(data)
        print(f"[OK] {path.name}: {len(data)} dòng")


if __name__ == "__main__":
    main()
