# Cosmetics Distributor Crawler

## Mục tiêu
Tìm các công ty **nhập khẩu và phân phối mỹ phẩm quốc tế vào nội địa** tại 6 nước:
**Indonesia, Thailand, Mongolia, Malaysia, Philippines, Kazakhstan**

Đầu ra: file CSV chứa thông tin công ty + liên hệ.

---

## Định nghĩa "target company"

Công ty cần tìm phải thỏa mãn:

- Nhập khẩu mỹ phẩm từ nước ngoài (HS Code 3304/3305/3307)
- Phân phối vào thị trường nội địa nước đó (không phải sản xuất)
- Có quy mô thương mại (không phải cá nhân mua lẻ)

---

## Kiến trúc Pipeline

```
[Discovery] → [Dedup & Validate] → [Enrichment] → [CSV Export]
     ↓
  Nguồn 1: Google SERP (SerpAPI)
  Nguồn 2: Kompass.com (scrape)
  Nguồn 3: Cosmoprof Exhibitor list (scrape)
  Nguồn 4: Beautetrade.com (scrape)
  Nguồn 5: Customs data APIs (ImportGenius / Trademo)
```

---

## Cấu trúc thư mục

```
crawldata_companies/
├── CLAUDE.md
├── .env.example
├── requirements.txt
├── config.py              # Cấu hình quốc gia, HS codes, keywords
├── models.py              # Dataclass Company, Contact
├── crawlers/
│   ├── base.py            # BaseCrawler: retry, rate limit, proxy
│   ├── serp.py            # Google SERP qua SerpAPI
│   ├── kompass.py         # Kompass.com directory scraper
│   ├── cosmoprof.py       # Cosmoprof Asia exhibitor list
│   ├── beautetrade.py     # BeauteTrade.com scraper
│   └── customs.py         # ImportGenius / Trademo API
├── enrichers/
│   ├── website.py         # Extract email/phone từ website công ty
│   └── hunter.py          # Hunter.io domain search
├── pipeline.py            # Orchestrator chạy toàn bộ pipeline
├── exporters/
│   └── csv_export.py      # Ghi kết quả ra CSV
└── tests/
    └── test_crawlers.py
```

---

## CSV Output Schema

| Column | Mô tả |
|---|---|
| `company_name` | Tên công ty |
| `country` | Quốc gia |
| `website` | Website |
| `email` | Email liên hệ |
| `phone` | Số điện thoại |
| `address` | Địa chỉ |
| `description` | Mô tả ngắn hoạt động |
| `brands_carried` | Thương hiệu mỹ phẩm đang phân phối |
| `source` | Nguồn tìm thấy (serp/kompass/customs/...) |
| `source_url` | URL gốc |
| `scale` | Large / Medium / Small (ước tính) |
| `import_frequency` | Tần suất nhập hàng (từ customs data) |
| `hs_codes` | Mã HS đã nhập |
| `found_at` | Thời gian crawl |

---

## Chiến lược tìm kiếm theo nguồn

### 1. Google SERP (serp.py)
Dùng SerpAPI hoặc Bright Data SERP. Query mẫu theo từng nước:

```python
QUERIES = {
    "indonesia": [
        '"distributor kosmetik" "import" site:.id',
        '"cosmetics distributor" Indonesia "contact us"',
        '"importir kosmetik" Indonesia email',
    ],
    "thailand": [
        '"cosmetics distributor" Thailand import "contact"',
        '"ผู้จัดจำหน่ายเครื่องสำอาง" นำเข้า',
    ],
    "malaysia": [
        '"cosmetics distributor" Malaysia import halal',
        '"pengedar kosmetik" Malaysia import',
    ],
    "philippines": [
        '"cosmetics distributor" Philippines import "contact us"',
        '"beauty distributor" Philippines wholesale',
    ],
    "kazakhstan": [
        '"cosmetics distributor" Kazakhstan import',
        '"дистрибьютор косметики" Казахстан импорт',
    ],
    "mongolia": [
        '"cosmetics distributor" Mongolia import',
        '"гоо сайхны бүтээгдэхүүн" импорт Монгол',
    ],
}
```

### 2. Kompass.com (kompass.py)

- URL pattern: `https://www.kompass.com/a/cosmetics-distributor/{country_code}/`
- Paginate qua trang danh mục → extract company cards
- NACE code: **4645** (Wholesale of perfume and cosmetics)

### 3. Cosmoprof Asia Exhibitor List (cosmoprof.py)

- URL: `https://www.cosmoprof-asia.com/en/exhibitors/`
- Filter theo country trong exhibitor list
- Các công ty ở đây đang CHỦ ĐỘNG tìm đối tác → quality leads

### 4. Customs Data (customs.py)

- **ImportGenius API** cho Indonesia, Thailand, Malaysia, Philippines
- **Trademo Intel API** cho Kazakhstan, Mongolia
- Query: HS Code `3304*`, shipper_country NOT IN target country (= hàng nhập khẩu)
- Group by buyer → tính import_frequency và estimated_value

---

## Enrichment Logic

Khi đã có `website` của công ty:

1. Scrape trang Contact / About → tìm email, phone, address
2. Gọi Hunter.io `/domain-search` → lấy email theo role
3. Ưu tiên title: `purchasing`, `procurement`, `sales`, `ceo`, `founder`, `general manager`

---

## Scale Classification

```python
def classify_scale(company) -> str:
    if company.import_value_usd > 500_000:  return "Large"
    if company.import_value_usd > 50_000:   return "Medium"
    if company.import_frequency > 6:        return "Medium"
    return "Small"
```

---

## Cách chạy

```bash
# Cài đặt
pip install -r requirements.txt
cp .env.example .env   # Điền API keys

# Chạy toàn bộ pipeline
python pipeline.py --countries indonesia,thailand,malaysia --output output/results.csv

# Chạy từng nguồn
python pipeline.py --source serp --countries all --output output/serp.csv
python pipeline.py --source kompass --countries all --output output/kompass.csv
python pipeline.py --source customs --countries indonesia,thailand --output output/customs.csv

# Chỉ enrich lại file CSV cũ
python pipeline.py --enrich-only --input output/results.csv
```

---

## Environment Variables

```env
# SerpAPI (Google Search)
SERPAPI_KEY=

# Proxy (nếu cần bypass blocking)
BRIGHT_DATA_USERNAME=
BRIGHT_DATA_PASSWORD=

# Customs APIs
IMPORTGENIUS_API_KEY=
TRADEMO_API_KEY=

# Email enrichment
HUNTER_API_KEY=

# Output
OUTPUT_DIR=./output
```

---

## Coding Standards

- Python 3.11+, async/await với `httpx.AsyncClient`
- Dùng `dataclasses` cho models, không dùng ORM (không cần DB)
- Mỗi crawler trả về `list[Company]`
- Dedup theo `(company_name.lower(), country)` trước khi export
- Retry 3 lần với exponential backoff cho HTTP errors
- Rate limit: tối thiểu 2s giữa các request cùng domain
- Log ra stdout với format: `[SOURCE][COUNTRY] message`
- Không dùng Scrapy — httpx + BeautifulSoup đủ dùng

---

## Thứ tự ưu tiên nguồn dữ liệu

1. **Customs data** — chính xác nhất, công ty thực sự nhập khẩu
2. **Kompass** — directory có sẵn, filter được theo ngành
3. **Cosmoprof** — leads chất lượng cao (chủ động tìm đối tác)
4. **Google SERP** — rộng nhất nhưng cần lọc nhiều
5. **BeauteTrade** — bổ sung thêm
