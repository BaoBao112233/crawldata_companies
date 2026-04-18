from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    serpapi_key: str = ""
    bright_data_username: str = ""
    bright_data_password: str = ""
    bright_data_host: str = "brd.superproxy.io:22225"
    importgenius_api_key: str = ""
    trademo_api_key: str = ""
    hunter_api_key: str = ""
    output_dir: str = "./output"


settings = Settings()

COUNTRIES = ["indonesia", "thailand", "malaysia", "philippines", "kazakhstan", "mongolia"]

HS_CODES = ["3304", "3305", "3307", "3301"]

KOMPASS_COUNTRY_CODES = {
    "indonesia": "id",
    "thailand": "th",
    "malaysia": "my",
    "philippines": "ph",
    "kazakhstan": "kz",
    "mongolia": "mn",
}

SERP_QUERIES: dict[str, list[str]] = {
    "indonesia": [
        '"distributor kosmetik" import Indonesia email',
        '"importir kosmetik" Indonesia "hubungi kami"',
        '"cosmetics distributor" Indonesia wholesale import',
        'site:.id "distributor" "kosmetik" "import" kontak',
    ],
    "thailand": [
        '"cosmetics distributor" Thailand import "contact us"',
        '"ผู้จัดจำหน่ายเครื่องสำอาง" นำเข้า ติดต่อ',
        '"beauty distributor" Thailand wholesale import email',
        'site:.th "cosmetics" "distributor" "import" contact',
    ],
    "malaysia": [
        '"cosmetics distributor" Malaysia import halal "contact"',
        '"pengedar kosmetik" Malaysia import emel',
        '"beauty distributor" Malaysia wholesale email',
        'site:.my "cosmetics" "distributor" "import" contact',
    ],
    "philippines": [
        '"cosmetics distributor" Philippines import "contact us"',
        '"beauty distributor" Philippines wholesale import email',
        '"cosmetics importer" Philippines contact',
        'site:.ph "cosmetics" "distributor" "import"',
    ],
    "kazakhstan": [
        '"cosmetics distributor" Kazakhstan import contact',
        '"дистрибьютор косметики" Казахстан импорт контакт',
        '"beauty distributor" Kazakhstan wholesale email',
        'site:.kz "косметика" "дистрибьютор" "импорт"',
    ],
    "mongolia": [
        '"cosmetics distributor" Mongolia import contact',
        '"гоо сайхны бүтээгдэхүүн" импорт Монгол холбоо барих',
        '"beauty distributor" Mongolia wholesale email',
        'site:.mn "cosmetics" "distributor" import',
    ],
}
