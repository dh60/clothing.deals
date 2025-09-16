import urllib.parse
from .base import BaseScraper, Product

class SsenseScraper(BaseScraper):
    def _build_url(self, gender: str, category: str, page: int = 1, size: str | None = None) -> str:
        base_url = self.base_url_template.format(gender=gender, category=category)
        params = {}
        if size:
            params['sizes'] = size
        if page > 1:
            params['page'] = page
        if not params:
            return base_url
        return f"{base_url}?{urllib.parse.urlencode(params)}"

    def _parse_product(self, category_key: str, item: dict, size: str | None = None) -> Product:
        price_info = item.get('priceByCountry', [{}])[0]
        sale_price_str = price_info.get('formattedLowest', {}).get('amount', '')
        original_price_str = price_info.get('formattedPrice', '')
        def parse_price(price_str: str) -> float:
            return float(price_str.replace("$", "").replace(",", "")) if price_str else 0.0
        sale_price = parse_price(sale_price_str)
        original_price = parse_price(original_price_str)
        discount = round((original_price - sale_price) / original_price * 100) if original_price > 0 else 0
        brand = (item.get('brand', {}).get('name', {}).get('en', '') or item.get('brand', '')).lower()
        name = item.get('name', {}).get('en', '') or item.get('name', '')
        link = f"https://www.ssense.com/en-ca{item.get('url')}"
        return Product(
            category_key=category_key, brand=brand, name=name,
            sale_price=sale_price, original_price=original_price, discount=discount,
            link=link, sizes=[size] if size else []
        )

    def _parse_metadata(self, data: dict) -> tuple[int, list]:
        total_pages = data.get('pagination_info', {}).get('totalPages', 1)
        sizes_metadata = data.get('metadata', {}).get('sizes', [])
        return total_pages, sizes_metadata