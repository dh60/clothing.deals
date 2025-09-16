import math
import logging
from .base import BaseScraper, Product

class GravitypopeScraper(BaseScraper):
    def __init__(self, config):
        super().__init__(config)
        self.items_per_page = 250
        self.captcha_enabled = False

    def _build_url(self, gender: str, category: str, page: int = 1, size: str | None = None) -> str:
        base = f"https://www.gravitypope.com/collections/{gender}-{category}"
        return f"{base}/products.json?limit={self.items_per_page}&page={page}"

    def _build_metadata_url(self, category_key: str) -> str:
        gender, category = category_key.split('_')
        return f"https://www.gravitypope.com/collections/{gender}-{category}.json"

    def _parse_metadata(self, data: dict) -> tuple[int, list]:
        total_products = data.get('collection', {}).get('products_count', 0)
        total_pages = math.ceil(total_products / self.items_per_page)
        return total_pages, []

    def _parse_product(self, category_key: str, item: dict, size: str | None = None) -> Product | None:
        brand = item.get('vendor', '').lower()
        name = item.get('title', '')
        handle = item.get('handle', '')
        url = f"https://www.gravitypope.com/products/{handle}" if handle else ''
        if not handle or not name:
            logging.warning(f"Skipping product in {category_key} with missing handle or title: name={name}, handle={handle}, url={url}")
            return None
        link = url
        variants = item.get('variants', [])
        if not variants:
            logging.warning(f"Skipping product in {category_key} with no variants: name={name}, url={link}")
            return None
        sizes = []
        sale_prices = set()
        original_prices = set()
        for v in variants:
            if not v.get('available', False):
                continue
            size = v.get('option2') or v.get('option1') or v.get('title', '')
            if not size:
                logging.warning(f"Skipping variant in {category_key} with no size for {name}, url={link}")
                continue
            sizes.append(size)
            price = v.get('price')
            compare_at = v.get('compare_at_price', price)
            try:
                price = float(str(price).replace(',', '')) if price is not None else 0.0
                compare_at = float(str(compare_at).replace(',', '')) if compare_at is not None else price
            except (ValueError, TypeError) as e:
                logging.warning(f"Using default price (0.0) for variant in {category_key} for {name} due to invalid price data: price={price}, compare_at={compare_at}, url={link}, error={e}")
                price = 0.0
                compare_at = 0.0
            sale_prices.add(price)
            original_prices.add(compare_at)
        if not sizes:
            logging.warning(f"Skipping product in {category_key} with no valid sizes: name={name}, sizes={sizes}, url={link}")
            return None
        sale_price = min(sale_prices) if sale_prices else 0.0
        original_price = max(original_prices) if original_prices else 0.0
        discount = round((original_price - sale_price) / original_price * 100) if original_price > sale_price else 0
        if not sale_prices:
            logging.info(f"Product in {category_key} has no valid prices, using defaults: name={name}, sale_price=0.0, original_price=0.0, url={link}")
        return Product(
            category_key=category_key,
            brand=brand,
            name=name,
            sale_price=sale_price,
            original_price=original_price,
            discount=discount,
            link=link,
            sizes=sorted(set(sizes))
        )