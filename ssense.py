import asyncio
import orjson
import tempfile
import urllib.parse
import logging
import math
from dataclasses import dataclass, field
from collections import defaultdict
from patchright.async_api import async_playwright

ITEMS_PER_PAGE = 120

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

CATEGORIES = [
    ("men", category) for category in [
        "belts-suspenders", "eyewear", "face-masks", "gloves", "hats", "jewelry",
        "keychains", "pocket-squares-tie-bars", "scarves", "socks", "ties",
        "towels", "wallets-card-holders", "watches", "bags", "jackets-coats",
        "jeans", "pants", "shirts", "shorts", "suits-blazers", "sweaters",
        "swimwear", "tops", "underwear-loungewear", "shoes"
    ]
] + [
    ("women", category) for category in [
        "bag-accessories", "belts-suspenders", "eyewear", "face-masks",
        "fine-jewelry", "gloves", "hats", "jewelry", "keychains", "scarves",
        "socks", "tech", "wallets-card-holders", "bags", "activewear", "dresses",
        "jackets-coats", "jeans", "jumpsuits", "lingerie", "pants", "shorts",
        "skirts", "sweaters", "swimwear", "tops", "shoes"
    ]
]

# Unchanged dataclasses and functions
@dataclass
class Product:
    category_key: str
    brand: str
    name: str
    sale_price: float
    original_price: float
    discount: int
    link: str
    sizes: list[str] = field(default_factory=list)

def create_product(category_key: str, item: dict, size: str | None = None) -> Product:
    price_info = item.get('priceByCountry', [{}])[0]
    sale_price_str = price_info.get('formattedLowest', {}).get('amount', '')
    original_price_str = price_info.get('formattedPrice', '')
    def parse_price(price_str: str) -> float:
        return float(price_str.replace("$", "").replace(",", "")) if price_str else 0.0
    sale_price = parse_price(sale_price_str)
    original_price = parse_price(original_price_str)
    discount = round((original_price - sale_price) / original_price * 100) if original_price > 0 else 0
    brand = (item.get('brand', {}).get('name', {}).get('en', '') or item.get('brand', '')).lower()
    return Product(
        category_key=category_key, brand=brand, name=item.get('name', {}).get('en', '') or item.get('name', ''),
        sale_price=sale_price, original_price=original_price, discount=discount,
        link=f"https://www.ssense.com/en-ca{item.get('url')}", sizes=[size] if size else []
    )

class SsenseScraper:
    def __init__(self):
        self.scraping_paused = asyncio.Event()
        self.scraping_paused.set()
        self.stats = defaultdict(int)

    def _build_url(self, gender: str, category: str, page: int = 1, size: str | None = None) -> str:
        base_url = f"https://www.ssense.com/en-ca/{gender}/{category}.json"
        params = {}
        if size: params['sizes'] = size
        if page > 1: params['page'] = page
        if not params: return base_url
        return f"{base_url}?{urllib.parse.urlencode(params)}"

    async def _solve_captcha(self, context, url: str):
        self.stats['captchas_triggered'] += 1
        logging.warning(f"CAPTCHA detected at {url}. Please solve it in the browser.")
        page = await context.new_page()
        await page.goto(url)
        while True:
            try:
                resp = await context.request.get(url)
                text = await resp.text()
                if '<!DOCTYPE html>' not in text:
                    orjson.loads(text)
                    logging.info(f"CAPTCHA solved for {url}. Resuming scrape.")
                    break
            except Exception: pass
            await asyncio.sleep(1)
        await page.close()
        self.scraping_paused.set()
      
    async def _fetch_page(self, context, url: str, category_key: str, size: str | None = None, return_metadata: bool = False):
        await self.scraping_paused.wait()
        
        for attempt in range(2):
            try:
                await self.scraping_paused.wait()
                
                resp = await context.request.get(url)
                text = await resp.text()
                
                if '<!DOCTYPE html>' in text:
                    if self.scraping_paused.is_set():
                        self.scraping_paused.clear()
                        await self._solve_captcha(context, url)
                    await self.scraping_paused.wait()
                    continue

                self.stats['requests_attempted'] += 1
                data = orjson.loads(text)
                products = [create_product(category_key, item, size=size) for item in data.get('products', [])]
                self.stats['products_found_raw'] += len(products)
                self.stats['requests_succeeded'] += 1
                
                if return_metadata:
                    total_pages = data.get('pagination_info', {}).get('totalPages', 1)
                    sizes_metadata = data.get('metadata', {}).get('sizes', [])
                    return products, total_pages, sizes_metadata
                else:
                    return products, None

            except Exception as e:
                logging.warning(f"Attempt {attempt + 1}/2 for {url} failed: {e}. Checking pause state before retry...")
                await self.scraping_paused.wait()
                await asyncio.sleep(1)

        self.stats['requests_failed'] += 1
        logging.error(f"All attempts failed for URL: {url}. Returning empty.")
        if return_metadata:
            return [], None, None
        else:
            return [], None
       
    async def scrape_all(self):
        logging.info("Starting scrape...")
        async with async_playwright() as pw:
            context = await pw.chromium.launch_persistent_context(
                tempfile.mkdtemp(), channel="chrome", headless=False, no_viewport=True
            )
            products_dict = {}
            all_page_tasks = []

            logging.info(f"Stage 1: Starting initial discovery for {len(CATEGORIES)} categories.")
            initial_tasks = [
                ( (gender, category), asyncio.create_task(self._fetch_page(
                    context, self._build_url(gender, category), f"{gender}_{category}", return_metadata=True
                ))) for gender, category in CATEGORIES
            ]
            initial_results = await asyncio.gather(*[task for _, task in initial_tasks], return_exceptions=True)

            logging.info("Stage 2: Calculating all pages from initial metadata...")
            for i, result in enumerate(initial_results):
                if isinstance(result, Exception): continue
                (gender, category), _ = initial_tasks[i]
                category_key = f"{gender}_{category}"
                products_base, total_pages_base, sizes_metadata = result

                for prod in products_base:
                    if prod.link not in products_dict:
                        products_dict[prod.link] = prod

                if not sizes_metadata:
                    for p in range(2, (total_pages_base or 1) + 1):
                        url = self._build_url(gender, category, page=p)
                        all_page_tasks.append(asyncio.create_task(self._fetch_page(context, url, category_key)))
                else:
                    for size_info in sizes_metadata:
                        size_key = size_info['key']
                        item_count = size_info['docCount']
                        total_pages_for_size = math.ceil(item_count / ITEMS_PER_PAGE)
                        
                        for p in range(1, total_pages_for_size + 1):
                            url = self._build_url(gender, category, page=p, size=size_key)
                            all_page_tasks.append(asyncio.create_task(self._fetch_page(context, url, category_key, size=size_key)))
            
            if all_page_tasks:
                logging.info(f"Stage 3: Executing {len(all_page_tasks)} total page tasks in one wave.")
                all_results = await asyncio.gather(*all_page_tasks, return_exceptions=True)
                for res in all_results:
                    if isinstance(res, Exception): continue
                    prods, _ = res
                    for prod in prods:
                        if prod.link in products_dict:
                            if prod.sizes and prod.sizes[0] not in products_dict[prod.link].sizes:
                                products_dict[prod.link].sizes.append(prod.sizes[0])
                        else:
                            products_dict[prod.link] = prod

            logging.info("Finalizing results.")
            products = list(products_dict.values())
            for p in products: p.sizes.sort()
            await context.close()

            logging.info("----------- SCRAPE COMPLETE -----------")
            logging.info(f"Requests Attempted: {self.stats['requests_attempted']}")
            logging.info(f"Requests Succeeded: {self.stats['requests_succeeded']}")
            logging.info(f"Requests Failed:    {self.stats['requests_failed']}")
            logging.info(f"CAPTCHAs Triggered: {self.stats['captchas_triggered']}")
            logging.info("---------------------------------------")
            logging.info(f"Raw Products Found: {self.stats['products_found_raw']}")
            logging.info(f"Unique Products Found: {len(products)}")
            logging.info("---------------------------------------")
            return products