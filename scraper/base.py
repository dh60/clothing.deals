import asyncio
import orjson
import tempfile
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

class BaseScraper:
    def __init__(self, config):
        self.config = config
        self.categories = config.get('categories', [])
        self.base_url_template = config.get('base_url_template', '')
        self.items_per_page = config.get('items_per_page', ITEMS_PER_PAGE)
        self.products_key = 'products'
        self.captcha_enabled = True
        self.scraping_paused = asyncio.Event()
        self.scraping_paused.set()
        self.stats = defaultdict(int)

    def _build_url(self, gender: str, category: str, page: int = 1, size: str | None = None) -> str:
        raise NotImplementedError("Subclasses must implement _build_url")

    def _parse_product(self, category_key: str, item: dict, size: str | None = None) -> Product:
        raise NotImplementedError("Subclasses must implement _parse_product")

    def _parse_metadata(self, data: dict) -> tuple[int, list]:
        raise NotImplementedError("Subclasses must implement _parse_metadata")

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
            except Exception:
                pass
            await asyncio.sleep(1)
        await page.close()
        self.scraping_paused.set()

    async def _fetch_json(self, context, url: str) -> dict | None:
        await self.scraping_paused.wait()
        for attempt in range(2):
            try:
                await self.scraping_paused.wait()
                resp = await context.request.get(url)
                text = await resp.text()
                if self.captcha_enabled and '<!DOCTYPE html>' in text:
                    if self.scraping_paused.is_set():
                        self.scraping_paused.clear()
                        await self._solve_captcha(context, url)
                    await self.scraping_paused.wait()
                    continue
                self.stats['requests_attempted'] += 1
                data = orjson.loads(text)
                self.stats['requests_succeeded'] += 1
                return data
            except Exception as e:
                logging.warning(f"Attempt {attempt + 1}/2 for {url} failed: {e}. Checking pause state before retry...")
                await self.scraping_paused.wait()
                await asyncio.sleep(1)
        self.stats['requests_failed'] += 1
        logging.error(f"All attempts failed for URL: {url}. Returning None.")
        return None

    async def _fetch_page(self, context, url: str, category_key: str, size: str | None = None, return_metadata: bool = False):
        if return_metadata:
            metadata_url = self._build_metadata_url(category_key) if hasattr(self, '_build_metadata_url') else url
            data = await self._fetch_json(context, metadata_url)
            if data is None:
                return [], 1, []
            total_pages, sizes_metadata = self._parse_metadata(data)
            if metadata_url == url:
                products_data = data
            else:
                products_data = await self._fetch_json(context, url)
                if products_data is None:
                    products_data = {}
            products = [p for p in (self._parse_product(category_key, item, size=size) for item in products_data.get(self.products_key, [])) if p is not None]
            self.stats['products_found_raw'] += len(products)
            return products, total_pages, sizes_metadata
        else:
            data = await self._fetch_json(context, url)
            if data is None:
                return [], None
            products = [p for p in (self._parse_product(category_key, item, size=size) for item in data.get(self.products_key, [])) if p is not None]
            self.stats['products_found_raw'] += len(products)
            return products, None

    async def scrape_all(self):
        logging.info(f"Starting scrape for {self.config['name']}...")
        async with async_playwright() as pw:
            context = await pw.chromium.launch_persistent_context(
                tempfile.mkdtemp(), channel="chrome", headless=False, no_viewport=True
            )
            products_dict = {}
            all_page_tasks = []
            logging.info(f"Stage 1: Starting initial discovery for {len(self.categories)} categories.")
            initial_tasks = [
                ((gender, category), asyncio.create_task(self._fetch_page(
                    context, self._build_url(gender, category), f"{gender}_{category}", return_metadata=True
                ))) for gender, category in self.categories
            ]
            initial_results = await asyncio.gather(*[task for _, task in initial_tasks], return_exceptions=True)
            logging.info("Stage 2: Calculating all pages from initial metadata...")
            for i, result in enumerate(initial_results):
                if isinstance(result, Exception):
                    logging.warning(f"Initial fetch failed for {initial_tasks[i][0]}: {result}")
                    continue
                (gender, category), _ = initial_tasks[i]
                category_key = f"{gender}_{category}"
                products_base, total_pages_base, sizes_metadata = result
                for prod in products_base:
                    if prod and prod.link not in products_dict:
                        products_dict[prod.link] = prod
                    elif prod is None:
                        logging.warning(f"Skipping None product in initial fetch for {category_key}")
                for p in range(2, (total_pages_base or 1) + 1):
                    url = self._build_url(gender, category, page=p)
                    all_page_tasks.append(asyncio.create_task(self._fetch_page(context, url, category_key)))
                if sizes_metadata:
                    for size_info in sizes_metadata:
                        size_key = size_info['key']
                        item_count = size_info['docCount']
                        total_pages_for_size = math.ceil(item_count / self.items_per_page)
                        for p in range(1, total_pages_for_size + 1):
                            url = self._build_url(gender, category, page=p, size=size_key)
                            all_page_tasks.append(asyncio.create_task(self._fetch_page(context, url, category_key, size=size_key)))
            logging.info(f"Stage 3: Executing {len(all_page_tasks)} total page tasks in one wave.")
            if all_page_tasks:
                all_results = await asyncio.gather(*all_page_tasks, return_exceptions=True)
                for res in all_results:
                    if isinstance(res, Exception):
                        logging.warning(f"Page fetch failed: {res}")
                        continue
                    prods, _ = res
                    for prod in prods:
                        if prod:
                            if prod.link in products_dict:
                                if prod.sizes and prod.sizes[0] not in products_dict[prod.link].sizes:
                                    products_dict[prod.link].sizes.append(prod.sizes[0])
                            else:
                                products_dict[prod.link] = prod
                        else:
                            logging.warning(f"Skipping None product in page fetch for {self.config['name']}")
            else:
                logging.info("No additional pages to scrape.")
            logging.info("Finalizing results.")
            products = list(products_dict.values())
            for p in products:
                p.sizes.sort()
            await context.close()
            logging.info(f"----------- SCRAPE COMPLETE FOR {self.config['name']} -----------")
            logging.info(f"Requests Attempted: {self.stats['requests_attempted']}")
            logging.info(f"Requests Succeeded: {self.stats['requests_succeeded']}")
            logging.info(f"Requests Failed: {self.stats['requests_failed']}")
            logging.info(f"CAPTCHAs Triggered: {self.stats['captchas_triggered']}")
            logging.info("---------------------------------------")
            logging.info(f"Raw Products Found: {self.stats['products_found_raw']}")
            logging.info(f"Unique Products Found: {len(products)}")
            logging.info("---------------------------------------")
            return products

    def get_stats(self):
        return dict(self.stats)