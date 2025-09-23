import asyncio
import orjson
import time
import re
import brotli
import logging
from typing import List, Dict, Any, Tuple, Optional, Set
from urllib.parse import urlparse
from lxml import etree
from patchright.async_api import async_playwright, APIRequestContext, Page, Error as PlaywrightError
from tqdm.asyncio import tqdm
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://www.ssense.com/en-ca/"
SITEMAP_URL = "https://www.ssense.com/sitemap.xml"
CATEGORY_API_URLS = [
    "https://www.ssense.com/en-ca/api/navigation/men/v2",
    "https://www.ssense.com/en-ca/api/navigation/women/v2",
    "https://www.ssense.com/en-ca/api/navigation/everything-else/v2"
]
PROFILE_DIR = "user_data"
OUTPUT_JSON_BR = "products.json.br"
CONCURRENCY = 500
RETRY_DELAY = 8
MAX_RETRIES = 3

ProductData = Dict[str, Any]
CategoryLookup = Dict[str, Dict[str, Any]]

NS = {'s': 'http://www.sitemaps.org/schemas/sitemap/0.9', 'image': 'http://www.google.com/schemas/sitemap-image/1.1'}
TARGET_LOCALE_PATH = urlparse(BASE_URL).path
logger = logging.getLogger(__name__)

class TqdmLoggingHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)

def setup_logging():
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    if log.hasHandlers():
        log.handlers.clear()
    log.addHandler(TqdmLoggingHandler())

def clean_size(size_name: str) -> Optional[str]:
    if not size_name:
        return None
    return size_name.strip().upper()

async def handle_captcha(api: APIRequestContext, page: Page, solver_lock: asyncio.Lock, captcha_event: asyncio.Event, sem: asyncio.Semaphore):
    if solver_lock.locked():
        await captcha_event.wait()
        return
    async with solver_lock:
        async with sem:
            check_resp = await api.get(BASE_URL)
        if check_resp.status != 200:
            logger.warning("CAPTCHA detected! Please solve it in the browser.")
            captcha_event.clear()
            await page.bring_to_front()
            await page.reload()
            captcha_title = "Access to this page has been denied"
            while True:
                try:
                    current_title = await page.title()
                    if captcha_title not in current_title:
                        logger.info("CAPTCHA appears to be solved.")
                        break
                    await asyncio.sleep(1)
                except PlaywrightError as e:
                    logger.warning(f"Error while checking page title: {e}. Retrying check.")
                    await asyncio.sleep(2)
            captcha_event.set()

async def fetch(url: str, api: APIRequestContext, page: Page, sem: asyncio.Semaphore, solver_lock: asyncio.Lock, captcha_event: asyncio.Event) -> Optional[bytes]:
    retries = 0
    while retries < MAX_RETRIES:
        await captcha_event.wait()
        async with sem:
            try:
                resp = await api.get(url)
                if resp.ok: return await resp.body()
                if resp.status == 404: return None
                if resp.status == 403:
                    await handle_captcha(api, page, solver_lock, captcha_event, sem)
                    continue
            except PlaywrightError as e:
                logger.warning(f"Network error for {url}: {e}. Retrying...")
        retries += 1
        await asyncio.sleep(RETRY_DELAY)
    logger.error(f"Skipping {url} after {MAX_RETRIES} retries.")
    return None

def parse_sitemap_sync(xml: bytes) -> List[Tuple[str, List[str]]]:
    root = etree.fromstring(xml)
    items = []
    source_locale_path = '/en-us/'
    
    for elem in root.xpath('//s:url[contains(s:loc, "/product/")]', namespaces=NS):
        try:
            url = elem.xpath('s:loc/text()', namespaces={'s': NS['s']})[0]
            url = url.replace(source_locale_path, TARGET_LOCALE_PATH)
            images = elem.xpath('image:image/image:loc/text()', namespaces={'image': NS['image']})
            items.append((url, images))
        except IndexError:
            continue
    return items

async def fetch_category_data(api: APIRequestContext, **kwargs) -> Optional[List[Dict]]:
    tasks = [fetch(url, api=api, **kwargs) for url in CATEGORY_API_URLS]
    results = await asyncio.gather(*tasks)
    all_categories = []
    for content in results:
        if content:
            try:
                data = orjson.loads(content)
                menu_data = data.get('menuData', {})
                if 'categories' in menu_data and isinstance(menu_data['categories'], list):
                    all_categories.extend(menu_data['categories'])
            except orjson.JSONDecodeError:
                logger.warning("Failed to parse a category API response.")
                continue
    return all_categories if all_categories else None

def build_category_lookup(categories: List[Dict], parent_id: Optional[str] = None) -> CategoryLookup:
    lookup = {}
    for cat in categories:
        cat_id = str(cat.get("id"))
        if cat_id:
            lookup[cat_id] = {"name": cat.get("name"), "parent_id": parent_id}
            if "children" in cat and cat.get("children"):
                lookup.update(build_category_lookup(cat["children"], cat_id))
    return lookup

def get_category_path(category_id: str, lookup: CategoryLookup) -> str:
    path = []
    current_id = str(category_id)
    while current_id in lookup:
        path.append(lookup[current_id]["name"])
        current_id = lookup[current_id].get("parent_id")
    return " → ".join(reversed(path)) if path else "Unknown"

def format_product(data_tuple: Tuple[bytes, str, List[str], CategoryLookup]) -> Optional[ProductData]:
    json_content, url, images, category_lookup = data_tuple
    try:
        data = orjson.loads(json_content)
        prod = data.get("product")
        if not prod: return None
        price_info = prod.get("price", [])
        regular_price = price_info[0].get("regular") if price_info else None
        lowest_price = price_info[0].get("lowest", {}).get("amount") if price_info else None
        category_id = str(prod.get("category", {}).get("id"))
        category_path = get_category_path(category_id, category_lookup)
        raw_sizes = [v.get("size", {}).get("name") for v in prod.get("variants", []) if v.get("inStock") and v.get("size", {}).get("name")]
        product_data = { "name": prod.get("name", {}).get("en"), "brand": prod.get("brand", {}).get("name", {}).get("en"), "gender": prod.get("gender"), "is_genderless": prod.get("isGenderless", False), "category": category_path, "regular": regular_price, "lowest": lowest_price, "description": prod.get("description", {}).get("en"), "sizes": sorted(list({clean_size(s) for s in raw_sizes if clean_size(s)})), "url": url, "images": images, "discount": round(100 * (regular_price - lowest_price) / regular_price) if regular_price and lowest_price and regular_price > lowest_price else 0, }
        return product_data
    except Exception as e:
        logger.warning(f"Failed to parse product {url}. Reason: {type(e).__name__}: {e}")
        return None

def get_existing_urls_from_json(json_file: str) -> Tuple[Set[str], List[ProductData]]:
    try:
        with open(json_file, "rb") as f:
            decompressed_data = brotli.decompress(f.read())
            existing_products = orjson.loads( decompressed_data )
        urls = {product['url'] for product in existing_products if 'url' in product}
        logger.info(f"Found {len(urls)} existing products from {json_file}.")
        return urls, existing_products
    except (FileNotFoundError, brotli.error, orjson.JSONDecodeError):
        logger.info("No existing product file found or file is invalid. Starting a full scrape.")
        return set(), []

async def parse_sitemap_urls(url: str, executor: ThreadPoolExecutor, **kwargs) -> List[Tuple[str, List[str]]]:
    content = await fetch(url, **kwargs)
    if not content:
        return []
    
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, parse_sitemap_sync, content)

async def product_consumer(product_queue: asyncio.Queue, results_queue: asyncio.Queue, pbar: tqdm, category_lookup: CategoryLookup, executor: ThreadPoolExecutor, **kwargs):
    loop = asyncio.get_running_loop()
    while True:
        item = await product_queue.get()
        if item is None: break
        url, images = item
        try:
            json_content = await fetch(f"{url}.json", **kwargs)
            if json_content:
                product = await loop.run_in_executor(
                    executor, format_product, (json_content, url, images, category_lookup)
                )
                if product:
                    await results_queue.put(product)
        finally:
            product_queue.task_done()
            if pbar: pbar.update(1)

async def results_collector(results_queue: asyncio.Queue, final_product_list: List[ProductData]):
    while True:
        product = await results_queue.get()
        if product is None: break
        final_product_list.append(product)
        results_queue.task_done()

async def main():
    setup_logging()
    start_time = time.time()
    
    sem = asyncio.Semaphore(CONCURRENCY)
    solver_lock = asyncio.Lock()
    captcha_event = asyncio.Event(); captcha_event.set()

    existing_url_set, existing_products = get_existing_urls_from_json(OUTPUT_JSON_BR)

    with ThreadPoolExecutor() as executor:
        async with async_playwright() as pw:
            ctx = await pw.chromium.launch_persistent_context(PROFILE_DIR, channel="chrome", headless=False, no_viewport=True)
            page = ctx.pages[0] if ctx.pages else await ctx.new_page()
            await page.goto(f"{BASE_URL}men")
            api = ctx.request
            shared_kwargs = {"page": page, "sem": sem, "solver_lock": solver_lock, "captcha_event": captcha_event, "api": api}

            raw_categories = None
            while not raw_categories:
                raw_categories = await fetch_category_data(**shared_kwargs)
                if not raw_categories:
                    logger.warning("Category data fetch failed. Retrying...")
                    await asyncio.sleep(2)
            category_lookup = build_category_lookup(raw_categories)

            sitemap_content = None
            while not sitemap_content:
                sitemap_content = await fetch(SITEMAP_URL, **shared_kwargs)
                if not sitemap_content:
                    logger.warning("Main sitemap fetch failed. Retrying...")
                    await asyncio.sleep(2)

            loop = asyncio.get_running_loop()
            root = await loop.run_in_executor(executor, etree.fromstring, sitemap_content)
            sitemap_urls = root.xpath("//s:loc[contains(text(), 'sitemap_products_list')]/text()", namespaces=NS)

            all_product_urls = []
            with tqdm(total=len(sitemap_urls), desc="Parsing Sitemaps", unit="sitemap") as pbar_parsing:
                parsing_tasks = [parse_sitemap_urls(url, executor, **shared_kwargs) for url in sitemap_urls]
                for future in asyncio.as_completed(parsing_tasks):
                    result = await future
                    all_product_urls.extend(result)
                    pbar_parsing.update(1)

            live_url_set = {url for url, _ in all_product_urls}
            urls_to_scrape = [(url, images) for url, images in all_product_urls if url not in existing_url_set]

            if not urls_to_scrape:
                logger.info("No new products to scrape.")
            else:
                product_queue_to_scrape = asyncio.Queue()
                for item in urls_to_scrape:
                    await product_queue_to_scrape.put(item)
                
                results_queue = asyncio.Queue(maxsize=CONCURRENCY * 2)
                final_product_list = []
                
                with tqdm(total=len(urls_to_scrape), desc="Scraping New Products", unit="item") as pbar_scraping:
                    collector_task = asyncio.create_task(results_collector(results_queue, final_product_list))
                    consumers = [
                        asyncio.create_task(product_consumer(product_queue_to_scrape, results_queue, pbar_scraping, 
                                                             category_lookup, executor, **shared_kwargs)) 
                        for _ in range(CONCURRENCY)
                    ]

                    await product_queue_to_scrape.join()

                    for _ in consumers:
                        await product_queue_to_scrape.put(None)
                    await asyncio.gather(*consumers)
                    
                    await results_queue.join()
                    await results_queue.put(None)
                    await collector_task
            
            logger.info("Filtering stale products from previous run...")
            still_live_existing_products = [p for p in existing_products if p.get('url') in live_url_set]
            final_product_list.extend(still_live_existing_products)

    logger.info(f"Scrape complete. Saving {len(final_product_list)} products to {OUTPUT_JSON_BR}...")
    with open(OUTPUT_JSON_BR, "wb") as f:
        f.write(brotli.compress(orjson.dumps(final_product_list), quality=11))
    
    logger.info(f"Export complete. Total time: {time.time() - start_time:.2f} seconds.")


if __name__ == "__main__":
    asyncio.run(main())