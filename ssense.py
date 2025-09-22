import asyncio
import orjson
import time
import sqlite3
import re
from typing import List, Dict, Any, Tuple, Optional
from lxml import etree
from patchright.async_api import async_playwright, APIRequestContext, Page, BrowserContext, Error as PlaywrightError
from tqdm.asyncio import tqdm

BASE_URL = "https://www.ssense.com/en-ca/"
SITEMAP_URL = "https://www.ssense.com/sitemap.xml"
PROFILE_DIR = "user_data"
DB_FILE = "products.db"
CONCURRENCY = 500
RETRY_DELAY = 6
MAX_RETRIES = 3

ProductData = Dict[str, Any]

def init_db():
    """Initializes a normalized three-table schema for maximum performance."""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS product_sizes")
        cursor.execute("DROP TABLE IF EXISTS sizes")
        cursor.execute("DROP TABLE IF EXISTS products")
        
        cursor.execute("""
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                url TEXT UNIQUE NOT NULL, name TEXT, brand TEXT, gender TEXT, category TEXT,
                regular REAL, lowest REAL, description TEXT, images TEXT, discount INTEGER,
                is_genderless INTEGER NOT NULL DEFAULT 0
            )""")
        
        cursor.execute("""
            CREATE TABLE sizes (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            )""")

        cursor.execute("""
            CREATE TABLE product_sizes (
                product_id INTEGER NOT NULL,
                size_id INTEGER NOT NULL,
                FOREIGN KEY (product_id) REFERENCES products (id) ON DELETE CASCADE,
                FOREIGN KEY (size_id) REFERENCES sizes (id) ON DELETE CASCADE,
                PRIMARY KEY (product_id, size_id)
            )""")

        cursor.execute("CREATE INDEX idx_product_brand ON products (brand)")
        cursor.execute("CREATE INDEX idx_product_gender ON products (gender)")
        cursor.execute("CREATE INDEX idx_product_lowest ON products (lowest)")
        cursor.execute("CREATE INDEX idx_product_discount ON products (discount)")
        cursor.execute("CREATE INDEX idx_size_name ON sizes (name)")
        conn.commit()

def get_product_count() -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            return conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    except sqlite3.Error:
        return 0

NS = {'s': 'http://www.sitemaps.org/schemas/sitemap/0.9', 'image': 'http://www.google.com/schemas/sitemap-image/1.1'}

def clean_size(size_name: str) -> Optional[str]:
    """
    Cleans a size string.
    - If it's purely letters (e.g., "XL"), return as is.
    - If it contains numbers (e.g., "US WAIST 32"), return only the first number found.
    """
    if not size_name:
        return None
    if size_name.isalpha():
        return size_name.upper()
    match = re.search(r'\d+', size_name)
    if match:
        return match.group(0)
    return size_name.upper()

async def handle_captcha(api: APIRequestContext, page: Page, solver_lock: asyncio.Lock, captcha_event: asyncio.Event, sem: asyncio.Semaphore):
    if solver_lock.locked():
        await captcha_event.wait()
        return
    async with solver_lock:
        async with sem:
            check_resp = await api.get(BASE_URL)
        if check_resp.status != 403:
            return
        captcha_event.clear()
        tqdm.write("\n--- CAPTCHA DETECTED ---")
        await page.bring_to_front()
        await page.reload()
        input("Please solve the CAPTCHA and press Enter.")
        captcha_event.set()
        tqdm.write("CAPTCHA solved. Resuming...")

async def fetch(url: str, api: APIRequestContext, page: Page, sem: asyncio.Semaphore, solver_lock: asyncio.Lock, captcha_event: asyncio.Event) -> Optional[bytes]:
    retries = 0
    while retries < MAX_RETRIES:
        await captcha_event.wait()
        async with sem:
            try:
                resp = await api.get(url)
                if resp.ok: return await resp.body()

                if resp.status == 404:
                    tqdm.write(f"HTTP 404 Not Found for {url}. Skipping.")
                    return None

                if resp.status == 403:
                    await handle_captcha(api, page, solver_lock, captcha_event, sem)
                    continue
                tqdm.write(f"HTTP {resp.status} for {url}. Retrying...")
            except PlaywrightError as e:
                tqdm.write(f"Network error for {url}: {e}. Retrying...")
        retries += 1
        await asyncio.sleep(RETRY_DELAY)
    tqdm.write(f"GIVING UP on {url} after {MAX_RETRIES} retries.")
    return None

def parse_product_sitemap(xml: bytes) -> List[Tuple[str, List[str]]]:
    root = etree.fromstring(xml)
    items = []
    for elem in root.xpath('//s:url[contains(s:loc, "/product/")]', namespaces=NS):
        try:
            url = elem.xpath('s:loc/text()', namespaces={'s': NS['s']})[0]
            images = elem.xpath('image:image/image:loc/text()', namespaces={'image': NS['image']})
            items.append((url.replace('/en-us/', '/en-ca/'), images))
        except IndexError:
            continue
    return items

def format_product(data: Dict, url: str, images: List[str]) -> Optional[ProductData]:
    """Formats the product and returns sizes as a Python list."""
    try:
        prod = data["product"]
        regular_price = prod.get("price", [{}])[0].get("regular")
        lowest_price = prod.get("price", [{}])[0].get("lowest", {}).get("amount")
        is_genderless = prod.get("isGenderless", False) # Extract isGenderless
        
        raw_sizes = [v.get("size", {}).get("name") for v in prod.get("variants", []) if v.get("inStock") and v.get("size", {}).get("name")]
        cleaned_sizes = {clean_size(s) for s in raw_sizes if clean_size(s)}
        available_sizes = sorted(list(cleaned_sizes))

        return {
            "name": prod["name"]["en"], "brand": prod["brand"]["name"]["en"],
            "gender": prod.get("gender"), "category": prod.get("category", {}).get("name", {}).get("en"),
            "regular": regular_price, "lowest": lowest_price,
            "description": prod.get("description", {}).get("en"),
            "sizes": available_sizes,
            "url": url, "images": orjson.dumps(images).decode(),
            "discount": round(100 * (regular_price - lowest_price) / regular_price) if regular_price and lowest_price and regular_price > lowest_price else 0,
            "is_genderless": is_genderless, # Add to dictionary
        }
    except (KeyError, IndexError, TypeError):
        return None

async def sitemap_producer(url: str, product_queue: asyncio.Queue, **kwargs):
    content = await fetch(url, **kwargs)
    if content:
        loop = asyncio.get_running_loop()
        items = await loop.run_in_executor(None, parse_product_sitemap, content)
        for item in items:
            await product_queue.put(item)

async def product_consumer(product_queue: asyncio.Queue, results_queue: asyncio.Queue, pbar: tqdm, **kwargs):
    while True:
        item = await product_queue.get()
        if item is None: break
        url, images = item
        try:
            json_content = await fetch(f"{url}.json", **kwargs)
            if json_content:
                product = format_product(orjson.loads(json_content), url, images)
                if product:
                    await results_queue.put(product)
        finally:
            product_queue.task_done()
            if pbar: pbar.update(1)

async def db_writer(results_queue: asyncio.Queue):
    conn = sqlite3.connect(DB_FILE, isolation_level=None)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("PRAGMA synchronous = NORMAL;")
    size_cache = {name: id for id, name in cursor.execute("SELECT id, name FROM sizes").fetchall()}
    while True:
        product = await results_queue.get()
        if product is None: break
        product_sizes = product.pop("sizes")
        try:
            cursor.execute("BEGIN")
            cursor.execute("""
                INSERT OR REPLACE INTO products (url, name, brand, gender, category, regular, lowest, description, images, discount, is_genderless)
                VALUES (:url, :name, :brand, :gender, :category, :regular, :lowest, :description, :images, :discount, :is_genderless)
            """, product)
            product_id = cursor.lastrowid
            
            size_ids_to_link = []
            for size_name in set(product_sizes):
                if size_name not in size_cache:
                    cursor.execute("INSERT OR IGNORE INTO sizes (name) VALUES (?)", (size_name,))
                    size_id = cursor.lastrowid
                    if size_id != 0: size_cache[size_name] = size_id
                if size_name in size_cache:
                    size_ids_to_link.append(size_cache[size_name])

            if size_ids_to_link:
                cursor.execute("DELETE FROM product_sizes WHERE product_id = ?", (product_id,))
                cursor.executemany("INSERT INTO product_sizes (product_id, size_id) VALUES (?, ?)", 
                                   [(product_id, sid) for sid in size_ids_to_link])
            cursor.execute("COMMIT")
        except sqlite3.Error as e:
            cursor.execute("ROLLBACK")
            tqdm.write(f"DB Error for {product.get('url', 'N/A')}: {e}")
        finally:
            results_queue.task_done()
    conn.close()

async def scrape():
    start_time = time.time()
    init_db()
    
    product_queue = asyncio.Queue()
    results_queue = asyncio.Queue(maxsize=CONCURRENCY * 2)
    sem = asyncio.Semaphore(CONCURRENCY)
    solver_lock = asyncio.Lock()
    captcha_event = asyncio.Event(); captcha_event.set()

    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(PROFILE_DIR, channel="chrome", headless=False, no_viewport=True)
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        await page.goto(BASE_URL)
        api = ctx.request

        shared_args = {"api": api, "page": page, "sem": sem, "solver_lock": solver_lock, "captcha_event": captcha_event}

        print("Discovering and parsing product URLs from sitemaps.")
        sitemap_content = await fetch(SITEMAP_URL, **shared_args)
        if not sitemap_content:
            print("CRITICAL: Main sitemap not found."); return

        root = etree.fromstring(sitemap_content)
        sitemap_urls = root.xpath("//s:loc[contains(text(), 'sitemap_products_list')]/text()", namespaces={'s': NS['s']})

        producers = [
            asyncio.create_task(sitemap_producer(url, product_queue, **shared_args))
            for url in sitemap_urls
        ]
        await asyncio.gather(*producers)
        
        print(f"✓ Sitemap discovery and parsing finished in {time.time() - start_time:.2f} seconds.")
        print(f"Found {product_queue.qsize()} total products. Starting concurrent scrape...")

        pbar = tqdm(total=product_queue.qsize(), desc="Scraping Products", unit="item")
        consumers = [asyncio.create_task(product_consumer(product_queue, results_queue, pbar, **shared_args)) for _ in range(CONCURRENCY)]
        db_writer_task = asyncio.create_task(db_writer(results_queue))

        await product_queue.join()
        await results_queue.join()

        for _ in consumers: await product_queue.put(None)
        await results_queue.put(None)
        await asyncio.gather(*consumers, db_writer_task)
        
        pbar.close()

    print(f"Saved {get_product_count()} products to {DB_FILE} in {time.time() - start_time:.2f} seconds.")

if __name__ == "__main__":
    asyncio.run(scrape())