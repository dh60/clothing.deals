import asyncio
import uvloop
import orjson
import brotli
import re
from time import time
from lxml import etree
from patchright.async_api import async_playwright
from tqdm.asyncio import tqdm

# Configuration
BASE = "https://www.ssense.com/en-ca" # Not sure what happens if set to another country.
LIMIT = 200 # Concurrency limit. Default 200, might have issues above this.
DELAY = 5 # Error retry delay. Wouldn't go lower than 5.
RETRIES = 5 # Error retry count. If you're using all retries then increase delay.
BATCH = 20000 # How many fetches before creating a new page. Huge slowdowns above 30000.

async def fetch(url, page, pool, lock, nocaptcha):
    last_error = None
    for attempt in range(RETRIES):
        await nocaptcha.wait() # Only allowed to fetch if no CAPTCHA is active.
        async with pool:
            try:
                status, body = await page.evaluate("async url => { const r = await fetch(url); return [r.status, await r.text()]; }", url)
                if status == 200:
                    return body.encode()
                if status == 404:
                    return None
                if status == 403:
                    async with lock:
                        if nocaptcha.is_set():
                            nocaptcha.clear()
                            await page.bring_to_front()
                            await page.reload()
                            input("CAPTCHA detected! Solve it in the browser, then press Enter here to continue...")
                            nocaptcha.set()
                    continue
                last_error = f"status {status}"
            except Exception as e:
                last_error = f"exception: {e}"
        await asyncio.sleep(DELAY)
    tqdm.write(f"Skipping {url} after {RETRIES} retries ({last_error}).")
    return None

async def main():
    """Main function to orchestrate the scraping process."""
    start = time()
    pool = asyncio.Semaphore(LIMIT)
    lock = asyncio.Lock()
    nocaptcha = asyncio.Event()
    nocaptcha.set()
    URL_PATTERN = re.compile(r"https://www.ssense.com/[a-z]{2}-[a-z]{2}")
    NS = "http://www.sitemaps.org/schemas/sitemap/0.9"
    NS_IMAGE = "http://www.google.com/schemas/sitemap-image/1.1"

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        page = await browser.new_page()
        await page.goto(f"{BASE}/men")

        # Step 1: Fetch and save structured category data.
        print("Fetching categories...")
        sections = ["men", "women", "everything-else"]
        contents = await asyncio.gather(*[fetch(f"{BASE}/api/navigation/{s}/v2.json", page, pool, lock, nocaptcha) for s in sections])
        category_data = {s: orjson.loads(c).get("menuData", {}).get("categories", []) if c else [] for s, c in zip(sections, contents)}
        print("Saving category data...")
        with open("categories.json.br", "wb") as f:
            f.write(brotli.compress(orjson.dumps(category_data), quality=11))

        # Step 2: Fetch and parse product URLs from sitemaps.
        product_urls = []
        sitemap_xml = await fetch("https://www.ssense.com/sitemap.xml", page, pool, lock, nocaptcha)
        if sitemap_xml:
            sitemap_urls = etree.fromstring(sitemap_xml).xpath("//s:loc[contains(text(), 'sitemap_products_list')]/text()", namespaces={"s": NS})
            tasks = [fetch(url, page, pool, lock, nocaptcha) for url in sitemap_urls]
            for sitemap_content_future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Parsing Sitemaps"):
                if content := await sitemap_content_future:
                    tree = etree.fromstring(content)
                    for element in tree.xpath('//s:url[contains(s:loc, "/product/")]', namespaces={"s": NS}):
                        if loc := element.xpath('s:loc/text()', namespaces={"s": NS}):
                            url = URL_PATTERN.sub(BASE, loc[0])
                            images = element.xpath('image:image/image:loc/text()', namespaces={"image": NS_IMAGE})
                            product_urls.append((url, images))
        print(f"Found {len(product_urls)} products.")

        # Step 3: Scrape JSON data for each product URL, and put it in a list.
        async def scrape(url, images):
            """Fetches and processes a single product's JSON data."""
            try:
                p = orjson.loads(await fetch(f"{url}.json", page, pool, lock, nocaptcha))["product"]
            except (TypeError, orjson.JSONDecodeError, KeyError, asyncio.TimeoutError):
                return None
            return {
                "name": p["name"]["en"],
                "brand": p["brand"]["name"]["en"],
                "gender": p["gender"],
                "isGenderless": p["isGenderless"],
                "allCategoryIds": p["allCategoryIds"],
                "category": p["category"]["id"],
                "regular": (regular := p["price"][0]["regular"]),
                "lowest": (lowest := p["price"][0]["lowest"]["amount"]),
                "description": p["description"]["en"],
                "sizes": [v["size"]["name"] for v in p["variants"] if v["inStock"]],
                "url": url,
                "images": images,
                "discount": round(((regular - lowest) / regular) * 100) if regular > lowest else 0,
                "productCode": p["productCode"],
                "color": p["primaryColor"].get("en"),
                "composition": p["composition"]["en"],
                "country": p["countryOrigin"]["nameByLanguage"]["en"],
            }

        products = []
        with tqdm(total=len(product_urls), desc="Scraping Products") as pbar:
            for num in range(0, len(product_urls), BATCH):
                await page.close()
                page = await browser.new_page()
                await page.goto(f"{BASE}/men")
                for future in asyncio.as_completed([scrape(url, images) for url, images in product_urls[num:num+BATCH]]):
                    pbar.update(1)
                    if p := await future:
                        products.append(p)

    # Step 4: Compress all scraped data to a JSON file.
    print(f"Saving {len(products)} products (this might take a few minutes)...")
    with open("products.json.br", "wb") as f:
        f.write(brotli.compress(orjson.dumps(products), quality=11))
    print(f"Export complete. Total time: {time() - start:.2f} seconds.")

uvloop.run(main())