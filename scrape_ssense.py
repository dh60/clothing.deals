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

def sort_sizes(sizes):
    """Sort sizes intelligently: numeric sizes numerically, letter sizes by standard order."""
    size_order = {
        'XXXS': 0, 'XXS': 1, 'XS': 2, 'S': 3, 'M': 4, 'L': 5, 'XL': 6, 'XXL': 7, 'XXXL': 8, 'XXXXL': 9,
        'OS': 999, 'ONE SIZE': 999, 'O/S': 999
    }

    def size_key(size):
        size_upper = size.strip().upper()
        # Check if it's a known letter size
        if size_upper in size_order:
            return (0, size_order[size_upper], 0, '')
        # Try to extract numeric value
        match = re.search(r'(\d+\.?\d*)', size)
        if match:
            num = float(match.group(1))
            # For sizes with leading zeros (000, 00, 0), sort by string length then numeric value
            # This ensures 000 < 00 < 0 < 1 < 2
            num_str = match.group(1)
            leading_zeros = len(num_str) - len(num_str.lstrip('0')) if num > 0 or '.' not in num_str else 0
            return (1, num, -leading_zeros, size)
        # Fallback: alphabetical
        return (2, 0, 0, size)

    return sorted(sizes, key=size_key)

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

        # Step 1: Fetch category navigation to build ID -> path mapping
        print("Fetching category navigation...")
        sections = ["men", "women", "everything-else"]
        contents = await asyncio.gather(*[fetch(f"{BASE}/api/navigation/{s}/v2.json", page, pool, lock, nocaptcha) for s in sections])
        category_data = {s: orjson.loads(c).get("menuData", {}).get("categories", []) if c else [] for s, c in zip(sections, contents)}

        category_paths = {}
        def process_category(cat, path=[]):
            cat_id = str(cat["id"])
            cat_name = cat["name"].upper()
            # Normalize "SHOES" to "FOOTWEAR" to match The Last Hunt structure
            if cat_name == "SHOES":
                cat_name = "FOOTWEAR"
            current_path = path + [cat_name]
            category_paths[cat_id] = current_path
            for child in cat.get("children", []):
                process_category(child, current_path)

        for section_cats in category_data.values():
            for cat in section_cats:
                process_category(cat)

        print(f"Built category mapping for {len(category_paths)} categories")

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

            # Build category path from allCategoryIds
            all_cat_ids = p.get("allCategoryIds", [])
            category_path = category_paths.get(all_cat_ids[-1]) if all_cat_ids else []

            sizes = [v["size"]["name"] for v in p["variants"] if v["inStock"]]
            return {
                "name": p["name"]["en"],
                "brand": p["brand"]["name"]["en"],
                "gender": "other" if p["isGenderless"] else p["gender"],
                "categoryPath": category_path,
                "regular": (regular := p["price"][0]["regular"]),
                "lowest": (lowest := p["price"][0]["lowest"]["amount"]),
                "description": p["description"]["en"],
                "sizes": sort_sizes(sizes),
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
    with open("products_ssense.json.br", "wb") as f:
        f.write(brotli.compress(orjson.dumps(products), quality=11))
    print(f"Export complete. Total time: {time() - start:.2f} seconds.")

uvloop.run(main())