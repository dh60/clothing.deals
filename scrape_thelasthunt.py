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
BASE = "https://www.thelasthunt.com"
LIMIT = 200
DELAY = 5
RETRIES = 5
BATCH = 20000

async def fetch(url, page, pool, lock, nocaptcha):
    last_error = None
    for attempt in range(RETRIES):
        await nocaptcha.wait()
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
    NS = "http://www.sitemaps.org/schemas/sitemap/0.9"

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        page = await browser.new_page()
        await page.goto(f"{BASE}/en-CA")

        # Step 1: Extract Build ID from page source
        print("Extracting Build ID...")
        page_content = await page.content()
        build_id = re.search(r'"buildId":"([^"]+)"', page_content).group(1)
        print(f"Build ID: {build_id}")

        # Step 2: Fetch product URLs from sitemaps
        product_urls = []
        print("Fetching product sitemap index...")
        sitemap_index = await fetch(f"{BASE}/sitemap/index.xml", page, pool, lock, nocaptcha)

        if sitemap_index:
            tree = etree.fromstring(sitemap_index)
            sitemap_urls = tree.xpath("//s:loc[contains(text(), 'products-')]/text()", namespaces={"s": NS})
            print(f"Found {len(sitemap_urls)} product sitemaps")

            tasks = [fetch(url, page, pool, lock, nocaptcha) for url in sitemap_urls]
            for sitemap_content_future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Parsing Product Sitemaps"):
                if content := await sitemap_content_future:
                    sitemap_tree = etree.fromstring(content)
                    for loc in sitemap_tree.xpath('//s:loc/text()', namespaces={"s": NS}):
                        if '/p/' in loc:
                            slug_match = re.search(r'/p/([^/]+)', loc)
                            if slug_match:
                                product_urls.append(slug_match.group(1))

        print(f"Found {len(product_urls)} products.")

        # Step 3: Scrape JSON data for each product
        async def scrape(slug):
            """Fetches and processes a single product's JSON data."""
            try:
                content = await fetch(f"{BASE}/_next/data/{build_id}/en-CA/p/{slug}.json", page, pool, lock, nocaptcha)
                p = orjson.loads(content)["pageProps"]["dehydratedState"]["queries"][0]["state"]["data"]
                attrs = {attr["name"]: attr["value"] for attr in p["masterVariant"]["attributesRaw"]}

                # Skip non-adult products (kids/youth)
                age = attrs.get("age")
                if age and age[0]["key"] != "adult":
                    return None

                # Extract gender
                gender = attrs["gender"][0]["key"]

                # Extract prices
                price = p["masterVariant"]["price"]
                regular = round(price["value"]["centAmount"] / 100)
                lowest = round(price["discounted"]["value"]["centAmount"] / 100) if price.get("discounted") else regular

                # Check if master variant is in stock
                master_availability = p["masterVariant"].get("availability", {})
                channels = master_availability.get("channels", {}).get("results", [])
                if not any(ch.get("availability", {}).get("isOnStock", False) for ch in channels):
                    return None

                # Extract sizes
                sizes = []
                for variant in [p["masterVariant"]] + p["variants"]:
                    variant_attrs = {attr["name"]: attr["value"] for attr in variant["attributesRaw"]}
                    size = variant_attrs["size"]
                    if size not in sizes:
                        sizes.append(size)

                # Extract category path from breadcrumbs
                breadcrumbs = orjson.loads(attrs["breadcrumbs"]) if isinstance(attrs["breadcrumbs"], str) else attrs["breadcrumbs"]
                category_path = []
                for bc in breadcrumbs:
                    if bc["node_key"] != "home":
                        for name_obj in bc["name"]:
                            if name_obj["locale"] == "en_CA":
                                category_path.append(name_obj["value"].upper())
                                break

                # Extract images
                images = [asset["sources"][0]["uri"] for asset in p["masterVariant"]["assets"] if asset["sources"]]

                return {
                    "name": p["name"],
                    "brand": attrs["brand_name"],
                    "gender": gender,
                    "categoryPath": category_path,
                    "regular": regular,
                    "lowest": lowest,
                    "description": p["description"],
                    "sizes": sort_sizes(sizes),
                    "url": f"{BASE}/p/{slug}",
                    "images": images,
                    "discount": round(((regular - lowest) / regular) * 100) if regular > lowest else 0,
                    "productCode": p["key"],
                    "color": attrs.get("color", ""),
                    "composition": attrs.get("material", ""),
                    "country": ""
                }

            except (TypeError, orjson.JSONDecodeError, KeyError, IndexError):
                return None

        products = []
        with tqdm(total=len(product_urls), desc="Scraping Products") as pbar:
            for num in range(0, len(product_urls), BATCH):
                await page.close()
                page = await browser.new_page()
                await page.goto(f"{BASE}/en-CA")
                for future in asyncio.as_completed([scrape(slug) for slug in product_urls[num:num+BATCH]]):
                    pbar.update(1)
                    if p := await future:
                        products.append(p)

    # Step 4: Compress all scraped data to a JSON file
    print(f"Saving {len(products)} products (this might take a few minutes)...")
    with open("thelasthunt_products.json.br", "wb") as f:
        f.write(brotli.compress(orjson.dumps(products), quality=11))
    print(f"Export complete. Total time: {time() - start:.2f} seconds.")

uvloop.run(main())
