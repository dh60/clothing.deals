import asyncio
import orjson
import brotli
import re
from time import time
from lxml import etree
from patchright.async_api import async_playwright
from tqdm.asyncio import tqdm

# Configuration
BASE = "https://www.ssense.com/en-ca/" # Not sure what happens if set to another country, there are "en" keys grabbed when scraping.
LIMIT = 500 # Concurrency limit, diminishing returns above 500.
DELAY = 5 # Wouldn't go lower than 5.
RETRIES = 3
NAMESPACE = {'s': 'http://www.sitemaps.org/schemas/sitemap/0.9', 'image': 'http://www.google.com/schemas/sitemap-image/1.1'}

async def fetch(url, browser, page, pool, lock, captcha):
    """Asynchronously fetches a URL, handling retries and CAPTCHA."""
    for attempt in range(RETRIES):
        await captcha.wait()
        async with pool:
            try:
                response = await browser.request.get(url)
                if response.ok:
                    return await response.body()
                if response.status == 404:
                    return None
                if response.status == 403:
                    if lock.locked():
                        await captcha.wait()
                    else:
                        async with lock:
                            tqdm.write("CAPTCHA detected! Please solve it in the browser.")
                            captcha.clear()
                            await page.bring_to_front()
                            await page.reload()
                            while "Access to this page has been denied" in await page.title():
                                await asyncio.sleep(1)
                            captcha.set()
                    continue
                tqdm.write(f"{response.status} for {url}, Attempt {attempt + 1}/3")
            except Exception as e:
                tqdm.write(f"Connection error for {url}, Attempt {attempt + 1}/3")
        await asyncio.sleep(DELAY)
    tqdm.write(f"Skipping {url} after {RETRIES} retries.")
    return None

async def main():
    """Main function to orchestrate the scraping process."""
    start = time()
    pool = asyncio.Semaphore(LIMIT)
    lock = asyncio.Lock()
    captcha = asyncio.Event()
    captcha.set()

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch_persistent_context(user_data_dir="Chrome", channel="chrome", headless=False, no_viewport=True)
        page = browser.pages[0]
        await page.goto(f"{BASE}men")
        await asyncio.sleep(1)
        
        # Step 1: Fetch and save structured category data.
        print("Fetching categories...")
        section = ["men", "women", "everything-else"]
        contents = await asyncio.gather(*[fetch(f"{BASE}api/navigation/{s}/v2.json", browser, page, pool, lock, captcha) for s in section])
        category_data = {s: orjson.loads(c).get('menuData', {}).get('categories', []) if c else [] for s, c in zip(section, contents)}
        print("Saving category data...")
        with open('categories.json.br', "wb") as f:
            f.write(brotli.compress(orjson.dumps(category_data), quality=11))
        
        # Step 2: Fetch and parse product URLs from sitemaps.
        product_urls = []
        sitemap_xml = await fetch("https://www.ssense.com/sitemap.xml", browser, page, pool, lock, captcha)
        if sitemap_xml:
            sitemap_urls = etree.fromstring(sitemap_xml).xpath("//s:loc[contains(text(), 'sitemap_products_list')]/text()", namespaces=NAMESPACE)
            tasks = [fetch(url, browser, page, pool, lock, captcha) for url in sitemap_urls]
            for sitemap_content_future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Parsing Sitemaps"):
                if content := await sitemap_content_future:
                    tree = etree.fromstring(content)
                    for element in tree.xpath('//s:url[contains(s:loc, "/product/")]', namespaces=NAMESPACE):
                        loc = element.xpath('s:loc/text()', namespaces=NAMESPACE)
                        if not loc:
                            continue
                        url = re.sub(r'https://www.ssense.com/[a-z]{2}-[a-z]{2}/', BASE, loc[0])
                        images = element.xpath('image:image/image:loc/text()', namespaces={'image': NAMESPACE['image']})
                        product_urls.append((url, images))
        print(f"Found {len(product_urls)} products.")
        queue = {url: (url, images) for url, images in product_urls}
        
        # Step 3: Scrape JSON data for each product URL, and put it in a list.
        async def scrape(url, images):
            """Fetches and processes a single product's JSON data."""
            json_url = f"{url}.json"
            for _ in range(RETRIES):
                content = await fetch(json_url, browser, page, pool, lock, captcha)
                if content is None:
                    return None
                data = orjson.loads(content)
                if "product" in data:
                    p = data["product"]
                    try:
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
                    except Exception as e:
                        tqdm.write(f"Data parsing error for {json_url}: {e}")
                await asyncio.sleep(1)
            return None

        products = []
        scrape_tasks = [scrape(url, images) for url, images in queue.values()]
        for future in tqdm(asyncio.as_completed(scrape_tasks), total=len(scrape_tasks), desc="Scraping Products"):
            if product := await future:
                products.append(product)

    # Step 4: Compress all scraped data to a JSON file.
    tqdm.write(f"Saving {len(products)} products (this might take a few minutes)...")
    with open('products.json.br', "wb") as f:
        f.write(brotli.compress(orjson.dumps(products), quality=11))
    print(f"Export complete. Total time: {time() - start:.2f} seconds.")

if __name__ == "__main__":
    asyncio.run(main())