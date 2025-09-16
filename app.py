import asyncio
import orjson
import os
import aiofiles
import yaml
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from scraper.factory import ScraperFactory

app = FastAPI()
templates = Jinja2Templates(directory=".")
CACHE_DIR = 'cache'
CONFIG_FILE = 'config.yaml'
all_products = []
site_stats = {}

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            return yaml.safe_load(f)
    return {'sites': {}}

config = load_config()

async def run_scrape(site_name: str | None = None):
    global all_products, site_stats
    all_products = []
    site_stats = {}
    scrapers = {}
    
    if site_name:
        if site_name not in config['sites']:
            raise ValueError(f"Site {site_name} not found in config.")
        sites_to_scrape = [site_name]
    else:
        sites_to_scrape = list(config['sites'].keys())
    
    for site in sites_to_scrape:
        scraper_config = config['sites'][site]
        scraper_class = ScraperFactory.get(scraper_config['scraper_class'])
        scraper = scraper_class(scraper_config)
        cache_file = os.path.join(CACHE_DIR, f'products_{site}.json')
        
        if os.path.exists(cache_file):
            async with aiofiles.open(cache_file, 'rb') as f:
                data = await f.read()
            site_products = orjson.loads(data)
        else:
            site_products = await scraper.scrape_all()
            serialized_products = [p.__dict__ for p in site_products]
            async with aiofiles.open(cache_file, 'wb') as f:
                await f.write(orjson.dumps(serialized_products))
            site_products = serialized_products
        
        for prod in site_products:
            prod['site'] = site  # Add site field for filtering
        
        all_products.extend(site_products)
        site_stats[site] = scraper.get_stats()

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/data")
async def get_data():
    return Response(content=orjson.dumps(all_products), media_type="application/json")

@app.get("/refresh")
async def refresh(site: str | None = None):
    try:
        await run_scrape(site)
        return {"message": f"Scrape refreshed for {'all sites' if not site else site}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_stats():
    return site_stats

if __name__ == '__main__':
    os.makedirs(CACHE_DIR, exist_ok=True)
    import uvicorn
    asyncio.run(run_scrape())
    uvicorn.run(app, host='0.0.0.0', port=8000)