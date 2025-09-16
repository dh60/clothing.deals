import asyncio
import orjson
import aiofiles
from fastapi import FastAPI
from fastapi.responses import FileResponse, Response
from ssense import SsenseScraper, Product

app = FastAPI()
CACHE_FILE = 'products_cache.json'
all_products = []

async def run_scrape():
    global all_products
    scraped_products = await SsenseScraper().scrape_all()
    all_products = [p.__dict__ for p in scraped_products]
    async with aiofiles.open(CACHE_FILE, 'wb') as f:
        await f.write(orjson.dumps(all_products))

@app.get("/")
async def index():
    return FileResponse("index.html")

@app.get("/data")
async def get_data():
    return Response(content=orjson.dumps(all_products), media_type="application/json")

if __name__ == '__main__':
    import uvicorn
    asyncio.run(run_scrape())
    uvicorn.run(app, host='0.0.0.0', port=8000)