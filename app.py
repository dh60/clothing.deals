import asyncio
import orjson
import os
import aiofiles
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from scraper import SsenseScraper, Product
app = FastAPI()
templates = Jinja2Templates(directory=".")
CACHE_FILE = 'products_cache.json'
all_products = []
async def run_scrape():
    global all_products
    if os.path.exists(CACHE_FILE):
        async with aiofiles.open(CACHE_FILE, 'rb') as f:
            data = await f.read()
        all_products = orjson.loads(data)
    else:
        scraped_products = await SsenseScraper().scrape_all()
        all_products = [p.__dict__ for p in scraped_products]
        async with aiofiles.open(CACHE_FILE, 'wb') as f:
            await f.write(orjson.dumps(all_products))
          
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
  
@app.get("/data")
async def get_data():
    return Response(content=orjson.dumps(all_products), media_type="application/json")
  
if __name__ == '__main__':
    import uvicorn
    asyncio.run(run_scrape())
    uvicorn.run(app, host='0.0.0.0', port=8000)