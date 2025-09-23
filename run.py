import uvicorn
import asyncio
import ssense 
from fastapi import FastAPI
from fastapi.responses import FileResponse

def run_scraper():
    try:
        asyncio.run(ssense.main())
    except Exception as e:
        print(f"A critical error occurred during scraping: {e}")
        input("Scraping failed. Press Enter to exit.")
        exit(1)

app = FastAPI()

@app.get("/")
async def serve_index():
    return FileResponse("index.html")

@app.get("/products.json.br")
async def serve_product_data():
    return FileResponse(
        "products.json.br",
        media_type="application/json",
        headers={"Content-Encoding": "br"}
    )

if __name__ == '__main__':
    run_scraper()
    print("--- Starting the web server at http://127.0.0.1:8000 ---")
    print("Open your web browser to that address to view the app.")
    uvicorn.run(app, host='127.0.0.1', port=8000)