import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse

app = FastAPI()

@app.get("/")
async def serve_index():
    return FileResponse("index.html")

@app.get("/categories.json.br")
async def serve_category_data():
    return FileResponse(
        "categories.json.br",
        media_type="application/json",
        headers={"Content-Encoding": "br"}
    )

@app.get("/products.json.br")
async def serve_product_data():
    return FileResponse(
        "products.json.br",
        media_type="application/json",
        headers={"Content-Encoding": "br"}
    )

if __name__ == '__main__':
    print("--- Starting the web server at http://127.0.0.1:8000 ---")
    print("Open your web browser to that address to view the app.")
    uvicorn.run(app, host='127.0.0.1', port=8000)