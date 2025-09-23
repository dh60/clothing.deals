import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse

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
    uvicorn.run(app, host='0.0.0.0', port=8000)