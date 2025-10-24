import uvicorn
import webbrowser
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
webbrowser.open('http://localhost:8000')
uvicorn.run(app, host='localhost', port=8000)