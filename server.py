import uvicorn
import webbrowser
from fastapi import FastAPI
from fastapi.responses import FileResponse
app = FastAPI()
@app.get("/")
async def serve_index():
    return FileResponse("index.html")
@app.get("/{filename}.json.br")
async def serve_products(filename: str):
    return FileResponse(
        f"{filename}.json.br",
        media_type="application/json",
        headers={"Content-Encoding": "br"}
    )
webbrowser.open('http://localhost:8000')
uvicorn.run(app, host='localhost', port=8000)