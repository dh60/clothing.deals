import sqlite3
import uvicorn
import orjson
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, Response

DB_FILE = "products.db"

app = FastAPI()

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = lambda cursor, row: {col[0]: row[idx] for idx, col in enumerate(cursor.description)}
    return conn

@app.get("/")
async def serve_index():
    return FileResponse("index.html")

@app.get("/data")
async def get_product_data(request: Request):
    query_params = request.query_params
    
    gender = query_params.get("gender", "all")
    brands_str = query_params.get("brands", "")
    
    min_discount_str = query_params.get("min_discount")
    min_discount = int(min_discount_str) if min_discount_str and min_discount_str.isdigit() else 0

    sort_by = query_params.get("sort_by", "discount_desc")
    min_price_str = query_params.get("min_price", "")
    max_price_str = query_params.get("max_price", "")
    sizes_str = query_params.get("sizes", "")
    
    base_query = """
        FROM products p
        LEFT JOIN (
            SELECT ps.product_id, GROUP_CONCAT(s.name) as available_sizes
            FROM product_sizes ps
            JOIN sizes s ON ps.size_id = s.id
            GROUP BY ps.product_id
        ) sz ON p.id = sz.product_id
    """
    conditions, params = [], []

    if gender != "all":
        conditions.append("p.gender = ?"); params.append(gender)
        conditions.append("LOWER(p.category) NOT LIKE '%baby%' AND LOWER(p.category) NOT LIKE '%kids%'")
        conditions.append("p.is_genderless = 0")
    if min_discount > 0:
        conditions.append("p.discount >= ?"); params.append(min_discount)
    if min_price_str.isdigit():
        conditions.append("p.lowest >= ?"); params.append(float(min_price_str))
    if max_price_str.isdigit():
        conditions.append("p.lowest <= ?"); params.append(float(max_price_str))
    
    if brands_str:
        brands = [b.strip().lower() for b in brands_str.split(',') if b.strip()]
        if brands:
            placeholders = ','.join('?' for _ in brands)
            conditions.append(f"LOWER(p.brand) IN ({placeholders})"); params.extend(brands)

    if sizes_str:
        sizes = [s.strip().upper() for s in sizes_str.split(',') if s.strip()]
        if sizes:
            placeholders = ','.join('?' for _ in sizes)
            subquery = f"""
                EXISTS (
                    SELECT 1 FROM product_sizes ps_filter
                    JOIN sizes s_filter ON ps_filter.size_id = s_filter.id
                    WHERE ps_filter.product_id = p.id AND s_filter.name IN ({placeholders})
                )
            """
            conditions.append(subquery)
            params.extend(sizes)

    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)

    order_clause = " ORDER BY p.discount DESC"
    if sort_by == "price_asc":
        order_clause = " ORDER BY p.lowest ASC"

    with get_db_connection() as conn:
        full_query = "SELECT p.*, sz.available_sizes " + base_query + order_clause
        products_raw = conn.execute(full_query, params).fetchall()

    products = []
    for p in products_raw:
        p['sizes'] = p['available_sizes'].split(',') if p['available_sizes'] else []
        del p['available_sizes']
        products.append(p)
        
    return Response(content=orjson.dumps(products), media_type="application/json")

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)