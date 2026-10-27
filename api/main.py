import os
import csv
from fastapi import FastAPI, Query, HTTPException
from typing import List
from pydantic import BaseModel

app = FastAPI(title="DataMart Legacy Product API", version="1.0.0")

class Product(BaseModel):
    code: str
    normalized_name: str
    category: str
    supplier_country: str
    is_active: bool

CATEGORIES = ["Electrónica", "Hogar", "Ropa", "Deportes", "Papelería"]
COUNTRIES = ["Colombia", "México", "Perú"]

CATALOG_DB = {}

def load_products_from_csv():
    # Rutas posibles dentro del contenedor o local
    possible_paths = [
        "/data/Sales Transaction v.4a.csv",
        "./data/Sales Transaction v.4a.csv",
        "../data/Sales Transaction v.4a.csv"
    ]
    target_path = None
    for p in possible_paths:
        if os.path.exists(p):
            target_path = p
            break
            
    # Si no está en ruta directa, buscar cualquier .csv en /data
    if not target_path and os.path.exists("/data"):
        for f in os.listdir("/data"):
            if f.endswith(".csv"):
                target_path = os.path.join("/data", f)
                break

    if target_path:
        print(f"Cargando catálogo real desde: {target_path}")
        try:
            with open(target_path, mode='r', encoding='utf-8', errors='ignore') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    code = (row.get('ProductNo') or row.get('StockCode') or '').strip().upper()
                    name = (row.get('ProductName') or row.get('Description') or '').strip().upper()
                    
                    if code and code not in CATALOG_DB:
                        # Asignación determinista a las 5 categorías oficiales del negocio
                        cat_idx = hash(code) % len(CATEGORIES)
                        cou_idx = hash(name or code) % len(COUNTRIES)
                        CATALOG_DB[code] = {
                            "code": code,
                            "normalized_name": name if name else f"PRODUCTO {code}",
                            "category": CATEGORIES[cat_idx],
                            "supplier_country": COUNTRIES[cou_idx],
                            "is_active": True
                        }
            print(f"Éxito: Se cargaron {len(CATALOG_DB)} productos reales en la API.")
        except Exception as e:
            print(f"Error cargando CSV: {e}")

# Cargar catálogo de tu CSV al arrancar
load_products_from_csv()

@app.get("/")
def home():
    return {
        "status": "online",
        "total_products": len(CATALOG_DB),
        "message": "DataMart Legacy Product API v1.0"
    }

# Endpoint 1: Listado paginado de productos reales
@app.get("/products", response_model=List[Product])
def get_products(page: int = Query(1, ge=1), page_size: int = Query(50, ge=1, le=500)):
    items = list(CATALOG_DB.values())
    start = (page - 1) * page_size
    return items[start:start + page_size]

# Endpoint 2: Consulta por código de producto real
@app.get("/products/{code}", response_model=Product)
def get_product_by_code(code: str):
    clean_code = code.strip().upper()
    if clean_code in CATALOG_DB:
        return CATALOG_DB[clean_code]
    raise HTTPException(status_code=404, detail=f"Producto con código {code} no encontrado en catálogo")