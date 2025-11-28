import json
import random
import time
from typing import List, Optional
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

app = FastAPI()

# Модель данных для запроса
class UserRequest(BaseModel):
    user_id: str

# Загружаем продукты при старте приложения
PRODUCTS_FILE = "psb_products_updated.json"
PRODUCTS_DB = []

try:
    with open(PRODUCTS_FILE, "r", encoding="utf-8") as f:
        PRODUCTS_DB = json.load(f)
    print(f"Загружено {len(PRODUCTS_DB)} банковских продуктов.")
except Exception as e:
    print(f"Ошибка при загрузке продуктов: {e}")
    # Фолбэк на случай ошибки чтения файла
    PRODUCTS_DB = []

# 1. Отдаем главную страницу (фронтенд)
@app.get("/")
async def read_index():
    return FileResponse('static/index.html')

# 2. API Ручка для рекомендаций
@app.post("/api/recommend")
async def get_recommendations(user: UserRequest):
    """
    Эндпоинт рекомендаций банковских продуктов.
    Принимает ID пользователя.
    Возвращает список рекомендованных услуг.
    """
    
    # Имитация работы ML-модели
    time.sleep(random.uniform(5, 8))
    
    if not PRODUCTS_DB:
        return {"user_id": user.user_id, "items": [], "error": "База продуктов пуста"}

    # выбираем 3-5 случайных продуктов из базы тк юзаем моки 
    count = random.randint(3, 5)
    recommendations = random.sample(PRODUCTS_DB, min(count, len(PRODUCTS_DB)))
    
    response_items = []
    for product in recommendations:
        # Генерируем фейковый "score" уверенности модели
        score = random.uniform(0.6, 0.98)
        
        response_items.append({
            "product_name": product.get("product_name"),
            "product_type": product.get("product_type"),
            "rate": product.get("rate"),
            "terms": product.get("term") or "Не указан",
            "requirements": product.get("requirements"),
            "url": product.get("source_url"),
            "score": score
        })
    
    # Сортируем по уверенности (score) по убыванию
    response_items.sort(key=lambda x: x["score"], reverse=True)
    
    return {"user_id": user.user_id, "items": response_items}

# Подключаем папку static
app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    print("Запуск сервера ПСБ Рекомендации на http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)