import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from app.routers import router

app = FastAPI()

# Подключаем статику (фронтенд)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Подключаем роутеры (API)
app.include_router(router)

if __name__ == "__main__":
    print("Запуск сервера ПСБ Рекомендации на http://localhost:8000")
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
