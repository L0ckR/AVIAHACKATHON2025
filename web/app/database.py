import json

PRODUCTS_FILE = "psb_products_updated.json"

def load_products():
    """Загружаем продукты из JSON файла."""
    try:
        with open(PRODUCTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"Загружено {len(data)} банковских продуктов.")
        return data
    except Exception as e:
        print(f"Ошибка при загрузке продуктов: {e}")
        return []

# Инициализируем базу данных при импорте модуля
PRODUCTS_DB = load_products()
