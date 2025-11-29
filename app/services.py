import time
import random
from app.database import PRODUCTS_DB

class RecommendationService:
    @classmethod
    def get_recommendations(cls, user_id: str) -> dict:
        """
        Сервис рекомендаций банковских продуктов.
        """
        # Имитация работы ML-модели
        time.sleep(random.uniform(0.5, 1.5))

        if not PRODUCTS_DB:
            return {"user_id": user_id, "items": [], "error": "База продуктов пуста"}

        # выбираем 3-5 случайных продуктов из базы
        count = random.randint(3, 5)
        recommendations = random.sample(PRODUCTS_DB, min(count, len(PRODUCTS_DB)))
        
        response_items = []
        for product in recommendations:
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
        
        response_items.sort(key=lambda x: x["score"], reverse=True)
        
        return {"user_id": user_id, "items": response_items}

    @classmethod
    def get_user_profile(cls, user_id: str) -> dict:
        """
        Возвращает моковый профиль пользователя с 'LLM-generated' портретом.
        """
        # Имитация задержки получения данных из CRM/Feature Store
        time.sleep(random.uniform(0.2, 0.6))
        
        segments = ["Mass", "Affluent", "Premium", "Private Banking"]
        interests_list = ["Путешествия", "Инвестиции", "Спорт", "Автомобили", "Недвижимость", "Технологии", "Искусство"]
        
        # Генерируем случайные данные, зависящие от ID (для стабильности можно использовать seed)
        random.seed(user_id)
        
        age = random.randint(21, 75)
        segment = random.choice(segments)
        income = random.choice(["Средний", "Выше среднего", "Высокий", "Очень высокий"])
        top_interests = random.sample(interests_list, k=random.randint(2, 4))
        risk_profile = random.choice(["Консервативный", "Умеренный", "Агрессивный"])
        
        # Генерируем фейковый "LLM-портрет"
        llm_summary = (
            f"Клиент {age} лет, относится к сегменту {segment}. Уровень дохода оценивается как «{income.lower()}». "
            f"В последнее время проявляет активный интерес к категориям: {', '.join(top_interests).lower()}. "
            f"Характеризуется как {risk_profile.lower()} инвестор. "
            "На основе транзакционной активности рекомендуется предложить продукты для сохранения капитала и премиальное обслуживание."
        )

        return {
            "user_id": user_id,
            "full_name": f"Клиент {user_id}",
            "age": age,
            "segment": segment,
            "income_level": income,
            "risk_profile": risk_profile,
            "loyalty_points": random.randint(0, 150000),
            "active_products_count": random.randint(1, 5),
            "top_interests": top_interests,
            "llm_summary": llm_summary  # <-- Добавлено поле портрета
        }
