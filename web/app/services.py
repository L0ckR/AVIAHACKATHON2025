import time
import random
import json
import os
from app.database import PRODUCTS_DB

def load_socdem_clusters():
    try:
        base_path = os.path.dirname(os.path.abspath(__file__)) 
        project_root = os.path.dirname(os.path.dirname(base_path)) 
        
        json_path = os.path.join(project_root, "data", "support", "socdem_cluster.json")
        
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading socdem clusters: {e}")
        # Fallback if file not found
        return {
            "0": {"name": "Family Realists", "description": "Highly family‑oriented..."},
        }

SOCDEM_CLUSTERS = load_socdem_clusters()

class RecommendationService:
    @classmethod
    def get_recommendations(cls, user_id: str) -> dict:
        """
        Сервис рекомендаций банковских продуктов.
        """
        time.sleep(random.uniform(0.5, 1.5))

        if not PRODUCTS_DB:
            return {"user_id": user_id, "items": [], "error": "База продуктов пуста"}

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
        time.sleep(random.uniform(0.2, 0.6))
        
        random.seed(user_id)
        
        age = random.randint(21, 75)
        
        first_names = ["Александр", "Мария", "Дмитрий", "Елена", "Сергей", "Ольга", "Иван", "Татьяна", "Максим", "Анна"]
        last_names = ["Иванов(а)", "Смирнов(а)", "Кузнецов(а)", "Попов(а)", "Васильев(а)", "Петров(а)", "Соколов(а)", "Михайлов(а)"]
        full_name = f"{random.choice(first_names)} {random.choice(last_names)}"

        cluster_id = str(random.randint(0, 4))
        cluster_info = SOCDEM_CLUSTERS.get(cluster_id, SOCDEM_CLUSTERS.get("0"))
        
        segment_name = cluster_info.get("russian_name", cluster_info.get("name"))
        segment_desc = cluster_info.get("russian_description", cluster_info.get("description"))
        
        income = random.choice(["Низкий", "Ниже среднего", "Средний", "Выше среднего", "Высокий", "Очень высокий"])
        
        interests_pool = ["Путешествия", "Инвестиции", "Спорт", "Автомобили", "Недвижимость", "Технологии", "Искусство", "Дача", "Дети", "Здоровье"]
        top_interests = random.sample(interests_pool, k=random.randint(3, 5))
        
        risk_profile = random.choice(["Консервативный", "Умеренный", "Агрессивный"])
        llm_summary = (
            f"<b>Психотип: {segment_name}</b><br><br>"
            f"{segment_desc} <br><br>"
            f"Уровень дохода: {income.lower()}. "
            f"Основные интересы: {', '.join(top_interests).lower()}. "
            f"Риск-профиль: {risk_profile.lower()}."
        )

        return {
            "user_id": user_id,
            "full_name": full_name,
            "age": age,
            "segment": segment_name, 
            "income_level": income,
            "risk_profile": risk_profile,
            "loyalty_points": random.randint(0, 150000),
            "active_products_count": random.randint(1, 5),
            "top_interests": top_interests,
            "llm_summary": llm_summary
        }
