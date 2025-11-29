from fastapi import APIRouter
from fastapi.responses import FileResponse
from app.schemas import UserRequest
from app.services import RecommendationService

router = APIRouter(prefix='', tags=['Работа с рекомендациями'])

@router.get("/")
async def read_index():
    """Главная страница с рекомендациями"""
    return FileResponse('static/index.html')

@router.get("/profile")
async def read_profile():
    """Страница профиля пользователя"""
    return FileResponse('static/profile.html')

@router.post("/api/recommend")
async def get_recommendations(user: UserRequest):
    """API получения рекомендаций"""
    return RecommendationService.get_recommendations(user.user_id)

@router.get("/api/profile/{user_id}")
async def get_profile(user_id: str):
    """API получения профиля пользователя"""
    return RecommendationService.get_user_profile(user_id)
