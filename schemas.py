"""  Модуль для описания и валидации формы, валидации изображений и видео перед загрузкой """

from pydantic import BaseModel, HttpUrl, Field
import mimetypes
from fastapi import UploadFile, HTTPException
from typing import Dict, List, Optional
from datetime import datetime

from typing_extensions import Tuple

from banned_words import BANNED_WORDS

from logging_config import get_logger

logger = get_logger()


# Описание схемы данных формы
class FormData(BaseModel):
    """Описание схемы данных формы."""

    name: str = Field(..., min_length=1, max_length=100, description="Имя пользователя")
    website_or_social: Optional[str] = Field(None, max_length=255, description="Веб-сайт или соц. сеть пользователя")
    activity_hobbies: Optional[str] = Field(None, max_length=500, description="Активность и хобби")
    hashtags: Optional[List[str]] = Field(None, min_items=1, max_items=20, description="Список хэштегов, связанных с пользователем (до 20 шт.)")
    adress: Optional[List[str]] = Field(None, min_items=1, max_items=10, description="Список адресов (макс. 10)")
    city: Optional[str] = Field(None, max_length=55, description="Город пользователя")
    coordinates: Optional[List[Tuple[float, float]]] = Field(None, min_items=1, max_items=10, description="Координаты пользователя (до 10 пар), каждая пара: [долгота, широта]")
    is_in_mlm: Optional[int] = Field(0, description="Флаг участия в МЛМ")
    is_incognito: bool = Field(False, description="Флаг инкогнито пользователя")
    wallet_number: str = Field(min_length=1, max_length=100, description="Номер кошелька пользователя")


    class Config:
        json_schema_extra = {
            "example": {
                "name": "John Doe",
                "website_or_social": "https://twitter.com/johndoe",
                "activity_hobbies": "Gaming, Traveling",
                "hashtags": ["#gaming", "#travel", "#photography"],
                "adress": [
                    "123 Example Street, Example City",
                    "456 Another Street, Another City"
                ],
                "city": "Example City",
                "coordinates": [
                    (37.7749, -122.4194),
                    (48.8566, 2.3522)
                ],
                "is_in_mlm": 1,
                "is_incognito": False,
                "wallet_number": "0x123abc456def"
            }
        }


async def serialize_form_data(data: Dict[str, any]) -> Dict[str, any]:
    """ Преобразование всех значений в data(данные формы), которые являются HttpUrl, в строки """
    try:
        for key, value in data.items():
            if isinstance(value, HttpUrl):
                data[key] = str(value)

        return data

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка сериализации данных: {str(e)}"
        )


async def filter_badwords(hashtags: List[str]) -> Dict[str, bool]:
    """Проверка наличия запрещенных слов в списке хэштегов"""
    logger.info("Проверка хэштегов на запрещённые слова.")

    # Преобразуем список хэштегов в строку, удаляя решетку у каждого хэштега
    cleaned_hashtags = " ".join(tag.lstrip("#") for tag in hashtags)

    # Проверяем, если запрещенные слова присутствуют в строке с хэштегами
    invalid_words = [word for word in BANNED_WORDS if word in cleaned_hashtags.lower()]

    if invalid_words:
        logger.warning(f"Найдены запрещённые слова в хэштегах: {', '.join(invalid_words)}")
        return {"has_invalid_words": True, "invalid_words": invalid_words}

    logger.info("Запрещённые слова не обнаружены в хэштегах.")
    return {"has_invalid_words": False}


async def validate_and_process_form(data: FormData):
    """ Объединяет валидацию и обработку данных формы """
    try:
        logger.info("Начало валидации и обработки данных формы.")

        # Проверка хэштегов на запрещенные слова
        hashtag_check = await filter_badwords(data.hashtags)
        if hashtag_check["has_invalid_words"]:
            invalid_words = ", ".join(hashtag_check["invalid_words"])
            logger.error(f"Хэштеги содержат запрещённые слова: {invalid_words}")
            raise HTTPException(
                status_code=400,
                detail=f"Неприемлемый контент в хэштегах: {invalid_words}"
            )

        # Преобразование HttpUrl в строку, если существует ключ 'website_or_social'
        data_dict = data.dict()
        if 'website_or_social' in data_dict:
            data_dict['website_or_social'] = str(data_dict['website_or_social'])

        # Логика обработки данных
        result = {
            "status": "success",
            "data": data_dict,
            "message": "Данные формы успешно обработаны"
        }
        logger.info("Данные формы успешно обработаны.")
        return result

    except HTTPException as e:
        logger.error(f"Ошибка валидации формы: {e.detail}")
        raise e
    except Exception as e:
        logger.error(f"Неожиданная ошибка при обработке формы: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка обработки формы: {str(e)}"
        )


# Проверка на валидность изображения по MIME-типу и расширению
def is_valid_image(file: UploadFile) -> bool:
    mime_type, _ = mimetypes.guess_type(file.filename)
    if mime_type and mime_type.startswith("image"):
        # Полный список поддерживаемых расширений для изображений
        image_extensions = [
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.heif',
            '.raw', '.svg', '.ico', '.eps', '.ai'
        ]
        return any(file.filename.endswith(ext) for ext in image_extensions)
    return False


# Проверка на валидность видео по MIME-типу и расширению
def is_valid_video(file: UploadFile) -> bool:
    mime_type, _ = mimetypes.guess_type(file.filename)
    if mime_type and mime_type.startswith("video"):
        # Полный список поддерживаемых расширений для видео
        video_extensions = [
            '.mp4', '.avi', '.mov', '.mkv', '.webm', '.flv', '.mpg', '.mpeg',
            '.3gp', '.wmv', '.rm', '.ogv', '.mpeg2', '.ts', '.vob'
        ]
        return any(file.filename.endswith(ext) for ext in video_extensions)
    return False


# Модели данных для передачи информации
class Token(BaseModel):
    access_token: str
    refresh_token: str


class TokenData(BaseModel):
    user_id: int


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str


class UserProfileResponse(BaseModel):
    id: int
    created_at: Optional[datetime]  # Опционально, так как профиль может не существовать
    name: Optional[str]
    user_logo_url: Optional[str]
    video_url: Optional[str]
    preview_url: Optional[str]
    activity_and_hobbies: Optional[str]
    is_moderated: Optional[bool]
    is_incognito: Optional[bool]
    is_in_mlm: Optional[int]
    website_or_social: Optional[str]
    is_admin: Optional[bool]
    adress: Optional[dict]  # JSONB может быть представлен как dict в Pydantic
    city: Optional[str]
    coordinates: Optional[str]
    followers_count: Optional[int]

    class Config:
        from_attributes = True  # Указывает Pydantic, что это модель SQLAlchemy


class UserResponse(BaseModel):
    id: int
    profile: Optional[UserProfileResponse]
    favorites: Optional[list[int]]
    tokens: Token  # Добавляем поле для токенов

    class Config:
        from_attributes = True


