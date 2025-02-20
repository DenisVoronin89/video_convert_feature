"""  Модуль для описания и валидации формы, валидации изображений и видео перед загрузкой """

from pydantic import BaseModel, HttpUrl, Field
import mimetypes
from fastapi import UploadFile
from typing import Dict, List, Optional
from datetime import datetime

from banned_words import BANNED_WORDS

from logging_config import get_logger

logger = get_logger()


class FormData(BaseModel):
    """Описание схемы данных формы."""
    name: str = Field(..., min_length=1, max_length=100, description="Имя пользователя")
    user_logo_url: HttpUrl = Field(..., description="URL логотипа пользователя")
    video_url: Optional[HttpUrl] = Field(None, description="URL видео пользователя")
    preview_url: Optional[HttpUrl] = Field(None, description="URL превью видео")
    activity_hobbies: Optional[str] = Field(None, min_length=1, max_length=500, description="Активность и хобби")
    is_incognito: bool = Field(False, description="Флаг инкогнито пользователя")
    is_in_mlm: Optional[int] = Field(0, description="Флаг участия в МЛМ (0 - нет, 1 - да)")
    adress: Optional[List[str]] = Field(None, min_items=1, max_items=10, description="Список адресов (макс. 10)") # Массив до 10 адресов
    city: Optional[str] = Field(None, min_length=1, max_length=55, description="Город пользователя")
    # Массив до 10 пар координат
    coordinates: Optional[List[List[float]]] = Field(
        None,
        min_items=1,
        max_items=10,
        description="Координаты пользователя (до 10 пар), каждая пара: [долгота, широта]"
    )


    class Config:
        json_schema_extra = {
            "example": {
                "name": "John Doe",
                "user_logo_url": "https://example.com/logo.jpg",
                "video_url": "https://example.com/video.mp4",
                "preview_url": "https://example.com/preview.jpg",
                "activity_hobbies": "Gaming, Traveling",
                "is_incognito": False,
                "is_in_mlm": 1,
                "adress": [
                    "123 Example Street, Example City",
                    "456 Another Street, Another City"
                ],
                "city": "Example City",
                "coordinates": [
                    [37.7749, -122.4194],  # Сан-Франциско
                    [48.8566, 2.3522]  # Париж
                ]
            }
        }



def filter_badwords(hashtags: str) -> Dict[str, bool]:
    """ Проверка наличия запрещенных слов в строке хэштегов """
    logger.info("Проверка хэштегов на запрещённые слова.")
    cleaned_hashtags = " ".join(tag.lstrip("#") for tag in hashtags.split()) # Удаление решеток из хэштегов, с ними не работает

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
        hashtag_check = filter_badwords(data.hashtags)
        if hashtag_check["has_invalid_words"]:
            invalid_words = ", ".join(hashtag_check["invalid_words"])
            logger.error(f"Хэштеги содержат запрещённые слова: {invalid_words}")
            raise HTTPException(
                status_code=400,
                detail=f"Неприемлемый контент в хэштегах: {invalid_words}"
            )

        # Преобразование HttpUrl в строку
        data_dict = data.dict()
        data_dict['url'] = str(data_dict['url'])

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


