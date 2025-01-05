"""  Модуль для описания и валидации формы, валидации изображений и видео перед загрузкой """

from pydantic import BaseModel, HttpUrl, Field
import mimetypes
from fastapi import UploadFile
from typing import Dict, List

from banned_words import BANNED_WORDS

from logging_config import get_logger

logger = get_logger()


class FormData(BaseModel):
    """Описание схемы данных формы."""
    name: str = Field(..., min_length=1, max_length=30, description="Имя пользователя")
    url: HttpUrl = Field(..., description="URL сайта или социальной сети")
    activity_hobbies: str = Field(..., min_length=1, max_length=60, description="Поле активности и хобби")
    hashtags: str = Field(..., max_length=60, description="Хэштеги (неприменимый контент)")
    is_incognito: bool = Field(False, description="Флаг инкогнито пользователя")
    wallet_number: str = Field(..., min_length=1, max_length=50, description="Номер кошелька пользователя")
    adress: str = Field(..., min_length=1, max_length=255, description="Адрес пользователя")
    coordinates: List[float] = Field(..., min_items=2, max_items=2, description="Координаты пользователя (широта, долгота)")

    class Config:
        json_schema_extra = {
            "example": {
                "name": "John Doe",
                "url": "https://example.com",
                "activity_hobbies": "Gaming, Traveling",
                "hashtags": "#gaming #traveling",
                "is_incognito": False,
                "wallet_number": "0x123456789ABCDEF",
                "adress": "123 Example Street, Example City",
                "coordinates": [37.7749, -122.4194]
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