"""  Модуль для описания и валидации формы """

from pydantic import BaseModel, HttpUrl, Field

from logging_config import get_logger

logger = get_logger()

class FormData(BaseModel):
    name: str = Field(..., min_length=1, max_length=30, description="Имя пользователя")
    url: HttpUrl = Field(..., description="URL сайта или социальной сети")
    activity_hobbies: str = Field(..., min_length=1, max_length=60, description="Поле активности и хобби")
    hashtags: str = Field(..., max_length=60, description="Хэштеги (неприменимый контент)")

    class Config:
        schema_extra = {
            "example": {
                "name": "John Doe",
                "url": "https://example.com",
                "activity_hobbies": "Gaming, Traveling",
                "hashtags": "#gaming #traveling"
            }
        }


async def submit_form(data: FormData):
    """
    Валидируем данные и возвращаем результат.
    """
    try:
        logger.info("Начинаем валидацию данных формы."). # Данные формы уже валидируются Pydantic, но я хочу перебдеть
        return {"message": "Данные формы успешно проверены", "data": data.dict()}

    except ValidationError as e:
        # В случае ошибок валидации
        logger.error(f"Ошибка валидации данных: {e.errors()}")
        raise HTTPException(status_code=422, detail=f"Ошибка валидации: {e.errors()}")

    except Exception as e:
        # Обработка других ошибок
        logger.exception("Произошла непредвиденная ошибка при валидации данных.")
        raise HTTPException(status_code=500, detail=f"Ошибка обработки формы: {str(e)}")
