import os
from uuid import uuid4
from fastapi import FastAPI, UploadFile, HTTPException, BackgroundTasks
from aiobotocore.session import get_session
import aiofiles

from database import init_db
from logging_config import get_logger

from video_handle.publisher import publish_task
from video_handle.subscriber import main as subscriber_main
from video_handle.worker import convert_to_vp9, extract_preview, upload_to_s3, save_video_to_db
from views import save_video_to_temp, save_image_to_temp, process_form
from schemas import FormData, submit_form
from check_payment import check_payment

# Получаем логгер
logger = get_logger()

# Конфиги для разработки и тестирования (НЕ ДЛЯ ПРОДАКШЕНА)
S3_BUCKET_NAME = "video-service"
AWS_REGION = "us-east-1"
AWS_ACCESS_KEY_ID = "SuOpyKZ54797K7y9vvaJ"
AWS_SECRET_ACCESS_KEY = "6NBChpstlgkjvRTawqKRuNvGBVNRG8EWIPCu4Izl"

PREVIEW_DURATION = 5 # Длина превью

# Сессия для работы с AWS S3
s3_session = get_session()

app = FastAPI()

@app.on_event("startup")
async def startup():
    """Функция запуска приложения"""
    await init_db()
    logger.info("Приложение успешно запущено. Соединение с базой данных установлено.")

@app.on_event("shutdown")
async def shutdown():
    """Функция завершения работы приложения"""
    await engine.dispose()
    logger.info("Приложение завершило работу. Соединение с базой данных закрыто.")


# Эндпоинт для обработки данных формы TODO форму кэшировать надо пока проверка платежа проходит
@app.post("/submit-form", summary="Submit Form Data", description="Обработка данных формы")
async def submit_form(data: FormData):
    return await process_form(data)


 #Эндпоинт для сохранения видео и изображения
@app.post("/add_img_and_video/")
async def add_img_and_video(file: UploadFile):
    try:
        # Сохранение видео
        video_path = await save_video_to_temp(file)
        if not video_path:
            raise HTTPException(status_code=500, detail="Не удалось сохранить видео")

        # Сохранение изображения
        image_path = await save_image_to_temp(file)
        if not image_path:
            raise HTTPException(status_code=500, detail="Не удалось сохранить изображение")

        return {
            "message": "Видео и изображение успешно сохранены",
            "video_path": video_path,
            "image_path": image_path
        }

    except HTTPException as e:
        raise e

    except Exception as e:
        logger.error(f"Произошла ошибка при сохранении файла: {str(e)}")
        raise HTTPException(status_code=500, detail="Ошибка при сохранении видео и изображения")


# Эндпоинт проверки оплаты
@app.post("/verify-payment/")
async def verify_payment(wallet_address: str):
    """Проверка оплаты"""
    try:
        # Проверка транзакции с использованием функции check_payment
        is_payment_valid = await check_payment(wallet_address)
        if not is_payment_valid:
            raise HTTPException(status_code=400, detail="Платеж не подтвержден")
        return {"message": "Платеж подтвержден"}
    except Exception as e:
        # Логирование ошибок
        logger.error(f"Ошибка при проверке платежа: {e}")
        raise HTTPException(status_code=500, detail="Ошибка при проверке платежа")


@app.post("/save_profile/")
async def save_profile(profile_data: FormData):
    """
    Получаем данные профиля, отправляем задачу на обработку в Redis
    и возвращаем клиенту сообщение о том, что задача отправлена на модерацию.
    """
    # Преобразуем данные формы в словарь
    form_data_dict = profile_data.dict()

    try:
        # Логируем начало обработки запроса
        logger.info("Получены данные профиля: %s", form_data_dict)

        # Подключаемся к Redis
        redis = aioredis.Redis(host='localhost', port=6379, db=0)
        logger.info("Соединение с Redis установлено.")

        # Публикуем задачу в Redis
        await publish_task(
            redis,
            input_path="path_to_input_video",  # Пример данных
            output_path="path_to_output_video",
            preview_path="path_to_preview",
            s3_key="generated_s3_key",
            form_data=form_data_dict  # Передаем данные формы как словарь
        )
        logger.info("Задача успешно отправлена в Redis.")

        # Ответ клиенту
        return {"message": "Ваш профиль успешно сохранен и отправлен на модерацию."}

    except aioredis.RedisError as e:
        # Логируем ошибку при работе с Redis
        logger.error("Ошибка при подключении или публикации в Redis: %s", str(e))
        raise HTTPException(status_code=500, detail=f"Ошибка при сохранении профиля в Redis: {str(e)}")

    except Exception as e:
        # Логируем общие ошибки
        logger.error("Ошибка при сохранении профиля: %s", str(e))
        raise HTTPException(status_code=500, detail=f"Ошибка при сохранении профиля: {str(e)}")


