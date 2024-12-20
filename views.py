import os
import aiofiles
from uuid import uuid4
from fastapi import UploadFile, HTTPException
from utils import get_file_size
from sqlalchemy.future import select

from logging_config import get_logger

from schemas import FormData


logger = get_logger()

async def create_video_temp_dir():
    """Создание временной директории для видео (video_temp) для файлов если её нет"""
    try:
        temp_video_dir = "./video_temp"
        if not os.path.exists(temp_video_dir):
            os.makedirs(temp_video_dir)
            logger.info(f"Создана временная директория: {temp_video_dir}")
        else:
            logger.info(f"Временная директория уже существует: {temp_video_dir}")
    except Exception as e:
        logger.error(f"Ошибка при создании временной директории: {e}")
        raise HTTPException(status_code=500, detail="Не удалось создать временную директорию")

async def save_video_to_temp(file: UploadFile):
    """Сохранение видео в папку temp"""
    try:
        await create_video_temp_dir()
        temp_video_path = f"./video_temp/{uuid4()}_{file.filename}"
        async with aiofiles.open(temp_video_path, "wb") as out_file:
            await out_file.write(await file.read())
        file_size = get_file_size(temp_video_path)
        logger.info(f"Файл сохранён во временной директории: {temp_video_path} (Размер: {file_size:.2f} MB)")
        return temp_video_path
    except Exception as e:
        logger.error(f"Ошибка при сохранении файла во временной директории: {e}")
        raise HTTPException(status_code=500, detail="Не удалось сохранить файл во временной директории")


async def create_image_temp_dir():
    """Создание временной директории для изображений (image_temp) если её нет"""
    try:
        image_temp_dir = "./image_temp"
        if not os.path.exists(image_temp_dir):
            os.makedirs(image_temp_dir)
            logger.info(f"Создана временная директория: {image_temp_dir}")
        else:
            logger.info(f"Временная директория для изображений уже существует: {image_temp_dir}")
    except Exception as e:
        logger.error(f"Ошибка при создании временной директории для изображений: {e}")
        raise HTTPException(status_code=500, detail="Не удалось создать временную директорию для изображений")

async def save_image_to_temp(file: UploadFile):
    """Сохранение изображения в папку image_temp"""
    try:
        await create_image_temp_dir()
        temp_image_path = f"./image_temp/{uuid4()}_{file.filename}"
        async with aiofiles.open(temp_image_path, "wb") as out_file:
            await out_file.write(await file.read())
        file_size = get_file_size(temp_image_path)
        logger.info(f"Изображение сохранено во временной директории: {temp_image_path} (Размер: {file_size:.2f} MB)")
        return temp_image_path
    except Exception as e:
        logger.error(f"Ошибка при сохранении изображения во временной директории: {e}")
        raise HTTPException(status_code=500, detail="Не удалось сохранить изображение во временной директории")


async def create_user_logo_dir():
    """Создание директории для логотипов пользователей (user_logo) если её нет."""
    try:
        user_logo_dir = "./user_logo"
        if not os.path.exists(user_logo_dir):
            os.makedirs(user_logo_dir)
            logger.info(f"Создана директория для логотипов пользователей: {user_logo_dir}")
        else:
            logger.info(f"Директория для логотипов пользователей уже существует: {user_logo_dir}")
    except Exception as e:
        logger.error(f"Ошибка при создании директории для логотипов пользователей: {e}")
        raise HTTPException(status_code=500, detail="Не удалось создать директорию для логотипов пользователей")


async def move_image_to_user_logo(temp_image_path: str, user_id: str):
    """
    Перемещение изображения из временной директории (image_temp) в директорию user_logo.

    :param temp_image_path: Путь к изображению во временной директории.
    :param user_id: Уникальный идентификатор пользователя, используется для именования файла.
    :return: Путь к файлу в директории user_logo.
    """
    try:
        await create_user_logo_dir()
        # Определяем конечный путь файла
        user_logo_path = f"./user_logo/{user_id}_{os.path.basename(temp_image_path)}"
        # Перемещаем файл
        shutil.move(temp_image_path, user_logo_path)
        file_size = get_file_size(user_logo_path)
        logger.info(f"Изображение перемещено в директорию user_logo: {user_logo_path} (Размер: {file_size:.2f} MB)")
        return user_logo_path
    except FileNotFoundError as fnf_error:
        logger.error(f"Файл не найден: {fnf_error}")
        raise HTTPException(status_code=404, detail="Файл для перемещения не найден")
    except Exception as e:
        logger.error(f"Ошибка при перемещении изображения в директорию user_logo: {e}")
        raise HTTPException(status_code=500, detail="Не удалось переместить изображение в директорию user_logo")


# Работа с формой
async def process_form(data: FormData):
    """
    Обработка данных формы: валидация и сохранение.
    """
    try:
        logger.info("Получены данные формы: %s", data.dict())

        # Пример логики обработки (в реальном случае - сохранение в БД)
        if "неприемлемый" in data.hashtags.lower():
            logger.warning("Обнаружен неприменимый контент в хэштегах")
            raise HTTPException(status_code=400, detail="Неприемлемый контент в хэштегах")

        # Имитация сохранения данных
        result = {
            "status": "success",
            "data": data.dict(),
            "message": "Данные формы успешно обработаны"
        }
        logger.info("Данные формы обработаны успешно: %s", result)
        return result

    except HTTPException as e:
        logger.error("Ошибка валидации данных: %s", e.detail)
        raise e

    except Exception as e:
        logger.exception("Непредвиденная ошибка при обработке формы")
        raise HTTPException(status_code=500, detail="Ошибка на сервере")


# Работа с хэштегами
async def get_videos_by_hashtag(tag):
    """
    Получает все видео, связанные с указанным хэштегом.

    :param tag: str, хэштег, по которому ищем связанные видео.
    :return: list, список видео, связанных с данным хэштегом. Пустой список, если хэштег не найден.
    """
    # Создаем сессию для взаимодействия с БД
    session = await get_session(engine)
    async with session.begin():  # Начинаем транзакцию
        # Ищем хэштег в базе данных по его тегу
        hashtag = await session.execute(
            select(Hashtag).filter(Hashtag.tag == tag)
        ).scalar()

        # Если хэштег найден, возвращаем связанные с ним видео
        if hashtag:
            return hashtag.videos  # Список видео, связанных с этим хэштэгом

        return []  # Если хэштег не найден, возвращаем пустой список
