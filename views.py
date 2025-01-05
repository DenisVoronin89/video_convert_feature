import os
import shutil
import aiofiles
from uuid import uuid4
from fastapi import UploadFile, HTTPException
from utils import get_file_size
from sqlalchemy.future import select

from logging_config import get_logger

logger = get_logger()


async def create_directories(directories_to_create: dict) -> dict:
    """
    Функция для создания необходимых директорий (запускается в стартапе)

    Args:
        directories_to_create (dict): Словарь с ключами и путями директорий для создания.

    Returns:
        dict: Словарь с путями и статусами (создана/уже существует). Его распарсить потом надо и рассовать пути в нужные функции
    """
    created_directories = {}
    try:
        for key, directory in directories_to_create.items():
            if not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)  # exist_ok=True безопаснее
                created_directories[key] = {"path": directory, "status": "created"}
                logger.info(f"Создана директория: {directory}")
            else:
                created_directories[key] = {"path": directory, "status": "exists"}
                logger.info(f"Директория уже существует: {directory}")
    except PermissionError as e:
        logger.error(f"Ошибка доступа при создании директории: {e.filename}, {e.strerror}")
        raise HTTPException(
            status_code=500,
            detail=f"Нет прав на создание директории: {e.filename}. Ошибка: {e.strerror}"
        )
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при создании директорий", exc_info=True)
        raise HTTPException(status_code=500, detail="Не удалось создать необходимые директории")

    return created_directories


async def save_image_to_temp(file: UploadFile, created_dirs: dict):
    """Сохранение изображения в папку image_temp"""
    try:
        # Путь к директории для изображения
        image_temp_path = created_dirs.get("image_temp", {}).get("path", "")

        if not image_temp_path:
            logger.error("Ошибка: директория 'image_temp' не найдена в конфигурации.")
            raise HTTPException(status_code=500, detail="Директория 'image_temp' не найдена в конфигурации.")

        # Формирование имени файла без пробелов (замена пробелов на подчеркивания)
        sanitized_image_filename = file.filename.replace(" ", "_")

        # Формирование пути к файлу
        temp_image_path = os.path.join(image_temp_path, f"{uuid4()}_{sanitized_image_filename}")

        # Сохранение изображения
        async with aiofiles.open(temp_image_path, "wb") as out_file:
            await out_file.write(await file.read())

        # Получение размера файла
        file_size = os.path.getsize(temp_image_path) / (1024 * 1024)
        logger.info(f"Изображение сохранено во временной директории: {temp_image_path} (Размер: {file_size:.2f} MB)")

        return temp_image_path
    except Exception as e:
        logger.error(f"Ошибка при сохранении изображения во временной директории: {e}")
        raise HTTPException(status_code=500, detail="Не удалось сохранить изображение во временной директории")



async def save_video_to_temp(file: UploadFile, created_dirs: dict):
    """Сохранение видео в папку temp"""
    try:
        temp_video_path = created_dirs.get("video_temp", {}).get("path", "")

        if not temp_video_path:
            logger.error("Ошибка: директория 'video_temp' не найдена в конфигурации.")
            raise HTTPException(status_code=500, detail="Директория 'video_temp' не найдена в конфигурации.")

        # Формирование имени файла без пробелов (замена пробелов на подчеркивания)
        sanitized_video_filename = file.filename.replace(" ", "_")

        # Формирование пути к файлу
        temp_video_path = os.path.join(temp_video_path, f"{uuid4()}_{sanitized_video_filename}")

        # Сохранение видео
        async with aiofiles.open(temp_video_path, "wb") as out_file:
            await out_file.write(await file.read())

        # Получение размера файла
        file_size = os.path.getsize(temp_video_path) / (1024 * 1024)
        logger.info(f"видео сохранено во временной директории: {temp_video_path} (Размер: {file_size:.2f} MB)")

        return temp_video_path
    except Exception as e:
        logger.error(f"Ошибка при сохранении видео во временной директории: {e}")
        raise HTTPException(status_code=500, detail="Не удалось сохранить видео во временной директории")


async def move_image_to_user_logo(image_path: str, created_dirs: dict) -> str:
    """
    Перемещение изображения из временной директории в директорию user_logo и возврат пути к файлу.

    :param image_path: Путь к изображению.
    :param created_dirs: Словарь с созданными директориями. (Вытащить оттуда нужный путь)
    :return: Путь к файлу в директории user_logo.
    """
    try:
        user_logo_dir = created_dirs.get("user_logo", {}).get("path", "")

        if not user_logo_dir:
            logger.error("Ошибка: директория 'user_logo' не найдена в конфигурации.")
            raise HTTPException(status_code=500, detail="Директория 'user_logo' не найдена в конфигурации.")

        # Генерация уникального имени файла
        user_logo_filename = f'{uuid4()}_{os.path.basename(image_path)}'
        user_logo_path = os.path.join(user_logo_dir, user_logo_filename)

        # Перемещение в постоянную директорию перед сохранением в БД
        shutil.move(image_path, user_logo_path)

        file_size = os.path.getsize(user_logo_path) / (1024 * 1024)
        logger.info(f"Изображение перемещено в директорию user_logo: {user_logo_path} (Размер: {file_size:.2f} MB)")

        logger.info(f"Путь к файлу для сохранения в БД: {user_logo_path}")

        # Возврат пути к файлу, а не URL
        return user_logo_path

    except FileNotFoundError as fnf_error:
        logger.error(f"Файл не найден: {fnf_error}")
        raise HTTPException(status_code=404, detail="Файл для перемещения не найден")
    except Exception as e:
        logger.error(f"Ошибка при перемещении изображения в директорию user_logo: {e}")
        raise HTTPException(status_code=500, detail="Не удалось переместить изображение в директорию user_logo")


# Работа с хэштегами
async def get_videos_by_hashtag(tag):
    """
    Получение всех видео, связанных с указанным хэштегом.
    """
    session = await get_session(engine)
    async with session.begin():
        # Поиск хэштега в БД по его тегу
        hashtag = await session.execute(
            select(Hashtag).filter(Hashtag.tag == tag)
        ).scalar()

        # Если хэштег найден, возвращает связанные с ним видео
        if hashtag:
            return hashtag.videos  # Список видео, связанных с этим хэштэгом

        return []  # Если хэштег не найден, возвращает пустой список
