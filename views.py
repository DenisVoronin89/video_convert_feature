import os
import shutil
import aiofiles
from uuid import uuid4
from fastapi import UploadFile, HTTPException
from geoalchemy2.shape import to_shape
from shapely.geometry import Point, MultiPoint
from typing import Optional, List, Union
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload, subqueryload, selectinload
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import desc, func

from models import UserProfiles, Hashtag, ProfileHashtag
from database import get_db_session_for_worker
from utils import process_coordinates_for_response, datetime_to_str, get_file_size

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


# Получение всех профилей без фильтров (не по алгоритму 10-10-10-10-10) с возможностью сортировки по новизне и популярности
async def get_all_profiles(
    page: int,
    sort_by: Optional[str],
    per_page: int
) -> dict:
    """
    Получает все профили пользователей с пагинацией и сортировкой.

    :param page: Номер страницы (начинается с 1).
    :param sort_by: Параметр сортировки (опционально). Возможные значения: "newest", "popularity".
    :param per_page: Количество профилей на странице.
    :return: Словарь с данными о профилях, включая пагинацию и общее количество.
    :raises HTTPException: Если произошла ошибка при выполнении запроса.
    """
    try:
        async with get_db_session_for_worker() as session:  # Управление сессией внутри функции
            # Базовый запрос: исключаем приватные профили
            query = select(UserProfiles).filter(UserProfiles.is_incognito == False)

            logger.info(f"Запрос всех профилей, страница: {page}, сортировка: {sort_by}, профилей на странице: {per_page}")

            # Применяем сортировку
            if sort_by == "newest":
                query = query.order_by(desc(UserProfiles.created_at))  # Сортировка по дате создания
            elif sort_by == "popularity":
                query = query.order_by(desc(UserProfiles.followers_count))  # Сортировка по количеству подписчиков

            # Пагинация с учетом параметра per_page
            offset = (page - 1) * per_page
            query = query.offset(offset).limit(per_page)

            # Добавление жадной загрузки хэштегов
            query = query.options(
                selectinload(UserProfiles.user),
                selectinload(UserProfiles.profile_hashtags).selectinload(ProfileHashtag.hashtag)
            )

            # Получаем результат
            result = await session.execute(query)
            profiles = result.scalars().all()

            # Формируем данные для ответа
            profiles_data = []
            for profile in profiles:
                # Обработка координат
                coordinates = process_coordinates_for_response(profile.coordinates)

                # Формирование структуры профиля
                profile_data = {
                    "id": profile.id,
                    "created_at": await datetime_to_str(profile.created_at),
                    "name": profile.name,
                    "user_logo_url": profile.user_logo_url,
                    "video_url": profile.video_url,
                    "preview_url": profile.preview_url,
                    "activity_and_hobbies": profile.activity_and_hobbies,
                    "is_moderated": profile.is_moderated,
                    "is_incognito": profile.is_incognito,
                    "is_in_mlm": profile.is_in_mlm,
                    "adress": profile.adress,
                    "coordinates": coordinates,
                    "followers_count": profile.followers_count,
                    "website_or_social": profile.website_or_social,
                    "user": {
                        "id": profile.user.id,
                        "wallet_number": profile.user.wallet_number,
                    },
                    "hashtags": [ph.hashtag.tag for ph in profile.profile_hashtags],  # Только хэштеги текущего профиля
                }
                profiles_data.append(profile_data)

            # Получаем общее количество профилей (без учета пагинации)
            total_query = select(func.count()).select_from(UserProfiles).filter(UserProfiles.is_incognito == False)
            total_result = await session.execute(total_query)
            total = total_result.scalar()

            logger.info(f"Получено {len(profiles)} профилей для страницы {page}")
            return {
                "page": page,
                "per_page": per_page,
                "total": total,
                "profiles": profiles_data,
            }

    except SQLAlchemyError as e:
        logger.error(f"Ошибка выполнения запроса к базе данных: {e}")
        raise HTTPException(status_code=500, detail="Ошибка базы данных, попробуйте позже.")

    except Exception as e:
        logger.error(f"Неизвестная ошибка: {e}")
        raise HTTPException(status_code=500, detail="Произошла ошибка, попробуйте позже.")




# Получить список юзеров по городу (поисковой запрос - показать юзеров в городе) с возможностью сортировки по новизне и популярности
async def get_profiles_by_city(
    city: str, page: int, sort_by: str, per_page: int, db: AsyncSession
):
    """ Логика для получения профилей пользователей по городу """
    try:
        # Базовый запрос: исключаем приватные профили
        query = select(UserProfiles).filter(
            UserProfiles.city == city,
            UserProfiles.is_incognito == False,  # Исключаем приватные профили
        )

        logger.info(f"Запрос профилей для города: {city}, страница: {page}, сортировка: {sort_by}, профилей на странице: {per_page}")

        # Если это первая страница и сортировка не указана
        if page == 1 and not sort_by:
            query = query.offset(0).limit(per_page)  # Пагинация с ограничением на количество записей на странице
            result = await db.execute(query)
            profiles = result.scalars().all()
            total = await db.execute(select([func.count()]).select_from(UserProfiles).filter(UserProfiles.city == city))
            total = total.scalar()
            logger.info(f"Получено {len(profiles)} профилей для первой страницы")
            return {
                "page": page,
                "per_page": per_page,
                "total": total,
                "profiles": profiles,
            }

        # Если указана сортировка
        if sort_by == "newest":
            query = query.order_by(desc(UserProfiles.created_at))
        elif sort_by == "popularity":
            query = query.order_by(desc(UserProfiles.followers_count))

        # Пагинация с учетом параметра per_page
        offset = (page - 1) * per_page
        query = query.offset(offset).limit(per_page)

        # Добавление жадной загрузки хэштегов
        query = query.options(selectinload(UserProfiles.hashtags))  # Жадная загрузка хэштегов

        # Получаем результат
        result = await db.execute(query)
        profiles = result.scalars().all()

        # Получаем общее количество профилей
        total = await db.execute(select([func.count()]).select_from(UserProfiles).filter(UserProfiles.city == city))
        total = total.scalar()

        logger.info(f"Получено {len(profiles)} профилей для страницы {page}")
        return {
            "page": page,
            "per_page": per_page,
            "total": total,
            "profiles": profiles,
        }

    except SQLAlchemyError as e:
        logger.error(f"Ошибка выполнения запроса к базе данных: {e}")
        raise Exception("Ошибка базы данных, попробуйте позже.") from e

    except Exception as e:
        logger.error(f"Неизвестная ошибка: {e}")
        raise Exception("Произошла ошибка, попробуйте позже.") from e


# Получение пользователя по номеру кошелька
async def get_profile_by_wallet_number(wallet_number: str, db: AsyncSession):
    """
    Логика получения профиля пользователя по номеру кошелька (асинхронно).

    :param wallet_number: Номер кошелька для поиска.
    :param db: Сессия базы данных.
    :return: Словарь с информацией о профиле.
    :raises HTTPException: Если профиль не найден или произошла ошибка.
    """
    try:
        # Хэширование номера кошелька для поиска
        hashed_wallet_number = hashlib.sha256(wallet_number.encode()).hexdigest()
        logger.info(f"Ищем пользователя с хэшированным номером кошелька: {hashed_wallet_number}")

        # Поиск пользователя по хэшированному номеру кошелька
        result = await db.execute(
            db.query(User).filter(User.wallet_number == hashed_wallet_number)
        )
        user = result.scalar_one_or_none()

        if not user:
            logger.error(f"Пользователь с номером кошелька {wallet_number} не найден.")
            raise HTTPException(status_code=404, detail="Пользователь с таким номером кошелька не найден.")

        if not user.is_profile_created or not user.profile:
            logger.error(f"Профиль пользователя с номером кошелька {wallet_number} не найден.")
            raise HTTPException(status_code=404, detail="Профиль пользователя не найден.")

        # Формирование данных профиля для ответа
        profile = user.profile
        profile_data = {
            "id": profile.id,
            "name": profile.name,
            "user_logo_url": profile.user_logo_url,
            "video_url": profile.video_url,
            "preview_url": profile.preview_url,
            "activity_and_hobbies": profile.activity_and_hobbies,
            "is_moderated": profile.is_moderated,
            "is_incognito": profile.is_incognito,
            "is_in_mlm": profile.is_in_mlm,
            "adress": profile.adress,
            "city": profile.city,
            "coordinates": profile.coordinates,
            "followers_count": profile.followers_count,
            "created_at": profile.created_at,
        }

        logger.info(f"Профиль пользователя с номером кошелька {wallet_number} успешно найден.")
        return profile_data

    except Exception as e:
        logger.error(f"Ошибка при получении профиля для кошелька {wallet_number}: {e}")
        raise HTTPException(status_code=500, detail="Ошибка сервера при получении профиля.")