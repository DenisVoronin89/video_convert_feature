import os
import shutil
import aiofiles
import hashlib
import random
from uuid import uuid4
from fastapi import UploadFile, HTTPException, status, Query
from geoalchemy2.shape import to_shape
from shapely.wkb import loads as wkb_loads
from shapely.geometry import Point, MultiPoint
from geoalchemy2 import Geography, Geometry
from geoalchemy2.functions import ST_SetSRID, ST_MakePoint, ST_DWithin
from typing import Optional, List, Union, Set
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload, subqueryload, selectinload
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import desc, func, bindparam, Integer, update
import redis.asyncio as redis
from redis.exceptions import RedisError

from models import UserProfiles, Hashtag, ProfileHashtag, User
from database import get_db_session_for_worker
from utils import process_coordinates_for_response, datetime_to_str, get_file_size, calculate_distance

from logging_config import get_logger

logger = get_logger()

# Big Boss Royal Wallet Executor  TODO В Енв файл добавить перед деплоем!!!
ROYAL_WALLET = "f789e481a037797a0625c7e76093f1da44c4dea77c3faf6f1a838a9e9fab529e"

# Настраиваем соединение с Redis
redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)


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


# Формируем ключи Redis для списка показанных профилей для юзера
async def get_shown_profiles_key(user_id: Optional[int], ip_address: Optional[str]) -> str:
    """
    Возвращает ключ для Redis на основе user_id или ip_address.

    :param user_id: ID авторизованного пользователя.
    :param ip_address: IP-адрес неавторизованного пользователя.
    :return: Ключ для Redis.
    :raises ValueError: Если не указан ни user_id, ни ip_address.
    """
    if user_id:
        return f"shown_profiles:{user_id}"
    elif ip_address:
        return f"shown_profiles:{ip_address}"
    else:
        raise ValueError("Необходим user_id или ip_address")


# Получение списка показанных профилей из Redis
async def get_shown_profiles(user_id: Optional[int], ip_address: Optional[str]) -> Set[int]:
    """
    Возвращает список показанных профилей для пользователя.

    :param user_id: ID авторизованного пользователя.
    :param ip_address: IP-адрес неавторизованного пользователя.
    :return: Множество ID показанных профилей.
    """
    shown_profiles_key = await get_shown_profiles_key(user_id, ip_address)
    shown_profiles = await redis_client.smembers(shown_profiles_key)
    if shown_profiles:
        return set(map(int, shown_profiles))
    return set()


# Добавление показанных профилей в Redis
async def add_shown_profiles(user_id: Optional[int], ip_address: Optional[str], profile_ids: List[int]):
    """
    Добавляет показанные профили для пользователя.

    :param user_id: ID авторизованного пользователя.
    :param ip_address: IP-адрес неавторизованного пользователя.
    :param profile_ids: Список ID профилей, которые нужно добавить.
    """
    shown_profiles_key = await get_shown_profiles_key(user_id, ip_address)
    if profile_ids:
        await redis_client.sadd(shown_profiles_key, *profile_ids)
        await redis_client.expire(shown_profiles_key, 3600)  # TTL = 1 час


# Получение всех профилей по тому же алгоритму или сортировке по новизне/популярности
async def get_all_profiles(
    page: int,
    sort_by: Optional[str] = Query(None, description="Параметр сортировки (newest, popularity)"),
    per_page: int = Query(50, description="Количество профилей на страницу (по умолчанию 50)")
) -> dict:
    """
    Получает все профили с пагинацией и сортировкой.

    :param page: Номер страницы (начинается с 1).
    :param sort_by: Параметр сортировки (newest, popularity).
    :param per_page: Количество профилей на страницу.
    :param user_id: ID авторизованного пользователя.
    :param ip_address: IP-адрес неавторизованного пользователя.
    :return: Словарь с данными о профилях, включая пагинацию и общее количество.
    :raises HTTPException: Если произошла ошибка при выполнении запроса.
    """
    try:
        async with get_db_session_for_worker() as session:
            logger.info(f"Запрос профилей: page={page}, per_page={per_page}, sort_by={sort_by}, user_id={user_id}, ip_address={ip_address}")

            # Получаем множество уже показанных профилей из Redis
            shown_profile_ids = await get_shown_profiles(user_id, ip_address)
            logger.info(f"Получены {len(shown_profile_ids)} ID показанных профилей: {list(shown_profile_ids)[:10]}...")  # Выводим первые 10

            # Базовый запрос
            base_query = (
                select(UserProfiles)
                .options(
                    joinedload(UserProfiles.user),
                    subqueryload(UserProfiles.profile_hashtags).subqueryload(ProfileHashtag.hashtag)
                )
                .filter(UserProfiles.is_incognito == False)
            )

            # Исключаем уже показанные профили
            if shown_profile_ids:
                base_query = base_query.filter(UserProfiles.id.notin_(list(shown_profile_ids)))

            # Применяем сортировку
            if sort_by == "newest":
                query = base_query.order_by(desc(UserProfiles.created_at))
                logger.info(f"Сортировка по новизне, страница {page}")
            elif sort_by == "popularity":
                query = base_query.order_by(desc(UserProfiles.followers_count))
                logger.info(f"Сортировка по популярности, страница {page}")
            else:
                # Если сортировка не указана, возвращаем случайные профили
                query = base_query.order_by(func.random())
                logger.info(f"Случайная сортировка, страница {page}")

            # Пагинация
            offset = (page - 1) * per_page
            query = query.offset(offset).limit(per_page)

            # Выполняем запрос
            profiles = (await session.execute(query)).scalars().unique().all()
            logger.info(f"Найдено {len(profiles)} профилей для страницы {page}")

            # Если профили найдены, сохраняем их ID в Redis
            if profiles:
                await add_shown_profiles(user_id, ip_address, [p.id for p in profiles])

            # Формируем данные для ответа
            profiles_data = [{
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
                "coordinates": await process_coordinates_for_response(profile.coordinates),
                "followers_count": profile.followers_count,
                "website_or_social": profile.website_or_social,
                "user": {
                    "id": profile.user.id,
                    "wallet_number": profile.user.wallet_number,
                },
                "hashtags": [ph.hashtag.tag for ph in profile.profile_hashtags],
            } for profile in profiles]

            # Получаем общее количество профилей (без учета пагинации)
            total_query = select(func.count()).select_from(UserProfiles).filter(UserProfiles.is_incognito == False)
            total_result = await session.execute(total_query)
            total = total_result.scalar()
            logger.info(f"Общее количество профилей (без учёта фильтров): {total}")

            return {
                "page": page,
                "per_page": per_page,
                "total": total,
                "profiles": profiles_data,
            }

    except SQLAlchemyError as e:
        logger.error(f"Ошибка запроса к базе: {e}")
        raise HTTPException(status_code=500, detail="Ошибка базы данных, попробуйте позже.")

    except Exception as e:
        logger.error(f"Неизвестная ошибка: {e}")
        raise HTTPException(status_code=500, detail="Произошла ошибка, попробуйте позже.")


# Получить список юзеров по городу (поисковой запрос - показать юзеров в городе) с возможностью сортировки по новизне и популярности
async def get_profiles_by_city(city: str, page: int, sort_by: str, per_page: int):
    """ Логика для получения профилей пользователей по городу """
    try:
        async with get_db_session_for_worker() as session:  # Управление сессии внутри функции
            # Базовый запрос: исключаем приватные профили
            query = select(UserProfiles).filter(
                UserProfiles.city == city,
                UserProfiles.is_incognito == False,  # Исключаем приватные профили
            )

            logger.info(f"Запрос профилей для города: {city}, страница: {page}, сортировка: {sort_by}, профилей на странице: {per_page}")

            # Если указана сортировка
            if sort_by == "newest":
                query = query.order_by(desc(UserProfiles.created_at))
            elif sort_by == "popularity":
                query = query.order_by(desc(UserProfiles.followers_count))

            # Пагинация с учетом параметра per_page
            offset = (page - 1) * per_page
            query = query.offset(offset).limit(per_page)

            # Жадная загрузка через selectinload
            query = query.options(
                selectinload(UserProfiles.user),  # Жадная загрузка данных пользователя
                selectinload(UserProfiles.profile_hashtags).selectinload(ProfileHashtag.hashtag),  # Жадная загрузка хэштегов
            )

            # Получаем результат
            result = await session.execute(query)
            profiles = result.unique().scalars().all()

            # Получаем общее количество профилей
            total_query = select(func.count()).select_from(UserProfiles).filter(UserProfiles.city == city)
            total_result = await session.execute(total_query)
            total = total_result.scalar()

            # Обработка профилей
            profiles_data = []
            for profile in profiles:
                # Обработка координат
                coordinates = await process_coordinates_for_response(profile.coordinates)

                # Формирование структуры профиля
                profile_data = {
                    "id": profile.id,
                    "created_at": await datetime_to_str(profile.created_at),  # Преобразование даты в строку
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

            logger.info(f"Получено {len(profiles_data)} профилей для страницы {page}")
            return {
                "page": page,
                "per_page": per_page,
                "total": total,
                "profiles": profiles_data,
            }

    except SQLAlchemyError as e:
        logger.error(f"Ошибка выполнения запроса к базе данных: {e}")
        raise Exception("Ошибка базы данных, попробуйте позже.") from e

    except Exception as e:
        logger.error(f"Неизвестная ошибка: {e}")
        raise Exception("Произошла ошибка, попробуйте позже.") from e


# Получение пользователя по номеру кошелька
async def get_profile_by_wallet_number(wallet_number: str):
    """
    Логика получения профиля пользователя по номеру кошелька (асинхронно).

    :param wallet_number: Номер кошелька для поиска.
    :return: Словарь с информацией о профиле.
    :raises HTTPException: Если профиль не найден или произошла ошибка.
    """
    try:
        async with get_db_session_for_worker() as db:  # Открываем сессию внутри функции
            # Хэширование номера кошелька для поиска
            hashed_wallet_number = hashlib.sha256(wallet_number.encode()).hexdigest()
            logger.info(f"Ищем пользователя с хэшированным номером кошелька: {hashed_wallet_number}")

            # Поиск пользователя по хэшированному номеру кошелька
            query = select(User).filter(User.wallet_number == hashed_wallet_number).options(
                selectinload(User.profile).selectinload(UserProfiles.profile_hashtags).selectinload(ProfileHashtag.hashtag)
            )
            result = await db.execute(query)
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
                "coordinates": await process_coordinates_for_response(profile.coordinates),  # Обработка координат
                "followers_count": profile.followers_count,
                "created_at": await datetime_to_str(profile.created_at),  # Преобразование даты в строку
                "hashtags": [ph.hashtag.tag for ph in profile.profile_hashtags],  # Хэштеги текущего профиля
            }

            logger.info(f"Профиль пользователя с номером кошелька {wallet_number} успешно найден.")
            return profile_data

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Ошибка при получении профиля для кошелька {wallet_number}: {e}")
        raise HTTPException(status_code=500, detail="Ошибка сервера при получении профиля.")


# Получение пользователя по имени
async def get_profile_by_username(username: str) -> List[dict]:
    """
    Логика получения профилей пользователя по имени (асинхронно).
    Поиск осуществляется по полному совпадению имени, но регистронезависимо.

    :param username: Имя пользователя для поиска (полное совпадение, регистронезависимо).
    :return: Список словарей с информацией о профилях.
    :raises HTTPException: Если произошла ошибка.
    """
    try:
        async with get_db_session_for_worker() as db:  # Открываем сессию внутри функции
            # Поиск профилей по полному совпадению имени (регистронезависимый поиск)
            query = select(UserProfiles).filter(func.lower(UserProfiles.name) == func.lower(username)).options(
                selectinload(UserProfiles.profile_hashtags).selectinload(ProfileHashtag.hashtag)
            )
            result = await db.execute(query)
            profiles = result.scalars().all()

            if not profiles:
                logger.error(f"Профили с именем '{username}' (регистронезависимо) не найдены.")
                raise HTTPException(status_code=404, detail="Профили с таким именем не найдены.")

            # Формирование данных профилей для ответа
            profiles_data = []
            for profile in profiles:
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
                    "coordinates": await process_coordinates_for_response(profile.coordinates),  # Обработка координат
                    "followers_count": profile.followers_count,
                    "created_at": await datetime_to_str(profile.created_at),  # Преобразование даты в строку
                    "hashtags": [ph.hashtag.tag for ph in profile.profile_hashtags],  # Хэштеги текущего профиля
                }
                profiles_data.append(profile_data)

            logger.info(f"Найдено {len(profiles)} профилей с именем '{username}' (регистронезависимо).")
            return profiles_data

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Ошибка при получении профилей для имени '{username}': {e}")
        raise HTTPException(status_code=500, detail="Ошибка сервера при получении профилей.")


# Получение профилей в радиусе N километров
async def fetch_nearby_profiles(longitude: float, latitude: float, radius: int = 10000) -> List[dict]:
    try:
        if not isinstance(longitude, (int, float)) or not isinstance(latitude, (int, float)):
            raise ValueError("Координаты должны быть числами.")

        point = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
        logger.info(f"Поиск профилей в радиусе {radius} м от точки: ({longitude}, {latitude})")

        async with get_db_session_for_worker() as db:
            query = (
                select(UserProfiles)
                .where(ST_DWithin(UserProfiles.coordinates, point, radius))
                .options(selectinload(UserProfiles.profile_hashtags).selectinload(ProfileHashtag.hashtag))
            )

            logger.debug(f"SQL запрос: {str(query.compile(compile_kwargs={'literal_binds': True}))}")
            result = await db.execute(query)
            profiles = result.scalars().all()

            if not profiles:
                logger.info("Нет профилей в радиусе.")
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Нет профилей в радиусе")

            profile_list = []

            for profile in profiles:
                try:
                    if not profile.coordinates:
                        logger.warning(f"Профиль {profile.id} не имеет координат.")
                        continue

                    # Преобразуем координаты в объект Shapely
                    try:
                        geometry = to_shape(profile.coordinates)  # Преобразуем WKB в Shapely
                    except Exception as e:
                        logger.error(f"Ошибка при преобразовании координат профиля {profile.id}: {str(e)}")
                        continue

                    logger.debug(f"Тип геометрии профиля {profile.id}: {geometry.geom_type}")
                    points = []

                    # Обработка геометрии
                    if geometry.geom_type == "MultiPoint":
                        points = list(geometry.geoms)  # Извлекаем все точки из MultiPoint
                    elif geometry.geom_type == "Point":
                        points = [geometry]  # Добавляем единственную точку
                    else:
                        logger.warning(
                            f"Профиль {profile.id} имеет неподдерживаемый тип геометрии: {geometry.geom_type}")
                        continue

                    for point in points:
                        profile_longitude = float(point.x)
                        profile_latitude = float(point.y)

                        # Логируем полученные координаты
                        logger.debug(f"Координаты профиля {profile.id}: {profile_longitude}, {profile_latitude}")

                        # Вычисляем расстояние
                        distance = await calculate_distance(latitude, longitude, profile_latitude, profile_longitude)
                        logger.info(f"Расстояние до точки профиля {profile.id}: {distance / 1000:.2f} км.")

                        if distance <= radius:
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
                                "coordinates": {
                                    "longitude": profile_longitude,
                                    "latitude": profile_latitude,
                                },
                                "followers_count": profile.followers_count,
                                "created_at": await datetime_to_str(profile.created_at) if profile.created_at else None,
                                "hashtags": [ph.hashtag.tag for ph in profile.profile_hashtags],
                                "website_or_social": profile.website_or_social,
                                "is_admin": profile.is_admin,
                                "language": profile.language,
                            }
                            profile_list.append(profile_data)
                            break
                        else:
                            logger.info(f"Точка профиля {profile.id} не попала в радиус.")
                except Exception as e:
                    logger.error(f"Ошибка при обработке профиля {profile.id}: {str(e)}")

            return profile_list

    except Exception as e:
        logger.error(f"Ошибка при выполнении поиска: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка при выполнении поиска.")


# Функция для выборки и отправки профилей на модерацию
async def get_profiles_for_moderation(
    admin_wallet: str, page: int
) -> dict:
    """
    Получает профили для модерации, если запрос пришел от администратора.

    Параметры:
        admin_wallet (str): Кошелек администратора, который запрашивает профили.
        page (int): Номер страницы (начинается с 1).

    Возвращает:
        dict: Словарь с данными о профилях, включая пагинацию и общее количество.

    Исключения:
        HTTPException: Если запрос не от администратора или произошла ошибка.
    """
    try:
        async with get_db_session_for_worker() as session:
            # Хэшируем кошелек администратора
            hashed_admin_wallet = hashlib.sha256(admin_wallet.encode()).hexdigest()

            # Находим пользователя по хэшированному кошельку
            user_query = select(User).filter(User.wallet_number == hashed_admin_wallet)
            user_result = await session.execute(user_query)
            user = user_result.scalar()

            if not user:
                logger.warning(f"Пользователь с кошельком {admin_wallet} не найден.")
                raise HTTPException(status_code=404, detail="Пользователь не найден.")

            # Проверяем, что у пользователя есть профиль и флаг is_admin = True
            admin_profile_query = (
                select(UserProfiles)
                .filter(UserProfiles.user_id == user.id, UserProfiles.is_admin == True)
            )
            admin_profile_result = await session.execute(admin_profile_query)
            admin_profile = admin_profile_result.scalar()

            if not admin_profile:
                logger.warning(f"Пользователь {admin_wallet} не является администратором.")
                raise HTTPException(status_code=403, detail="Только администраторы могут запрашивать профили для модерации.")

            # Запрос для получения профилей на модерацию (is_moderated = False)
            query = (
                select(UserProfiles)
                .filter(UserProfiles.is_moderated == False)
                .options(
                    selectinload(UserProfiles.user),  # Жадная загрузка User
                    selectinload(UserProfiles.profile_hashtags).selectinload(ProfileHashtag.hashtag)
                )
            )

            # Пагинация: 25 профилей на страницу
            per_page = 25
            offset = (page - 1) * per_page
            query = query.offset(offset).limit(per_page)

            # Выполняем запрос
            result = await session.execute(query)
            profiles = result.scalars().all()

            # Формируем данные для ответа
            profiles_data = []
            for profile in profiles:
                coordinates = await process_coordinates_for_response(profile.coordinates)
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
                        "wallet_number": profile.user.wallet_number,  # Обращаемся к User.wallet_number
                    },
                    "hashtags": [ph.hashtag.tag for ph in profile.profile_hashtags],
                }
                profiles_data.append(profile_data)

            # Получаем общее количество профилей на модерацию
            total_query = select(func.count()).filter(UserProfiles.is_moderated == False)
            total_result = await session.execute(total_query)
            total = total_result.scalar()

            logger.info(f"Получено {len(profiles)} профилей для модерации, страница {page}")
            return {
                "page": page,
                "per_page": per_page,
                "total": total,
                "profiles": profiles_data,
            }

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Ошибка при получении профилей для модерации: {e}")
        raise HTTPException(status_code=500, detail="Ошибка сервера, попробуйте позже.")


# Даем права админа с кошелька босса
async def grant_admin_rights(request_wallet: str, target_wallet: str) -> bool:
    """
    Дает права администратора пользователю, если запрос пришел с правильного кошелька.

    Параметры:
        request_wallet (str): Кошелек, с которого поступил запрос.
        target_wallet (str): Кошелек, которому нужно дать права администратора.

    Возвращает:
        bool: True, если права успешно выданы, иначе False.

    Исключения:
        HTTPException: Если что-то пошло не так.
    """
    # Открываем сессию внутри функции
    async with get_db_session_for_worker() as db:
        try:
            # Хэшируем кошелек, с которого пришел запрос
            hashed_request_wallet = hashlib.sha256(request_wallet.encode()).hexdigest()

            # Хэшируем кошелек целевого пользователя
            hashed_target_wallet = hashlib.sha256(target_wallet.encode()).hexdigest()

            # Проверяем, совпадает ли хэш кошелька, с которого пришел запрос, с royal_wallet
            if hashed_request_wallet != ROYAL_WALLET:
                logger.warning(f"Неавторизованный запрос от кошелька: {request_wallet}")
                return False

            # Находим пользователя по хэшированному кошельку в таблице User
            result = await db.execute(
                select(User).filter(User.wallet_number == hashed_target_wallet)
            )
            user = result.scalar_one_or_none()

            if not user:
                logger.warning(f"Пользователь с кошельком {target_wallet} не найден.")
                return False

            # Находим профиль пользователя по user_id в таблице UserProfiles
            profile_result = await db.execute(
                select(UserProfiles).filter(UserProfiles.user_id == user.id)
            )
            user_profile = profile_result.scalar_one_or_none()

            if not user_profile:
                logger.warning(f"Профиль для пользователя с кошельком {target_wallet} не найден.")
                return False

            # Обновляем флаг is_admin для целевого профиля
            stmt = (
                update(UserProfiles)
                .where(UserProfiles.id == user_profile.id)
                .values(is_admin=True)
            )

            await db.execute(stmt)
            await db.commit()

            logger.info(f"Права администратора успешно выданы для кошелька: {target_wallet}")
            return True

        except Exception as e:
            logger.error(f"Ошибка при выдаче прав администратора: {e}")
            await db.rollback()
            raise HTTPException(status_code=500, detail="Ошибка сервера при выдаче прав администратора.")


# Описание логики модерации
async def moderate_profile(
    admin_wallet: str,  # Нехэшированный кошелек администратора
    profile_id: int,  # ID профиля для модерации
    moderation: bool,  # True — профиль прошел модерацию, False — не прошел
) -> dict:
    """
    Модерирует профиль пользователя.

    Параметры:
        admin_wallet (str): Нехэшированный кошелек администратора.
        profile_id (int): ID профиля для модерации.
        moderation (bool): Результат модерации (True — одобрено, False — отклонено).

    Возвращает:
        dict: Сообщение о результате модерации.

    Исключения:
        HTTPException: Если запрос не от администратора или произошла ошибка.
    """
    try:
        # Хэшируем кошелек администратора
        hashed_admin_wallet = hashlib.sha256(admin_wallet.encode()).hexdigest()

        # Открываем сессию через get_db_session_for_worker
        async with get_db_session_for_worker() as session:
            # Проверяем, что запрос пришел от администратора
            admin_query = select(UserProfiles).join(User).filter(
                User.wallet_number == hashed_admin_wallet,
                UserProfiles.is_admin == True
            )
            admin_result = await session.execute(admin_query)
            admin_profile = admin_result.scalar()

            if not admin_profile:
                logger.warning(f"Неавторизованный от кошелька запрос : {admin_wallet}")
                raise HTTPException(status_code=403, detail="Только администраторы могут модерировать профили, да.")

            # Находим профиль для модерации по ID
            target_profile_query = select(UserProfiles).filter(
                UserProfiles.id == profile_id
            )
            target_profile_result = await session.execute(target_profile_query)
            target_profile = target_profile_result.scalar()

            if not target_profile:
                logger.warning(f"Профиль с ID {profile_id} не найден, да.")
                raise HTTPException(status_code=404, detail="Профиль не найден, да.")

            # Обновляем профиль в зависимости от результата модерации
            if moderation:
                # Профиль прошел модерацию, да
                stmt = (
                    update(UserProfiles)
                    .where(UserProfiles.id == profile_id)
                    .values(is_moderated=True)
                )
                message = "Профиль успешно прошел модерацию, да."
            else:
                # Профиль не прошел модерацию, да
                stmt = (
                    update(UserProfiles)
                    .where(UserProfiles.id == profile_id)
                    .values(is_moderated=False, is_incognito=True)
                )
                message = "Профиль не прошел модерацию и теперь скрыт, да."

            await session.execute(stmt)
            await session.commit()

            logger.info(f"Профиль с ID {profile_id} был обновлен: {message}")
            return {"message": message}

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Ошибка при модерации профиля: {e}")
        raise HTTPException(status_code=500, detail="Ошибка сервера при модерации профиля, да.")

