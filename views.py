import os
import shutil
import aiofiles
import hashlib
import random
from math import ceil # Импортируем ceil для округления вверх
from dotenv import load_dotenv
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
from sqlalchemy.sql.expression import not_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import desc, func, bindparam, Integer, update, and_
import redis.asyncio as redis
from redis.exceptions import RedisError

from models import UserProfiles, Hashtag, ProfileHashtag, User
from database import get_db_session_for_worker
from utils import process_coordinates_for_response, datetime_to_str, get_file_size, calculate_distance, generate_unique_link
from cashe import get_favorites_from_cache

from logging_config import get_logger

logger = get_logger()

load_dotenv()

# Big Boss Royal Wallet Executor (Ты знаешь для чего)
ROYAL_WALLET = os.getenv("ROYAL_WALLET")

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


# Получение всех профилей по тому же алгоритму или сортировке по новизне/популярности
async def get_all_profiles(
    page: int,
    sort_by: Optional[str] = None,  # Просто строка без Query
    per_page: int = 50  # Просто число без Query
) -> dict:
    """
    Получает все профили с пагинацией и сортировкой.

    :param page: Номер страницы (начинается с 1).
    :param sort_by: Параметр сортировки (newest, popularity).
    :param per_page: Количество профилей на страницу.
    :return: Словарь с данными о профилях, включая пагинацию и общее количество.
    :raises HTTPException: Если произошла ошибка при выполнении запроса.
    """
    try:
        async with get_db_session_for_worker() as session:
            logger.info(f"Запрос профилей: page={page}, per_page={per_page} ({type(per_page)}), sort_by={sort_by}")

            # Принудительное приведение к int, если FastAPI каким-то образом передал Query
            if not isinstance(per_page, int):
                per_page = int(per_page)
                logger.warning(f"per_page приведён к int: {per_page}")

            # Базовый запрос
            base_query = (
                select(UserProfiles)
                .options(
                    joinedload(UserProfiles.user),
                    subqueryload(UserProfiles.profile_hashtags).subqueryload(ProfileHashtag.hashtag)
                )
            )

            # Применение сортировки
            if sort_by == "newest":
                query = base_query.order_by(desc(UserProfiles.created_at))
                logger.info(f"Сортировка по новизне, страница {page}")
            elif sort_by == "popularity":
                query = base_query.order_by(desc(UserProfiles.followers_count))
                logger.info(f"Сортировка по популярности, страница {page}")
            else:
                logger.info(f"Сортировка не указана, применяем алгоритм, страница {page}")

                # Собираем 10 самых новых
                newest_profiles = (await session.execute(
                    base_query.order_by(desc(UserProfiles.created_at)).limit(10)
                )).scalars().all()
                logger.info(f"Собрано {len(newest_profiles)} самых новых профилей")

                # Собираем 10 самых популярных
                popular_profiles = (await session.execute(
                    base_query.order_by(desc(UserProfiles.followers_count)).limit(10)
                )).scalars().all()
                logger.info(f"Собрано {len(popular_profiles)} самых популярных профилей")

                # Собираем 10 профилей, где is_in_mlm != 0
                mlm_profiles_query = base_query.filter(
                    and_(
                        UserProfiles.is_in_mlm != 0,
                        UserProfiles.is_in_mlm.isnot(None)
                    )
                ).limit(10)
                mlm_profiles = (await session.execute(mlm_profiles_query)).scalars().all()
                logger.info(f"Собрано {len(mlm_profiles)} профилей с is_in_mlm != 0")

                # Собираем 10 профилей, где video_url не None
                video_profiles_query = base_query.filter(
                    UserProfiles.video_url.isnot(None)
                ).limit(10)
                video_profiles = (await session.execute(video_profiles_query)).scalars().all()
                logger.info(f"Собрано {len(video_profiles)} профилей с видео")

                # Собираем 10 случайных профилей
                random_profiles = (await session.execute(
                    base_query.order_by(func.random()).limit(10)
                )).scalars().all()
                logger.info(f"Собрано {len(random_profiles)} случайных профилей")

                # Объединяем все профили и убираем дубликаты
                profiles = newest_profiles + popular_profiles + mlm_profiles + video_profiles + random_profiles
                unique_profiles = list({profile.id: profile for profile in profiles}.values())
                logger.info(f"Общее количество уникальных профилей после объединения: {len(unique_profiles)}")

                # Добиваем до per_page случайными профилями, если нужно
                if len(unique_profiles) < per_page:
                    remaining_profiles_query = base_query.filter(
                        not_(UserProfiles.id.in_([p.id for p in unique_profiles]))
                    )
                    remaining_profiles = (await session.execute(remaining_profiles_query)).scalars().all()

                    if remaining_profiles:
                        needed = per_page - len(unique_profiles)
                        unique_profiles.extend(random.sample(remaining_profiles, min(needed, len(remaining_profiles))))
                        logger.info(f"Добавлено {len(remaining_profiles)} оставшихся профилей для достижения per_page")

                # Получаем все оставшиеся профили, которые не попали в выборку по алгоритму
                remaining_all_profiles_query = base_query.filter(
                    not_(UserProfiles.id.in_([p.id for p in unique_profiles]))
                )
                remaining_all_profiles = (await session.execute(remaining_all_profiles_query)).scalars().all()

                # Объединяем все профили: сначала те, что по алгоритму, потом оставшиеся
                all_profiles = unique_profiles + remaining_all_profiles
                logger.info(f"Общее количество всех профилей после добавления оставшихся: {len(all_profiles)}")

                # Формируем страницы
                pages = [all_profiles[i:i + per_page] for i in range(0, len(all_profiles), per_page)]
                total_pages = ceil(len(all_profiles) / per_page)  # Используем ceil для округления вверх
                logger.info(f"Всего страниц: {total_pages}")

                # Если страница выходит за пределы
                if page > total_pages:
                    logger.info(f"Профили просмотрены. Запрошенная страница ({page}) превышает {total_pages}.")
                    return {
                        "theme": "Макс, это для тебя корешок ^^",
                        "page_number": page,
                        "total_profiles": len(all_profiles),
                        "total_pages": total_pages,
                        "message": "Профили просмотрены. Начните с первой страницы.",
                        "profiles": [],
                    }

                # Получаем профили для текущей страницы
                current_page_profiles = pages[page - 1]

                # Проверяем, является ли текущая страница последней и неполной
                is_last_page = page == total_pages
                is_incomplete_page = len(current_page_profiles) < per_page

                # Формируем сообщение
                offset = (page - 1) * per_page
                end_index = offset + len(current_page_profiles)  # Корректный конечный индекс
                message = f"Показаны профили {offset + 1}-{end_index} из {len(all_profiles)}."

                if is_last_page and is_incomplete_page:
                    message += " Это последняя страница. Начните просмотр профилей со страницы номер 1."

                # Формируем ответ
                profiles_data = [{
                    "id": profile.id,
                    "created_at": await datetime_to_str(profile.created_at),
                    "name": profile.name,
                    "user_logo_url": profile.user_logo_url,
                    "video_url": profile.video_url,
                    "preview_url": profile.preview_url,
                    "poster_url": profile.poster_url,
                    "activity_and_hobbies": profile.activity_and_hobbies,
                    "is_moderated": profile.is_moderated,
                    "is_incognito": profile.is_incognito,
                    "is_in_mlm": profile.is_in_mlm,
                    "adress": profile.adress,
                    "coordinates": await process_coordinates_for_response(profile.coordinates),
                    "followers_count": profile.followers_count,
                    "website_or_social": profile.website_or_social,
                    "user_link": profile.user_link,
                    "is_adult_content": profile.is_adult_content,
                    "user": {
                        "id": profile.user.id,
                        "wallet_number": profile.user.wallet_number,
                    },
                    "hashtags": [ph.hashtag.tag for ph in profile.profile_hashtags if ph.hashtag is not None],
                } for profile in current_page_profiles]

                return {
                    "theme": "Макс, это для тебя корешок ^^",
                    "page_number": page,
                    "total_profiles": len(all_profiles),
                    "total_pages": total_pages,
                    "message": message,
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
            total_query = select(func.count()).select_from(UserProfiles).filter(
                UserProfiles.city == city,
                UserProfiles.is_incognito == False,  # Исключаем приватные профили
            )
            total_result = await session.execute(total_query)
            total = total_result.scalar()

            # Рассчитываем общее количество страниц
            total_pages = (total + per_page - 1) // per_page

            # Формируем сообщение о пагинации
            if total == 0:
                message = "Нет профилей для отображения."
            else:
                start_index = (page - 1) * per_page + 1
                end_index = min(page * per_page, total)
                if start_index > end_index:
                    message = "Нет профилей для отображения на этой странице."
                else:
                    message = f"Показаны профили {start_index}-{end_index} из {total}."

            # Проверяем, является ли текущая страница последней и неполной
            is_last_page = page == total_pages
            is_incomplete_page = len(profiles) < per_page

            if is_last_page and is_incomplete_page:
                message += " Это последняя страница. Начните просмотр профилей со страницы номер 1."

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
                    "poster_url": profile.poster_url,
                    "activity_and_hobbies": profile.activity_and_hobbies,
                    "is_moderated": profile.is_moderated,
                    "is_incognito": profile.is_incognito,
                    "is_in_mlm": profile.is_in_mlm,
                    "adress": profile.adress,
                    "coordinates": coordinates,
                    "followers_count": profile.followers_count,
                    "website_or_social": profile.website_or_social,
                    "user_link": profile.user_link,
                    "is_adult_content": profile.is_adult_content,
                    "user": {
                        "id": profile.user.id,
                        "wallet_number": profile.user.wallet_number,
                    },
                    "hashtags": [ph.hashtag.tag for ph in profile.profile_hashtags],  # Только хэштеги текущего профиля
                }
                profiles_data.append(profile_data)

            logger.info(f"Получено {len(profiles_data)} профилей для страницы {page}")
            return {
                "theme": "Макс, это для тебя корешок ^^",  # Добавляем тему
                "page_number": page,  # Номер текущей страницы
                "total_profiles": total,  # Общее количество профилей
                "total_pages": total_pages,  # Общее количество страниц
                "message": message,  # Сообщение о пагинации
                "profiles": profiles_data,  # Список профилей
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
            # Поиск пользователя по номеру кошелька
            query = select(User).filter(User.wallet_number == wallet_number).options(
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
                "poster_url": profile.poster_url,
                "activity_and_hobbies": profile.activity_and_hobbies,
                "is_moderated": profile.is_moderated,
                "is_incognito": profile.is_incognito,
                "is_in_mlm": profile.is_in_mlm,
                "adress": [profile.adress] if profile.adress else [],  # Преобразуем в список
                "city": profile.city,
                "coordinates": await process_coordinates_for_response(profile.coordinates),
                "followers_count": profile.followers_count,
                "created_at": await datetime_to_str(profile.created_at),
                "hashtags": [ph.hashtag.tag for ph in profile.profile_hashtags],
                "website_or_social": profile.website_or_social,
                "is_admin": profile.is_admin,
                "language": profile.language,
                "user_link": profile.user_link,
                "is_adult_content": profile.is_adult_content,
                "user": {
                    "id": user.id,
                    "wallet_number": user.wallet_number
                }
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
    Поиск осуществляется по полному совпадению имени (регистронезависимо),
    с нормализацией пробелов и учетом е/ё как одинаковых букв.

    :param username: Имя пользователя для поиска
    :return: Список словарей с информацией о профилях
    :raises HTTPException: Если произошла ошибка
    """
    try:
        # Функция для нормализации строки поиска
        def normalize_search_term(term: str) -> str:
            term = term.strip().lower()
            term = ' '.join(term.split())  # Нормализация пробелов
            term = term.replace('ё', 'е')  # Для поиска считаем ё == е
            return term

        # Функция для сравнения имен в БД
        def normalize_db_name(name: str) -> str:
            name = name.strip().lower()
            name = ' '.join(name.split())  # Нормализация пробелов
            name = name.replace('ё', 'е')  # Для поиска считаем ё == е
            return name

        search_term = normalize_search_term(username)

        async with get_db_session_for_worker() as db:
            # Создаем SQL-функцию для нормализации имен в БД
            normalized_name = func.lower(
                func.regexp_replace(
                    func.regexp_replace(
                        func.trim(UserProfiles.name),
                        '\s+', ' ', 'g'
                    ),
                    'ё', 'е', 'g'
                )
            )

            query = select(UserProfiles).filter(
                normalized_name == search_term
            ).options(
                selectinload(UserProfiles.profile_hashtags).selectinload(ProfileHashtag.hashtag),
                selectinload(UserProfiles.user)
            )

            result = await db.execute(query)
            profiles = result.scalars().all()

            if not profiles:
                logger.error(f"Профили с именем '{username}' (нормализовано: '{search_term}') не найдены.")
                raise HTTPException(status_code=404, detail="Профили с таким именем не найдены.")

            # Формирование данных профилей
            profiles_data = []
            for profile in profiles:
                profile_data = {
                    "id": profile.id,
                    "name": profile.name,  # Оригинальное имя (без нормализации)
                    "user_logo_url": profile.user_logo_url,
                    "video_url": profile.video_url,
                    "preview_url": profile.preview_url,
                    "poster_url": profile.poster_url,
                    "activity_and_hobbies": profile.activity_and_hobbies,
                    "is_moderated": profile.is_moderated,
                    "is_incognito": profile.is_incognito,
                    "is_in_mlm": profile.is_in_mlm,
                    "adress": [profile.adress] if profile.adress else [],
                    "city": profile.city,
                    "coordinates": await process_coordinates_for_response(profile.coordinates),
                    "followers_count": profile.followers_count,
                    "created_at": await datetime_to_str(profile.created_at),
                    "hashtags": [ph.hashtag.tag for ph in profile.profile_hashtags],
                    "website_or_social": profile.website_or_social,
                    "is_admin": profile.is_admin,
                    "language": profile.language,
                    "user_link": profile.user_link,
                    "is_adult_content": profile.is_adult_content,
                    "user": {
                        "id": profile.user.id,
                        "wallet_number": profile.user.wallet_number
                    }
                }
                profiles_data.append(profile_data)

            logger.info(f"Найдено {len(profiles)} профилей для запроса '{username}' (нормализовано: '{search_term}')")
            return profiles_data

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Ошибка при поиске профилей по имени '{username}': {e}")
        raise HTTPException(
            status_code=500,
            detail="Ошибка сервера при поиске профилей."
        )


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
                .options(
                    selectinload(UserProfiles.profile_hashtags).selectinload(ProfileHashtag.hashtag),
                    selectinload(UserProfiles.user)
                )
            )

            logger.debug(f"SQL запрос: {str(query.compile(compile_kwargs={{'literal_binds': True}}))}")
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
                        geometry = to_shape(profile.coordinates)
                    except Exception as e:
                        logger.error(f"Ошибка при преобразовании координат профиля {profile.id}: {str(e)}")
                        continue

                    logger.debug(f"Тип геометрии профиля {profile.id}: {geometry.geom_type}")
                    points = []

                    if geometry.geom_type == "MultiPoint":
                        points = list(geometry.geoms)
                    elif geometry.geom_type == "Point":
                        points = [geometry]
                    else:
                        logger.warning(f"Профиль {profile.id} имеет неподдерживаемый тип геометрии: {geometry.geom_type}")
                        continue

                    for point in points:
                        profile_longitude = float(point.x)
                        profile_latitude = float(point.y)

                        logger.debug(f"Координаты профиля {profile.id}: {profile_longitude}, {profile_latitude}")

                        distance = await calculate_distance(latitude, longitude, profile_latitude, profile_longitude)
                        logger.info(f"Расстояние до точки профиля {profile.id}: {distance / 1000:.2f} км.")

                        if distance <= radius:
                            profile_data = {
                                "id": profile.id,
                                "name": profile.name,
                                "user_logo_url": profile.user_logo_url,
                                "video_url": profile.video_url,
                                "preview_url": profile.preview_url,
                                "poster_url": profile.poster_url,
                                "activity_and_hobbies": profile.activity_and_hobbies,
                                "is_moderated": profile.is_moderated,
                                "is_incognito": profile.is_incognito,
                                "is_in_mlm": profile.is_in_mlm,
                                "adress": [profile.adress] if profile.adress else [],  # Преобразуем в список
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
                                "user_link": profile.user_link,
                                "is_adult_content": profile.is_adult_content,
                                "user": {  # Добавляем информацию о пользователе
                                    "id": profile.user.id,
                                    "wallet_number": profile.user.wallet_number
                                }
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
    user_id: int,  # ID пользователя из токена
    page: int = 1,
    per_page: int = 50
) -> dict:
    """
    Получает профили для модерации, если запрос пришел от администратора.

    Параметры:
        user_id (int): ID пользователя, который запрашивает профили.
        page (int): Номер страницы (начинается с 1).
        per_page (int): Количество профилей на страницу (50–100).

    Возвращает:
        dict: Словарь с данными о профилях, включая пагинацию и общее количество.

    Исключения:
        HTTPException: Если запрос не от администратора или произошла ошибка.
    """
    try:
        async with get_db_session_for_worker() as session:
            # Проверяем, что пользователь является администратором
            admin_profile_query = (
                select(UserProfiles)
                .filter(UserProfiles.user_id == user_id, UserProfiles.is_admin == True)
            )
            admin_profile_result = await session.execute(admin_profile_query)
            admin_profile = admin_profile_result.scalar()

            if not admin_profile:
                logger.warning(f"Пользователь {user_id} не является администратором.")
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

            # Пагинация
            if per_page < 50 or per_page > 100:
                raise HTTPException(status_code=400, detail="Параметр per_page должен быть в диапазоне 50–100.")

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
                    "poster_url": profile.poster_url,
                    "activity_and_hobbies": profile.activity_and_hobbies,
                    "is_moderated": profile.is_moderated,
                    "is_incognito": profile.is_incognito,
                    "is_in_mlm": profile.is_in_mlm,
                    "adress": profile.adress,
                    "coordinates": coordinates,
                    "followers_count": profile.followers_count,
                    "website_or_social": profile.website_or_social,
                    "user_link": profile.user_link,
                    "is_adult_content": profile.is_adult_content,
                    "user": {
                        "id": profile.user.id,
                        "wallet_number": profile.user.wallet_number,
                    },
                    "hashtags": [ph.hashtag.tag for ph in profile.profile_hashtags],
                }
                profiles_data.append(profile_data)

            # Получаем общее количество профилей на модерацию
            total_query = select(func.count()).filter(UserProfiles.is_moderated == False)
            total_result = await session.execute(total_query)
            total_profiles = total_result.scalar()

            # Рассчитываем общее количество страниц
            total_pages = (total_profiles + per_page - 1) // per_page

            # Формируем сообщение о пагинации
            if total_profiles == 0:
                message = "Нет профилей для модерации."
            else:
                start_index = (page - 1) * per_page + 1
                end_index = min(page * per_page, total_profiles)
                if start_index > end_index:
                    message = "Нет профилей для отображения на этой странице."
                else:
                    message = f"Показаны профили {start_index}-{end_index} из {total_profiles}."

            # Проверяем, является ли текущая страница последней и неполной
            is_last_page = page == total_pages
            is_incomplete_page = len(profiles) < per_page

            if is_last_page and is_incomplete_page:
                message += " Это последняя страница. Начните просмотр профилей со страницы номер 1."

            logger.info(f"Получено {len(profiles)} профилей для модерации, страница {page}")
            return {
                "theme": "Макс, это для тебя корешок ^^",  # Добавляем тему
                "page_number": page,  # Номер текущей страницы
                "total_profiles": total_profiles,  # Общее количество профилей
                "total_pages": total_pages,  # Общее количество страниц
                "message": message,  # Сообщение о пагинации
                "profiles": profiles_data,  # Список профилей
            }

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Ошибка при получении профилей для модерации: {e}")
        raise HTTPException(status_code=500, detail="Ошибка сервера, попробуйте позже.")


# Даем права админа с кошелька босса
async def grant_admin_rights(user_id: int, target_wallet: str) -> bool:
    """
    Дает права администратора пользователю, если запрос пришел от пользователя с ROYAL_WALLET.

    Параметры:
        user_id (int): ID пользователя, который запрашивает выдачу прав.
        target_wallet (str): Кошелек, которому нужно дать права администратора.

    Возвращает:
        bool: True, если права успешно выданы, иначе False.

    Исключения:
        HTTPException: Если что-то пошло не так.
    """
    # Открываем сессию внутри функции
    async with get_db_session_for_worker() as db:
        try:
            # Находим пользователя по user_id
            result = await db.execute(
                select(User).filter(User.id == user_id)
            )
            user = result.scalar_one_or_none()

            if not user:
                logger.warning(f"Пользователь с ID {user_id} не найден.")
                return False

            # Хэшируем кошелек пользователя, который запрашивает выдачу прав
            hashed_user_wallet = hashlib.sha256(user.wallet_number.encode()).hexdigest()

            # Проверяем, совпадает ли хэшированный кошелек пользователя с ROYAL_WALLET
            if hashed_user_wallet != ROYAL_WALLET:
                logger.warning(f"Неавторизованный запрос от пользователя с ID {user_id}.")
                return False

            # Находим целевого пользователя по кошельку (target_wallet уже хэширован в базе)
            target_result = await db.execute(
                select(User).filter(User.wallet_number == target_wallet)
            )
            target_user = target_result.scalar_one_or_none()

            if not target_user:
                logger.warning(f"Пользователь с кошельком {target_wallet} не найден.")
                return False

            # Находим профиль целевого пользователя
            profile_result = await db.execute(
                select(UserProfiles).filter(UserProfiles.user_id == target_user.id)
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
    user_id: int,  # ID администратора из токена
    profile_id: int,  # ID профиля для модерации
    moderation: bool,  # True — профиль прошел модерацию, False — не прошел
    is_adult_content: bool   # флаг 18+ контента
) -> dict:
    """
    Модерирует профиль пользователя с учетом флага взрослого контента.

    Параметры:
        user_id (int): ID администратора.
        profile_id (int): ID профиля для модерации.
        moderation (bool): Результат модерации (True — одобрено, False — отклонено).
        is_adult_content (bool): Флаг взрослого контента (по умолчанию False).

    Возвращает:
        dict: Сообщение о результате модерации.

    Исключения:
        HTTPException: Если запрос не от администратора или произошла ошибка.
    """
    async with get_db_session_for_worker() as session:
        try:
            # Проверяем, что запрос пришел от администратора
            admin_query = select(UserProfiles).filter(
                UserProfiles.user_id == user_id,
                UserProfiles.is_admin == True
            )
            admin_result = await session.execute(admin_query)
            admin_profile = admin_result.scalar()

            if not admin_profile:
                logger.warning(f"Пользователь с ID {user_id} не является администратором.")
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
            update_values = {
                "is_adult_content": is_adult_content  # Всегда обновляем флаг взрослого контента
            }

            if moderation:
                # Профиль прошел модерацию
                update_values["is_moderated"] = True
                message = f"Профиль успешно прошел модерацию (18+: {is_adult_content}), да."
            else:
                # Профиль не прошел модерацию
                update_values.update({
                    "is_moderated": False,
                    "is_incognito": True
                })
                message = "Профиль не прошел модерацию и теперь скрыт, да."

            stmt = (
                update(UserProfiles)
                .where(UserProfiles.id == profile_id)
                .values(**update_values)
            )

            await session.execute(stmt)
            await session.commit()

            logger.info(f"Профиль с ID {profile_id} был обновлен: {message}")
            return {"message": message}

        except HTTPException as e:
            await session.rollback()
            raise e
        except Exception as e:
            await session.rollback()
            logger.error(f"Ошибка при модерации профиля: {e}")
            raise HTTPException(status_code=500, detail="Ошибка сервера при модерации профиля, да.")


# Логика генерации новой ссылки для профиля и формирование нового ответа на фронт
async def regenerate_user_link(profile_id: int):
    """
    Генерирует новую уникальную ссылку для профиля, сохраняет в БД и возвращает полные данные профиля
    с корректной загрузкой хэштегов и избранного

    :param profile_id: ID профиля для обновления
    :return: Полные данные профиля с новой ссылкой в требуемом формате
    """
    async with get_db_session_for_worker() as session:
        try:
            # 1. Получаем профиль из БД с загрузкой хэштегов
            stmt = select(UserProfiles).where(UserProfiles.id == profile_id).options(
                selectinload(UserProfiles.profile_hashtags).selectinload(ProfileHashtag.hashtag)
            )
            result = await session.execute(stmt)
            profile = result.scalars().first()

            if not profile:
                raise HTTPException(status_code=404, detail="Профиль не найден")

            # 2. Получаем пользователя
            stmt_user = select(User).where(User.id == profile.user_id)
            result_user = await session.execute(stmt_user)
            user = result_user.scalars().first()

            if not user:
                raise HTTPException(status_code=404, detail="Пользователь не найден")

            # 3. Генерируем новую ссылку
            new_user_link = await generate_unique_link()

            # 4. Обновляем ссылку в профиле
            profile.user_link = new_user_link
            session.add(profile)
            await session.commit()

            # 5. Формируем данные профиля для ответа (как в /api/user/login)
            coordinates = await process_coordinates_for_response(profile.coordinates) if profile.coordinates else None
            created_at_str = await datetime_to_str(profile.created_at) if profile.created_at else None

            profile_data = {
                "id": profile.id,
                "name": profile.name,
                "user_logo_url": profile.user_logo_url,
                "video_url": profile.video_url,
                "preview_url": profile.preview_url,
                "poster_url": profile.poster_url,
                "activity_and_hobbies": profile.activity_and_hobbies,
                "is_moderated": profile.is_moderated,
                "is_incognito": profile.is_incognito,
                "is_in_mlm": profile.is_in_mlm,
                "adress": profile.adress,
                "city": profile.city,
                "coordinates": coordinates,
                "followers_count": profile.followers_count,
                "created_at": created_at_str,
                "hashtags": [ph.hashtag.tag for ph in profile.profile_hashtags],
                "website_or_social": profile.website_or_social,
                "is_admin": profile.is_admin,
                "language": profile.language,
                "user_link": profile.user_link,
                "is_adult_content": profile.is_adult_content
            }

            # 6. Получаем избранное (полные данные профилей, а не только ID)
            favorites = await get_favorites_from_cache(user.id)
            logger.info(f"Получено избранное: {[fav['id'] for fav in favorites]}")

            # 7. Формируем итоговый ответ в том же формате, что и /api/user/login
            response_data = {
                "id": user.id,
                "profile": profile_data,
                "favorites": favorites  # Полные данные избранных профилей
            }

            return response_data

        except SQLAlchemyError as e:
            await session.rollback()
            logger.error(f"Ошибка базы данных: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Ошибка работы с базой данных"
            )
        except Exception as e:
            await session.rollback()
            logger.error(f"Непредвиденная ошибка: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Ошибка при обработке запроса"
            )


# Логика получения профиля по ссылке
async def get_profile_by_link(user_link: str):
    """
    Получает полные данные профиля по уникальной ссылке

    :param user_link: Уникальная ссылка профиля
    :return: Данные профиля в том же формате, что и regenerate_user_link
    """
    async with get_db_session_for_worker() as session:
        try:
            # 1. Получаем профиль из БД с загрузкой хэштегов
            stmt = select(UserProfiles).where(
                UserProfiles.user_link == user_link
            ).options(
                selectinload(UserProfiles.profile_hashtags).selectinload(ProfileHashtag.hashtag),
                selectinload(UserProfiles.user)
            )
            result = await session.execute(stmt)
            profile = result.scalars().first()

            if not profile:
                raise HTTPException(status_code=404, detail="Профиль по данной ссылке не найден")

            # 2. Получаем связанного пользователя
            user = profile.user
            if not user:
                raise HTTPException(status_code=404, detail="Пользователь не найден")

            # 3. Формируем данные профиля для ответа
            coordinates = await process_coordinates_for_response(profile.coordinates) if profile.coordinates else None
            created_at_str = await datetime_to_str(profile.created_at) if profile.created_at else None

            profile_data = {
                "id": profile.id,
                "name": profile.name,
                "user_logo_url": profile.user_logo_url,
                "video_url": profile.video_url,
                "preview_url": profile.preview_url,
                "poster_url": profile.poster_url,
                "activity_and_hobbies": profile.activity_and_hobbies,
                "is_moderated": profile.is_moderated,
                "is_incognito": profile.is_incognito,
                "is_in_mlm": profile.is_in_mlm,
                "adress": profile.adress,
                "city": profile.city,
                "coordinates": coordinates,
                "followers_count": profile.followers_count,
                "created_at": created_at_str,
                "hashtags": [ph.hashtag.tag for ph in profile.profile_hashtags],
                "website_or_social": profile.website_or_social,
                "is_admin": profile.is_admin,
                "language": profile.language,
                "user_link": profile.user_link,
                "is_adult_content": profile.is_adult_content
            }

            # 4. Получаем избранное (для текущего пользователя, если авторизован)
            favorites = []
            if user:
                favorites = await get_favorites_from_cache(user.id)
                logger.info(f"Получено избранное для пользователя {user.id}")

            # 5. Формируем итоговый ответ
            response_data = {
                "id": user.id,
                "profile": profile_data,
                "favorites": favorites
            }

            return response_data

        except SQLAlchemyError as e:
            logger.error(f"Ошибка базы данных: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Ошибка работы с базой данных"
            )
        except Exception as e:
            logger.error(f"Непредвиденная ошибка: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Ошибка при обработке запроса"
            )