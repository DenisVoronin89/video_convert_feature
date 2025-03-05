""" Модуль для описания работы с кэшем: кэш счетчика подписчиков и кэш добавленных в избранное,
    получение 50 профилей на первоначальную отдачу клиентам.
    Описание логики актуализации данных в кэше Redis и актуализации данных в БД """

import random
import os
from datetime import timedelta
import json
import hashlib
from math import ceil
from fastapi import HTTPException, status
import redis.asyncio as redis
from pydantic import HttpUrl
from redis.exceptions import RedisError
from typing import Optional
from sqlalchemy import func, desc, or_, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import OperationalError, IntegrityError, SQLAlchemyError
from sqlalchemy.orm import joinedload, subqueryload, selectinload
from sqlalchemy.future import select
from geoalchemy2.shape import to_shape
from shapely.wkt import loads as wkt_loads
from shapely.geometry import Point, MultiPoint
from typing import List, Dict, Set, Tuple

from logging_config import get_logger
from database import get_db_session, get_db_session_for_worker
from models import UserProfiles, Favorite, Hashtag, ProfileHashtag, User
from utils import datetime_to_str, process_coordinates_for_response, parse_coordinates
from views import move_image_to_user_logo
from schemas import serialize_form_data, FormData


logger = get_logger()

# Настраиваем соединение с Redis
redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True) # Надо ли этот тут?

# Константы для TTL пользователей, пагинированных страниц с профилями, сортированных по новизне/популярности сетов (12 часов)
CACHE_PROFILES_TTL_SEK = 43200


# ЛОГИКА РАБОТЫ С ИЗБРАННЫМ

# Увеличить счётчик подписчиков
async def increment_subscribers_count(profile_id: int):
    """
    Увеличить количество подписчиков на 1.

    :param profile_id: ID профиля.
    :return: Новое количество подписчиков.
    """
    try:
        # Проверяем, существует ли ключ для счетчика подписчиков
        if not await redis_client.exists(f'subscribers_count:{profile_id}'):
            # Если ключа нет, создаем его с начальным значением 0
            await redis_client.set(f'subscribers_count:{profile_id}', 0)

        # Увеличиваем счетчик подписчиков на 1
        new_count = await redis_client.incr(f'subscribers_count:{profile_id}')

        # Логируем результат
        logger.info(f"Количество подписчиков профиля {profile_id} увеличено. Новое значение: {new_count}")

        return new_count
    except Exception as e:
        # Логируем ошибку, если что-то пошло не так
        logger.error(f"Ошибка при увеличении счетчика подписчиков профиля {profile_id}: {str(e)}")
        raise


# Уменьшить счётчик подписчиков
async def decrement_subscribers_count(profile_id: int):
    """
    Уменьшить количество подписчиков на 1.

    :param profile_id: ID профиля.
    :return: Новое количество подписчиков.
    """
    try:
        # Проверяем, существует ли ключ для счетчика подписчиков
        if not await redis_client.exists(f'subscribers_count:{profile_id}'):
            # Если ключа нет, создаем его с начальным значением 0
            await redis_client.set(f'subscribers_count:{profile_id}', 0)

        # Уменьшаем счетчик подписчиков на 1
        new_count = await redis_client.decr(f'subscribers_count:{profile_id}')

        # Проверяем, не стало ли значение отрицательным
        if new_count < 0:
            # Если стало, сбрасываем счетчик на 0
            await redis_client.set(f'subscribers_count:{profile_id}', 0)
            logger.warning(f"Счетчик подписчиков профиля {profile_id} стал отрицательным. Сброшен на 0.")
            new_count = 0

        # Логируем результат
        logger.info(f"Количество подписчиков профиля {profile_id} уменьшено. Новое значение: {new_count}")

        return new_count
    except Exception as e:
        # Логируем ошибку, если что-то пошло не так
        logger.error(f"Ошибка при уменьшении счетчика подписчиков профиля {profile_id}: {str(e)}")
        raise


# Получение текущего значения счётчика подписчиков
async def get_subscribers_count_from_cache(profile_id: int):
    """
    Получение количества подписчиков из кэша.

    :param profile_id: ID профиля.
    :return: Количество подписчиков или None, если данных нет.
    """
    try:
        # Получаем значение счетчика подписчиков из Redis
        subscribers_count = await redis_client.get(f'subscribers_count:{profile_id}')

        # Если значение не найдено, возвращаем None
        if subscribers_count is None:
            logger.info(f"Счетчик подписчиков для профиля {profile_id} не найден в кэше.")
            return None

        # Логируем результат
        logger.info(f"Получено количество подписчиков для профиля {profile_id}: {int(subscribers_count)}")

        # Возвращаем значение как целое число
        return int(subscribers_count)
    except Exception as e:
        # Логируем ошибку, если что-то пошло не так
        logger.error(f"Ошибка при получении счетчика подписчиков профиля {profile_id}: {str(e)}")
        raise


# Добавить элемент в список избранного
async def add_to_favorites(user_id: int, profile_id: int):
    """
    Добавить профиль в избранное пользователя.

    :param user_id: ID пользователя.
    :param profile_id: ID профиля для добавления.
    :return: Статус операции.
    """
    try:
        # Проверяем, есть ли профиль уже в избранном
        already_in_favorites = await redis_client.sismember(f'favorites:{user_id}', profile_id)

        # Если профиль уже в избранном, возвращаем статус
        if already_in_favorites:
            logger.info(f"Профиль {profile_id} уже в избранном пользователя {user_id}.")
            return {"status": "already_in_favorites"}

        # Добавляем профиль в избранное
        await redis_client.sadd(f'favorites:{user_id}', profile_id)

        # Логируем результат
        logger.info(f"Профиль {profile_id} добавлен в избранное пользователя {user_id}.")

        return {"status": "added"}
    except Exception as e:
        # Логируем ошибку, если что-то пошло не так
        logger.error(f"Ошибка при добавлении профиля {profile_id} в избранное пользователя {user_id}: {str(e)}")
        raise


# Удалить элемент из списка избранного
async def remove_from_favorites(user_id: int, profile_id: int):
    """
    Удалить профиль из избранного пользователя.

    :param user_id: ID пользователя.
    :param profile_id: ID профиля для удаления.
    :return: Статус операции.
    """
    try:
        # Проверяем, есть ли профиль в избранном
        exists_in_favorites = await redis_client.sismember(f'favorites:{user_id}', profile_id)

        # Если профиля нет в избранном, возвращаем статус
        if not exists_in_favorites:
            logger.info(f"Профиль {profile_id} не найден в избранном пользователя {user_id}.")
            return {"status": "not_in_favorites"}

        # Удаляем профиль из избранного
        await redis_client.srem(f'favorites:{user_id}', profile_id)

        # Логируем результат
        logger.info(f"Профиль {profile_id} удален из избранного пользователя {user_id}.")

        return {"status": "removed"}
    except Exception as e:
        # Логируем ошибку, если что-то пошло не так
        logger.error(f"Ошибка при удалении профиля {profile_id} из избранного пользователя {user_id}: {str(e)}")
        raise


# Получение списка избранных из кэша
async def get_favorites_from_cache(user_id: int) -> List[dict]:
    """
    Получение списка избранных из кэша и полной информации по каждому избранному профилю.

    :param user_id: ID пользователя.
    :return: Список профилей с полной информацией (или пустой список, если данных нет).
    """
    try:
        # Получаем список ID избранных профилей из Redis
        favorites = await redis_client.smembers(f'favorites:{user_id}')
        if not favorites:
            logger.info(f"Нет избранных профилей в кэше для пользователя {user_id}.")
            return []

        # Преобразуем ID из строк в числа
        favorite_ids = [int(favorite) for favorite in favorites]
        logger.info(f"Получены избранные профили для пользователя {user_id}: {favorite_ids}")

        # Получаем полную информацию по каждому избранному профилю
        profiles = await get_profiles_by_ids(favorite_ids)
        return profiles

    except Exception as e:
        logger.error(f"Ошибка при получении избранных профилей для пользователя {user_id}: {str(e)}")
        raise


# Функция для слива данных (счетчик подписчиков и избранное) из Redis в БД
async def sync_data_to_db():
    """
    Синхронизирует данные из Redis в базу данных:
    - Обновляет счетчики подписчиков для профилей.
    - Добавляет новые связи "избранное" в базу данных.
    - Удаляет записи из Redis, если юзер или профиль удалены в БД.
    """
    try:
        # Открываем сессию для работы с базой данных
        async with get_db_session_for_worker() as db:

            # Обновляем счетчики подписчиков
            profile_keys = await redis_client.keys('subscribers_count:*')
            for profile_key in profile_keys:
                profile_id = int(profile_key.split(':')[-1])
                subscribers_count = await redis_client.get(profile_key)

                if subscribers_count:
                    # Проверяем, существует ли профиль в БД
                    profile_stmt = await db.execute(select(UserProfiles).filter_by(id=profile_id))
                    profile = profile_stmt.scalar_one_or_none()

                    if profile:
                        # Обновляем счетчик подписчиков
                        profile.followers_count = int(subscribers_count)
                        db.add(profile)
                    else:
                        # Если профиль удален в БД, удаляем запись из Redis
                        await redis_client.delete(profile_key)
                        logger.info(f"Профиль {profile_id} удален в БД. Запись {profile_key} удалена из Redis.")

            # Обновляем избранное
            user_keys = await redis_client.keys('favorites:*')
            for user_key in user_keys:
                user_id = int(user_key.split(':')[-1])
                favorite_profiles = await redis_client.smembers(user_key)

                if favorite_profiles:
                    # Проверяем, существует ли юзер в БД
                    user_stmt = await db.execute(select(User).filter_by(id=user_id))
                    user = user_stmt.scalar_one_or_none()

                    if user:
                        for profile_id in favorite_profiles:
                            profile_id = int(profile_id)

                            # Проверяем, существует ли профиль в БД
                            profile_stmt = await db.execute(select(UserProfiles).filter_by(id=profile_id))
                            profile = profile_stmt.scalar_one_or_none()

                            if profile:
                                # Проверяем, есть ли такая связь в базе данных
                                exists_stmt = await db.execute(
                                    select(Favorite).filter_by(user_id=user_id, profile_id=profile_id)
                                )
                                exists = exists_stmt.scalar_one_or_none()

                                if not exists:
                                    # Добавляем новую связь "избранное"
                                    new_favorite = Favorite(user_id=user_id, profile_id=profile_id)
                                    db.add(new_favorite)
                            else:
                                # Если профиль удален в БД, удаляем его из избранного в Redis
                                await redis_client.srem(user_key, profile_id)
                                logger.info(f"Профиль {profile_id} удален в БД. Удален из избранного пользователя {user_id}.")
                    else:
                        # Если юзер удален в БД, удаляем его избранное из Redis
                        await redis_client.delete(user_key)
                        logger.info(f"Пользователь {user_id} удален в БД. Запись {user_key} удалена из Redis.")

            # Сохраняем все изменения в базе данных
            await db.commit()

        logger.info("Данные о избранном и счетчике подписчиков успешно синхронизированы из кеша в базу данных для всех пользователей.")

    except Exception as e:
        logger.error(f"Ошибка синхронизации данных из кеша в базу данных: {str(e)}")
        raise


# ЛОГИКА РАБОТЫ С ПЕРВОНАЧАЛЬНОЙ ОТДАЧЕЙ 50 ПРОФИЛЕЙ

# Кэширование профилей в Redis
async def cache_profiles_in_redis(profiles):
    """
    Кэширует список профилей в Redis с уникальным ключом и временем жизни 60 секунд.
    :param profiles: Список профилей, которые нужно закэшировать.
    :raises RedisError: Ошибка подключения или работы с Redis.
    :raises Exception: Ошибка в случае некорректных данных.
    """
    try:
        # Преобразуем профили в JSON формат
        cache_data = json.dumps(profiles, default=str)

        # Кэшируем данные в Redis с TTL 62 секунды
        await redis_client.setex("profiles_cache", 62, cache_data)

        # logger.info("Профили успешно закэшированы в Redis с TTL 62 сек.")
    except RedisError as e:
        logger.error(f"Ошибка Redis при кэшировании профилей: {str(e)}")
        raise RedisError("Не удалось закэшировать профили в Redis.") from e
    except Exception as e:
        logger.error(f"Ошибка при обработке и кэшировании профилей: {str(e)}")
        raise Exception("Не удалось закэшировать профили.") from e


# Получение кэшированных профилей из Redis
async def get_cached_profiles(redis_client: redis.Redis):
    """
    Получение кэшированных профилей из Redis.

    :param redis_client: Экземпляр клиента Redis
    :return: Список профилей или None, если кэш пуст.
    """
    try:
        cached_data = await redis_client.get("profiles_cache")
        if cached_data:
            logger.info("Профили получены из кэша.")
            return json.loads(cached_data)  # Десериализуем из JSON
        logger.info("Кэш профилей пуст.")
        return None
    except RedisError as redis_e:
        logger.error(f"Ошибка при работе с Redis: {str(redis_e)}")
        raise HTTPException(status_code=500, detail="Ошибка кэширования данных в Redis")
    except Exception as e:
        logger.error(f"Ошибка при получении кэшированных профилей: {str(e)}")
        raise HTTPException(status_code=500, detail="Ошибка при получении данных из кэша")


# Получение профилей по хэштегам с кэшированием и сортировкой
async def get_profiles_by_hashtag(
    hashtag: str, page: int, per_page: int, sort_by: Optional[str]
):
    """
    Получает профили пользователей по хэштегу с кэшированием, сортировкой и пагинацией.

    Параметры:
        hashtag (str): Хэштег для поиска (с решеткой или без).
        page (int): Номер страницы (начинается с 1).
        per_page (int): Количество профилей на странице.
        sort_by (Optional[str]): Тип сортировки:
            - "newest": По дате создания (новые сначала).
            - "popularity": По количеству подписчиков (популярные сначала).

    Возвращает:
        dict: Данные о профилях и пагинации.

    Исключения:
        HTTPException: При ошибке запроса к базе данных или Redis.
    """
    # Убираем решетку и приводим к нижнему регистру сразу при получении хэштега
    normalized_hashtag = hashtag.lstrip("#").lower()

    # Формируем ключ для кэша
    cache_key = f"profiles_hashtag_{normalized_hashtag}_page_{page}_per_page_{per_page}_sort_{sort_by}"

    # Проверка наличия данных в кэше
    cached_data = await redis_client.get(cache_key)
    if cached_data:
        logger.info(f"Данные найдены в кэше для ключа {cache_key}.")
        return json.loads(cached_data)  # Декодируем данные из JSON

    try:
        async with get_db_session_for_worker() as db:
            # Запрос к БД с поиском по точному совпадению нормализованного хэштега
            query = (
                select(UserProfiles)
                .join(UserProfiles.profile_hashtags)
                .join(ProfileHashtag.hashtag)
                .filter(Hashtag.tag == normalized_hashtag)  # Точное совпадение
                .options(
                    selectinload(UserProfiles.profile_hashtags)
                    .selectinload(ProfileHashtag.hashtag)
                )
            )

            # Применяем сортировку
            if sort_by == "newest":
                query = query.order_by(desc(UserProfiles.created_at))  # По новизне
            elif sort_by == "popularity":
                query = query.order_by(desc(UserProfiles.followers_count))  # По популярности

            # Пагинация
            offset = (page - 1) * per_page
            query = query.offset(offset).limit(per_page)

            # Выполняем запрос
            result = await db.execute(query)
            profiles = result.scalars().all()

            logger.info(f"Найдено профилей: {len(profiles)}")

            # Получаем общее количество профилей
            total_query = (
                select(func.count())
                .select_from(UserProfiles)
                .join(UserProfiles.profile_hashtags)
                .join(ProfileHashtag.hashtag)
                .filter(Hashtag.tag == normalized_hashtag)  # Тот же фильтр
            )
            total = (await db.execute(total_query)).scalar()

            logger.info(f"Общее количество профилей: {total}")

            # Формируем данные профилей для ответа
            profiles_data = [
                {
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
                    "coordinates": await process_coordinates_for_response(profile.coordinates),
                    "followers_count": profile.followers_count,
                    "created_at": await datetime_to_str(profile.created_at),
                    "hashtags": [ph.hashtag.tag for ph in profile.profile_hashtags],
                }
                for profile in profiles
            ]

            # Формируем ответ
            response_data = {
                "page": page,
                "per_page": per_page,
                "total": total,
                "profiles": profiles_data,
            }

            # Сохраняем данные в кэш с TTL в 2 часа
            await redis_client.setex(cache_key, timedelta(hours=2), json.dumps(response_data))
            logger.info(f"Данные сохранены в кэш для ключа {cache_key}.")

            return response_data

    except Exception as e:
        logger.error(f"Ошибка получения профилей по хэштегу {hashtag}: {e}")
        raise HTTPException(status_code=500, detail="Ошибка сервера при получении профилей.")


# Получение профилей по id
async def get_profiles_by_ids(profile_ids: List[int]) -> List[dict]:
    try:
        profiles_from_redis = []
        missing_ids = []

        # Шаг 1: Проверяем Redis
        for profile_id in profile_ids:
            profile_data = await redis_client.get(f"profile:{profile_id}")
            if profile_data:
                profiles_from_redis.append(json.loads(profile_data))  # Данные из Redis
            else:
                missing_ids.append(profile_id)  # Данных нет, добавляем в список для БД

        # Лог: Сколько данных найдено в Redis
        logger.info(f"Найдено {len(profiles_from_redis)} профилей в Redis. Отсутствует {len(missing_ids)} профилей.")

        # Шаг 2: Если в Redis нихуя нет, лезем в БД
        if missing_ids:
            logger.info(f"Загружаем {len(missing_ids)} профилей из базы данных...")
            async with get_db_session_for_worker() as session:
                query = (
                    select(UserProfiles)
                    .where(UserProfiles.id.in_(missing_ids))  # Ищем только недостающие
                )
                result = await session.execute(query)
                profiles_from_db = result.scalars().all()  # Данные из БД

                # Обрабатываем данные из БД
                processed_profiles = []
                for profile in profiles_from_db:
                    profile_data = {
                        "id": profile.id,
                        "name": profile.name,
                        # ... остальные поля ...
                    }
                    processed_profiles.append(profile_data)

                # Лог: Данные загружены из БД
                logger.info(f"Успешно загружено {len(processed_profiles)} профилей из базы данных.")

            # Объединяем данные из Redis и БД
            all_profiles = profiles_from_redis + processed_profiles
            return all_profiles

        # Если всё найдено в Redis, просто возвращаем
        return profiles_from_redis

    except Exception as e:
        logger.error(f"Ошибка при получении профилей: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Ошибка при получении профилей."
        )


# Сохранение профиля без видео
async def save_profile_to_db_without_video(
    form_data: FormData,
    image_data: dict,
    created_dirs: dict,
    new_user_image: bool = True  # Новый параметр
):
    """
    Сохраняет или обновляет профиль пользователя в базе данных без видео.

    :param form_data: Данные формы.
    :param image_data: Данные изображения.
    :param new_user_image: Флаг, указывающий, нужно ли обновлять аватарку.
    """
    try:
        # Преобразование данных формы в словарь
        form_data_dict = form_data.dict()

        # Хэшируем номер кошелька
        wallet_number = form_data_dict.get("wallet_number")
        if not wallet_number:
            raise ValueError("Номер кошелька не указан.")
        hashed_wallet_number = hashlib.sha256(wallet_number.encode()).hexdigest()

        # Сериализация данных формы (HttpUrl в строку)
        form_data_dict = await serialize_form_data(form_data_dict)

        # Лог полученных данных
        logger.info(f"Получены данные профиля: {form_data_dict}")
        logger.info(f"Получены данные о изображении: {image_data}")

        # Получаем координаты из form_data_dict
        coordinates = form_data_dict.get("coordinates")

        # Преобразуем координаты в строку WKT, если они есть
        multi_point_wkt = None
        if coordinates:
            multi_point_wkt = await parse_coordinates(coordinates)

        # Если new_user_image == True, обрабатываем новое изображение
        if new_user_image:
            # Извлечение путей к файлам из JSON
            try:
                image_path = image_data.get("image_path")

                if not image_path:
                    raise ValueError("Путь к файлу не был найден в данных JSON.")

                logger.info(f"Путь к изображению: {image_path}")

                # Преобразование путей в абсолютные, если они относительные
                absolute_image_path = os.path.abspath(image_path)

                logger.info(f"Абсолютный путь к изображению: {absolute_image_path}")

                # Проверка существования изображения
                if not os.path.isfile(absolute_image_path):
                    logger.error(f"Путь к изображению не ведет к файлу: {absolute_image_path}")
                    raise HTTPException(status_code=400, detail="Указанный путь к изображению не ведет к файлу.")

                # Перенос изображения в постоянную папку "user_logo"
                try:
                    # Перенос изображения и получение пути
                    user_logo_path = await move_image_to_user_logo(absolute_image_path, created_dirs)
                    logger.info(f"Изображение успешно перемещено в постоянную папку: {user_logo_path}")
                except Exception as e:
                    logger.error(f"Ошибка при перемещении изображения: {str(e)}")
                    raise HTTPException(status_code=500, detail=f"Ошибка при перемещении изображения: {str(e)}")

                # Преобразование user_logo_path в строку, так как это объект HttpUrl
                if isinstance(user_logo_path, HttpUrl):
                    user_logo_path = str(user_logo_path)
            except Exception as e:
                logger.error(f"Ошибка при извлечении путей из JSON: {str(e)}")
                raise HTTPException(status_code=400, detail="Ошибка при извлечении путей из JSON.")
        else:
            # Если new_user_image == False, оставляем старое значение user_logo_url
            user_logo_path = None  # Это значение не будет использоваться

        # Открываем сессию для работы с базой данных
        async with get_db_session_for_worker() as session:
            try:
                # Ищем пользователя в БД
                stmt = select(User).where(User.wallet_number == hashed_wallet_number)
                result = await session.execute(stmt)
                user = result.scalars().first()

                if not user:
                    logger.error(f"Пользователь с данным кошельком не найден.")
                    raise HTTPException(status_code=400, detail="Пользователь с данным кошельком не найден.")

                # Проверяем, есть ли уже профиль у пользователя
                stmt = select(UserProfiles).where(UserProfiles.user_id == user.id)
                result = await session.execute(stmt)
                profile = result.scalars().first()

                if profile:
                    current_is_admin = profile.is_admin

                    # Обновление данных профиля
                    profile.name = form_data_dict.get("name")
                    profile.website_or_social = form_data_dict.get("website_or_social")
                    profile.activity_and_hobbies = form_data_dict.get("activity_hobbies") if form_data_dict.get("activity_hobbies") is not None else None
                    profile.adress = form_data_dict.get("adress")
                    profile.city = form_data_dict.get("city")
                    profile.coordinates = multi_point_wkt
                    profile.is_in_mlm = form_data_dict.get("is_in_mlm")
                    profile.is_incognito = form_data_dict.get("is_incognito", False)
                    profile.language = form_data_dict.get("language")
                    if new_user_image:  # Обновляем аватарку только если new_user_image == True
                        profile.user_logo_url = user_logo_path
                    profile.video_url = None  # Видео отсутствует
                    profile.preview_url = None  # Превью отсутствует

                    profile.is_admin = current_is_admin

                    session.add(profile)
                    logger.info(f"Обновлен профиль пользователя {user.id}, is_admin сохранен: {current_is_admin}")
                else:
                    # Создание нового профиля
                    new_profile = UserProfiles(
                        name=form_data_dict.get("name"),
                        website_or_social=form_data_dict.get("website_or_social"),
                        activity_and_hobbies=form_data_dict.get("activity_hobbies"),
                        adress=form_data_dict.get("adress"),
                        city=form_data_dict.get("city"),
                        coordinates=multi_point_wkt,
                        is_in_mlm=form_data_dict.get("is_in_mlm"),
                        is_incognito=form_data_dict.get("is_incognito", False),
                        is_moderated=False,
                        is_admin=False,
                        user_id=user.id,
                        language=form_data_dict.get("language"),
                        user_logo_url=user_logo_path if new_user_image else None,  # Добавляем путь к логотипу только если new_user_image == True
                        video_url=None,  # Видео отсутствует
                        preview_url=None  # Превью отсутствует
                    )

                    user.is_profile_created = True

                    session.add(new_profile)
                    await session.flush()
                    profile = new_profile
                    logger.info(f"Создан профиль для пользователя {user.id}, флаг is_profile_created установлен в True")

                # Работа с хэштегами
                if form_data_dict.get("hashtags"):  # Проверяем, что хэштеги переданы
                    hashtags_list = [tag.strip().lower().lstrip("#") for tag in form_data_dict["hashtags"] if tag.strip()]

                    if hashtags_list:
                        # Поиск и проверка существующих хэштегов
                        existing_hashtags_stmt = select(Hashtag).where(Hashtag.tag.in_(hashtags_list))
                        existing_hashtags_result = await session.execute(existing_hashtags_stmt)
                        existing_hashtags = {tag.tag: tag for tag in existing_hashtags_result.scalars().all()}

                        # Список для хранения новых хэштегов
                        new_hashtags = []

                        for hashtag in hashtags_list:
                            if hashtag not in existing_hashtags:
                                # Если хэштег отсутствует - добавляем
                                new_hashtag = Hashtag(tag=hashtag)
                                session.add(new_hashtag)
                                new_hashtags.append(new_hashtag)

                        # Фиксируем новые хэштеги в базе данных
                        await session.flush()

                        # Теперь создаем связи между профилем и хэштегами
                        for hashtag in hashtags_list:
                            if hashtag not in existing_hashtags:
                                # Находим новый хэштег в списке new_hashtags
                                new_hashtag = next((h for h in new_hashtags if h.tag == hashtag), None)
                                if new_hashtag:
                                    # Добавление связи между профилем и новым хэштегом
                                    profile_hashtag = ProfileHashtag(profile_id=profile.id, hashtag_id=new_hashtag.id)
                                    session.add(profile_hashtag)
                            else:
                                # Привязка существующего хэштега к профилю через таблицу ProfileHashtag
                                profile_hashtag = ProfileHashtag(profile_id=profile.id, hashtag_id=existing_hashtags[hashtag].id)
                                session.add(profile_hashtag)

                            logger.info(f"Хэштеги добавлены/обновлены для профиля пользователя {user.id}")

                # Подтверждаем изменения в БД
                await session.commit()
                logger.info(f"Профиль успешно сохранен.")
                return {
                    "message": "Профиль успешно сохранен",
                    "profile_id": profile.id  # Добавляем ID профиля
                }

            except OperationalError as e:
                logger.warning(f"Ошибка базы данных (возможно, конфликт транзакций): {str(e)}")
                await session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Ошибка базы данных. Попробуйте позже."
                )

            except IntegrityError as e:
                logger.error(f"Ошибка целостности данных: {str(e)}")
                await session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Ошибка целостности данных. Проверьте введенные данные."
                )

            except Exception as e:
                logger.error(f"Непредвиденная ошибка: {str(e)}")
                await session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Непредвиденная ошибка. Попробуйте позже."
                )

    except Exception as e:
        logger.error(f"Ошибка в функции save_profile_to_db_without_video: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Непредвиденная ошибка. Попробуйте позже."
        )


# ОТДАЧА ВСЕХ ПРОФИЛЕЙ И ПАГИНАЦИЯ

# Функция для формирования страниц из закешированных профилей по алгоритму
async def create_pages_from_cached_profiles(redis_client: redis.Redis) -> Tuple[int, int]:
    """
    Формирует страницы по 50 профилей из закешированных данных в Redis.
    Учитываются только публичные профили (is_incognito=False).
    Профили распределяются по страницам глобально, без привязки к пользователям.

    :param redis_client: Клиент Redis.
    :return: Кортеж (общее количество профилей, общее количество страниц).
    """
    try:
        # Получаем все закешированные профили из Redis
        cached_profile_keys = await redis_client.keys("profile:*")
        all_profiles = []
        for key in cached_profile_keys:
            profile_data = await redis_client.get(key)
            if profile_data:
                profile = json.loads(profile_data)
                # Фильтруем только публичные профили
                if not profile.get("is_incognito", False):
                    all_profiles.append(profile)

        total_profiles = len(all_profiles)
        logger.info(f"Всего публичных профилей в кеше: {total_profiles}")

        # Разделяем профили на категории
        popular_profiles = sorted(all_profiles, key=lambda x: x.get("followers_count", 0), reverse=True)[:10]
        new_profiles = sorted(all_profiles, key=lambda x: x.get("created_at", ""), reverse=True)[:10]
        mlm_profiles = [p for p in all_profiles if p.get("is_in_mlm", False)][:10]
        random_profiles = random.sample(all_profiles, min(10, len(all_profiles)))
        no_video_profiles = [p for p in all_profiles if not p.get("video_url")][:10]

        # Объединяем профили в единый список без дубликатов
        profiles_to_show = popular_profiles + new_profiles + mlm_profiles + random_profiles + no_video_profiles
        unique_profiles = list({p["id"]: p for p in profiles_to_show}.values())
        logger.info(f"Собрано уникальных профилей по алгоритму: {len(unique_profiles)}")

        # Если уникальных профилей меньше 50, добавляем случайные из оставшихся
        if len(unique_profiles) < 50:
            remaining_profiles = [p for p in all_profiles if p["id"] not in {x["id"] for x in unique_profiles}]
            if remaining_profiles:
                logger.info(f"Добавляем {50 - len(unique_profiles)} случайных профилей из оставшихся.")
                unique_profiles.extend(
                    random.sample(remaining_profiles, min(50 - len(unique_profiles), len(remaining_profiles))))

        # Теперь формируем страницы из всех профилей, а не только из уникальных по алгоритму
        page_size = 50
        pages = [all_profiles[i:i + page_size] for i in range(0, len(all_profiles), page_size)]
        total_pages = ceil(len(all_profiles) / page_size)  # Округляем вверх
        logger.info(f"Сформировано страниц: {total_pages}")

        # Если страниц нет, завершаем выполнение
        if not pages:
            logger.warning("Нет данных для формирования страниц.")
            return total_profiles, 0

        # Кешируем страницы в Redis с TTL 12 часов
        for page_number, page_profiles in enumerate(pages, start=1):
            cache_key = f"page_{page_number}"
            try:
                await redis_client.setex(
                    cache_key,
                    CACHE_PROFILES_TTL_SEK,
                    json.dumps(page_profiles)
                )
            except redis.RedisError as e:
                logger.error(f"Ошибка Redis при кешировании страницы {page_number}: {str(e)}")
                raise

        logger.info(f"В кэше размещено {total_pages} страниц.")
        return total_profiles, total_pages

    except Exception as e:
        logger.error(f"Ошибка при формировании страниц: {e}")
        raise


# Получение данных страницы из Редис
async def get_page_data_from_cache(
    page_number: int,
    redis_client: redis.Redis,
    total_profiles: int,
    total_pages: int
) -> Dict:
    try:
        cache_key = f"page_{page_number}"
        page_profiles = await redis_client.get(cache_key)

        if not page_profiles:
            return {
                "theme": "Макс, это для тебя корешок ^^",
                "page_number": page_number,
                "total_profiles": total_profiles,
                "total_pages": total_pages,
                "message": "Страница не найдена.",
                "profiles": [],
            }

        await redis_client.expire(cache_key, CACHE_PROFILES_TTL_SEK)

        profiles = json.loads(page_profiles)
        is_last_page = page_number == total_pages
        is_incomplete_page = len(profiles) < 50

        start_index = (page_number - 1) * 50 + 1
        end_index = min(start_index + len(profiles) - 1, total_profiles)  # Корректный конечный индекс

        message = f"Показаны профили {start_index}-{end_index} из {total_profiles}."

        if is_last_page and is_incomplete_page:
            message += " Это последняя страница. Начните просмотр профилей со страницы номер 1."

        return {
            "theme": "Макс, это для тебя корешок ^^",
            "page_number": page_number,
            "total_profiles": total_profiles,
            "total_pages": total_pages,
            "message": message,
            "profiles": profiles,
        }

    except Exception as e:
        logger.error(f"Ошибка при получении страницы {page_number} из Redis: {str(e)}")
        raise


# Получение профилей и кэширование их в редис
async def fetch_and_cache_profiles(redis_client: redis.Redis) -> tuple[int, int]:
    """
    Загружает профили из базы данных, обрабатывает их и кэширует в Redis.
    Удаляет из Redis профили, которые были удалены из базы данных.
    Каждый профиль сохраняется в Redis под ключом `profile:{id}`.
    Также создает Sorted Sets для сортировки профилей по новизне и популярности.
    После кеширования профилей формирует страницы.
    Запускается каждые 5 минут в фоновом процессе через планировщик.

    :param redis_client: Клиент Redis.
    :return: Кортеж (общее количество профилей, общее количество страниц).
    """
    try:
        async with get_db_session_for_worker() as session:
            # Формируем запрос с загрузкой хэштегов и данных пользователя
            query = (
                select(UserProfiles)
                .options(
                    selectinload(UserProfiles.profile_hashtags).selectinload(ProfileHashtag.hashtag),
                    selectinload(UserProfiles.user)  # Загружаем данные о пользователе
                )
            )

            # Выполняем запрос
            result = await session.execute(query)
            profiles = result.scalars().all()

            if not profiles:
                logger.info("Нет профилей для кэширования.")
                return 0, 0

            # Получаем список ID профилей из базы данных
            db_profile_ids = {profile.id for profile in profiles}

            # Получаем текущие ключи профилей из Redis
            cached_profile_keys = await redis_client.keys("profile:*")
            cached_profile_ids = {int(key.split(":")[1]) for key in cached_profile_keys}

            # Находим профили, которые нужно удалить из Redis
            profiles_to_delete = cached_profile_ids - db_profile_ids
            if profiles_to_delete:
                logger.info(f"Найдено {len(profiles_to_delete)} профилей для удаления из Redis.")
                for profile_id in profiles_to_delete:
                    await redis_client.delete(f"profile:{profile_id}")
                    await redis_client.zrem("profiles:newest", profile_id)
                    await redis_client.zrem("profiles:popularity", profile_id)
                logger.info(f"Удалено {len(profiles_to_delete)} профилей из Redis.")

            # Обрабатываем профили
            processed_count = 0
            for profile in profiles:
                try:
                    # Обрабатываем координаты
                    coordinates = None
                    if profile.coordinates:
                        geometry = to_shape(profile.coordinates)  # Преобразуем WKB в Shapely
                        if isinstance(geometry, Point):
                            coordinates = {
                                "longitude": float(geometry.x),
                                "latitude": float(geometry.y),
                            }
                        elif isinstance(geometry, MultiPoint):
                            first_point = list(geometry.geoms)[0]
                            coordinates = {
                                "longitude": float(first_point.x),
                                "latitude": float(first_point.y),
                            }

                    # Получаем данные о пользователе
                    user_data = {
                        "id": profile.user.id if profile.user else None,
                        "wallet_number": profile.user.wallet_number if profile.user else None,
                    }

                    # Формируем данные профиля
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
                        "coordinates": coordinates,
                        "followers_count": profile.followers_count,
                        "created_at": profile.created_at.isoformat() if profile.created_at else None,
                        "hashtags": [ph.hashtag.tag for ph in profile.profile_hashtags],
                        "website_or_social": profile.website_or_social,
                        "is_admin": profile.is_admin,
                        "language": profile.language,
                        "user": user_data,  # Добавлен блок данных о пользователе
                    }

                    # Кэшируем профиль в Redis под ключом `profile:{id}`
                    await redis_client.setex(
                        f"profile:{profile.id}",
                        int(timedelta(seconds=CACHE_PROFILES_TTL_SEK).total_seconds()),  # Преобразуем часы в секунды
                        json.dumps(profile_data, default=str)
                    )

                    # Добавляем профиль в Sorted Sets для сортировки
                    if profile.created_at:
                        created_at_timestamp = int(profile.created_at.timestamp())
                        await redis_client.zadd("profiles:newest", {profile.id: created_at_timestamp})
                    if profile.followers_count:
                        await redis_client.zadd("profiles:popularity", {profile.id: profile.followers_count})

                    processed_count += 1
                except Exception as e:
                    logger.error(f"Ошибка при обработке профиля {profile.id}: {str(e)}")

            logger.info(f"Успешно обработано и закэшировано {processed_count} профилей в Redis.")

            # После кеширования профилей формируем страницы
            total_profiles, total_pages = await create_pages_from_cached_profiles(redis_client)
            logger.info(f"Сформировано и закэшировано {total_pages} страниц из {total_profiles} профилей.")

            return total_profiles, total_pages

    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса и кэшировании профилей: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Ошибка при выполнении запроса и кэшировании профилей."
        )


# Функция для получения данных страницы из Redis
async def get_page_data_from_cache(
    page_number: int,
    redis_client: redis.Redis,
    total_profiles: int,
    total_pages: int
) -> Dict:
    """
    Возвращает данные страницы из Redis.
    Если страница не существует, возвращает сообщение о завершении просмотра.

    :param page_number: Номер страницы.
    :param redis_client: Клиент Redis.
    :param total_profiles: Общее количество профилей.
    :param total_pages: Общее количество страниц.
    :return: Данные страницы или сообщение о завершении.
    """
    try:
        cache_key = f"page_{page_number}"
        page_profiles = await redis_client.get(cache_key)

        if not page_profiles:
            logger.warning(f"Страница {page_number} не найдена в Redis.")
            return {
                "theme": "Макс, это для тебя корешок ^^",  # Сообщение на фронт
                "page_number": page_number,
                "total_profiles": total_profiles,
                "total_pages": total_pages,
                "message": "Страница не найдена.",
                "profiles": [],
            }

        # Обновляем TTL ключа на 12 часов (передаём секунды напрямую)
        await redis_client.expire(cache_key, CACHE_PROFILES_TTL_SEK)

        profiles = json.loads(page_profiles)
        logger.info(f"Получено {len(profiles)} профилей для страницы {page_number} из Redis.")

        # Проверяем, является ли текущая страница последней и неполной
        is_last_page = page_number == total_pages
        is_incomplete_page = len(profiles) < 50

        # Формируем сообщение
        offset = (page_number - 1) * 50
        end_index = offset + len(profiles)  # Корректный конечный индекс
        message = f"Показаны профили {offset + 1}-{end_index} из {total_profiles}."

        if is_last_page and is_incomplete_page:
            message += " Это последняя страница. Начните просмотр профилей со страницы номер 1."

        return {
            "theme": "Макс, это для тебя корешок ^^",  # Сообщение на фронт
            "page_number": page_number,
            "total_profiles": total_profiles,
            "total_pages": total_pages,
            "message": message,
            "profiles": profiles,
        }

    except Exception as e:
        logger.error(f"Ошибка при получении страницы {page_number} из Redis: {str(e)}")
        raise


# Функция для получения профилей из Редиски, если пришел запрос на сортировку пагинируем из сортированных списков
async def get_all_profiles_by_page(
    page: int = 1,  # Номер страницы
    sort_by: Optional[str] = None,  # Параметр сортировки (newest или popularity)
    redis_client: redis.Redis = None  # Клиент Redis
) -> Dict:
    """
    Возвращает страницу с профилями, учитывая сортировку и пагинацию.

    :param page: Номер страницы.
    :param sort_by: Параметр сортировки (newest или popularity).
    :param redis_client: Клиент Redis.
    :return: Словарь с данными о профилях, включая пагинацию и общее количество.
    """
    try:
        # Если передана сортировка, используем отсортированные списки
        if sort_by:
            sorted_set_key = f"profiles:{sort_by}"

            # Получаем общее количество профилей в отсортированном списке
            total_profiles = await redis_client.zcard(sorted_set_key)
            if total_profiles == 0:
                logger.warning(f"Нет профилей в отсортированном списке {sorted_set_key}.")
                return {
                    "theme": "Макс, это для тебя корешок ^^",  # Сообщение на фронт
                    "page_number": page,
                    "total_profiles": 0,
                    "total_pages": 0,
                    "message": "Профили просмотрены. Начните с первой страницы.",
                    "profiles": [],
                }

            # Вычисляем смещение и лимит для пагинации
            page_size = 50
            offset = (page - 1) * page_size
            end = offset + page_size - 1

            # Проверяем, чтобы offset и end не выходили за пределы
            if offset < 0 or end < 0 or offset >= total_profiles:
                logger.warning(f"Некорректные значения offset={offset} или end={end} для страницы {page}.")
                return {
                    "theme": "Макс, это для тебя корешок ^^",  # Сообщение на фронт
                    "page_number": page,
                    "total_profiles": total_profiles,
                    "total_pages": (total_profiles + page_size - 1) // page_size,
                    "message": "Некорректная страница.",
                    "profiles": [],
                }

            # Получаем ID профилей из отсортированного списка
            profile_ids = await redis_client.zrange(sorted_set_key, offset, end)

            # Получаем данные профилей по их ID
            profiles = []
            for profile_id in profile_ids:
                profile_data = await redis_client.get(f"profile:{profile_id}")
                if profile_data:
                    try:
                        profiles.append(json.loads(profile_data))
                    except json.JSONDecodeError as e:
                        logger.error(f"Ошибка при декодировании профиля {profile_id}: {str(e)}")
                        continue

            # Вычисляем общее количество страниц
            total_pages = (total_profiles + page_size - 1) // page_size

            # Проверяем, есть ли профили на текущей странице
            if not profiles:
                logger.warning(f"Нет профилей на странице {page} в отсортированном списке {sorted_set_key}.")
                return {
                    "theme": "Макс, это для тебя корешок ^^",  # Сообщение на фронт
                    "page_number": page,
                    "total_profiles": total_profiles,
                    "total_pages": total_pages,
                    "message": "Профили просмотрены. Начните с первой страницы.",
                    "profiles": [],
                }

            logger.info(f"Получено {len(profiles)} профилей для страницы {page} из отсортированного списка {sorted_set_key}.")

            # Возвращаем данные
            return {
                "theme": "Макс, это для тебя корешок ^^",  # Сообщение на фронт
                "page_number": page,
                "total_profiles": total_profiles,
                "total_pages": total_pages,
                "message": f"Показаны профили {offset + 1}-{min(end + 1, total_profiles)} из {total_profiles}.",
                "profiles": profiles,
            }

        # Если сортировка не передана, используем заранее сформированные страницы
        # Получаем общее количество профилей и страниц
        total_profiles, total_pages = await fetch_and_cache_profiles(redis_client)

        # Логируем запрос
        logger.info(f"Запрошена страница {page}. Всего профилей: {total_profiles}, всего страниц: {total_pages}.")

        # Получаем данные страницы
        page_data = await get_page_data_from_cache(
            page_number=page,
            redis_client=redis_client,
            total_profiles=total_profiles,
            total_pages=total_pages
        )

        # Логируем успешное выполнение
        logger.info(f"Показана страница {page} из {total_pages}. Профили: {len(page_data['profiles'])}.")

        return page_data

    except Exception as e:
        logger.error(f"Ошибка при получении страницы {page}: {str(e)}")
        raise HTTPException(status_code=500, detail="Ошибка при получении профилей")