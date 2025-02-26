""" Модуль для описания работы с кэшем: кэш счетчика подписчиков и кэш добавленных в избранное,
    получение 50 профилей на первоначальную отдачу клиентам.
    Описание логики актуализации данных в кэше Redis и актуализации данных в БД """

import random
from datetime import timedelta
import json
from fastapi import HTTPException
import redis.asyncio as redis
from redis.exceptions import RedisError
from typing import Optional
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, subqueryload, selectinload
from sqlalchemy.future import select
from geoalchemy2.shape import to_shape
from shapely.wkt import loads as wkt_loads
from shapely.geometry import Point, MultiPoint

from logging_config import get_logger
from database import get_db_session, get_db_session_for_worker
from models import UserProfiles, Favorite, Hashtag, ProfileHashtag
from utils import datetime_to_str, process_coordinates_for_response


logger = get_logger()

# Настраиваем соединение с Redis
redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True) # Надо ли этот тут?

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
async def get_favorites_from_cache(user_id: int):
    """
    Получение списка избранных из кэша.

    :param user_id: ID пользователя.
    :return: Список ID избранных профилей (или пустой список, если данных нет).
    """
    try:
        favorites = await redis_client.smembers(f'favorites:{user_id}')
        if favorites:
            result = [int(favorite) for favorite in favorites]
            logger.info(f"Retrieved favorites for user {user_id}: {result}")
            return result
        logger.info(f"No favorites found in cache for user {user_id}.")
        return []
    except Exception as e:
        logger.error(f"Failed to retrieve favorites for user {user_id}: {str(e)}")
        raise


# Функция для слива данных из Redis в БД
async def sync_data_to_db():
    """
    Синхронизирует данные из Redis в базу данных:
    - Обновляет счетчики подписчиков для профилей.
    - Добавляет новые связи "избранное" в базу данных.
    """
    try:
        # Открываем сессию для работы с базой данных
        async with get_db_session_for_worker() as db:

            # Получаем все ключи профилей из Redis (например: subscribers_count:1, subscribers_count:2, ... )
            profile_keys = await redis_client.keys('subscribers_count:*')

            # Обновляем счетчики подписчиков для каждого профиля
            for profile_key in profile_keys:
                # Убираем decode(), так как ключи уже являются строками
                profile_id = int(profile_key.split(':')[-1])
                subscribers_count = await redis_client.get(profile_key)

                if subscribers_count:
                    # Находим профиль по ID и обновляем количество подписчиков
                    profile_stmt = await db.execute(select(UserProfiles).filter_by(id=profile_id))
                    profile = profile_stmt.scalar_one_or_none()
                    if profile:
                        profile.followers_count = int(subscribers_count)
                        db.add(profile)

            # Получаем все ключи избранных профилей из Redis
            user_keys = await redis_client.keys('favorites:*')

            # Обновляем избранное для каждого пользователя
            for user_key in user_keys:
                # Убираем decode(), так как ключи уже являются строками
                user_id = int(user_key.split(':')[-1])
                favorite_profiles = await redis_client.smembers(user_key)

                if favorite_profiles:
                    for profile_id in favorite_profiles:
                        profile_id = int(profile_id)

                        # Проверяем, есть ли такая связь в базе данных
                        exists_stmt = await db.execute(
                            select(Favorite).filter_by(user_id=user_id, profile_id=profile_id)
                        )
                        exists = exists_stmt.scalar_one_or_none()

                        if not exists:
                            # Добавляем новую связь "избранное"
                            new_favorite = Favorite(user_id=user_id, profile_id=profile_id)
                            db.add(new_favorite)

            # Сохраняем все изменения в базе данных
            await db.commit()

        logger.info("Данные успешно синхронизированы из кеша в базу данных.")

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


# Получение 50 профилей на первоначальную отдачу клиенту (ЛОГИ ЗАКОМЕНТИЛ БЕСЯТ)
async def get_sorted_profiles():
    """
    Получение 50 профилей на первоначальную отдачу клиенту.
    Профили выбираются по следующим критериям:
    1. **Популярные профили**: 10 профилей с наибольшим количеством подписчиков (followers_count).
    2. **Новые профили**: 10 последних созданных профилей (created_at).
    3. **MLM-профили**: 10 профилей, у которых указано участие в MLM (is_in_mlm не равно None или 0).
    4. **Случайные профили**: 10 случайных профилей (используется функция random()).
    5. **Профили без видео**: 10 профилей, у которых отсутствует видео (video_url равно None).
    Каждый профиль загружается с данными пользователя, хэштегами и координатами.
    После выборки профили кешируются в Redis для быстрого доступа.
    """
    # logger.info("Запуск выборки профилей из базы данных.")

    try:
        async with get_db_session_for_worker() as session:
            # Базовый запрос с загрузкой связанных данных
            base_query = (
                select(UserProfiles)
                .options(
                    joinedload(UserProfiles.user),
                    subqueryload(UserProfiles.profile_hashtags).subqueryload(ProfileHashtag.hashtag)
                )
                .filter(UserProfiles.is_incognito == False)
            )

            # Выборки профилей по критериям
            popular_profiles = (await session.execute(
                base_query.order_by(UserProfiles.followers_count.desc()).limit(10)
            )).scalars().unique().all()

            new_profiles = (await session.execute(
                base_query.order_by(UserProfiles.created_at.desc()).limit(10)
            )).scalars().unique().all()

            mlm_profiles = (await session.execute(
                base_query.filter(UserProfiles.is_in_mlm.isnot(None), UserProfiles.is_in_mlm != 0).limit(10)
            )).scalars().unique().all()

            random_profiles = (await session.execute(
                base_query.order_by(func.random()).limit(10)
            )).scalars().unique().all()

            no_video_profiles = (await session.execute(
                base_query.filter(UserProfiles.video_url == None).limit(10)
            )).scalars().unique().all()

            # Объединяем профили в единый список
            profiles = popular_profiles + new_profiles + mlm_profiles + random_profiles + no_video_profiles
            logger.info(f"Выбрано и получено {len(profiles)} профилей.")

            result = []

            for profile in profiles:
                # Собираем хэштеги только для текущего профиля
                profile_hashtags = [
                    ph.hashtag.tag for ph in profile.profile_hashtags if ph.hashtag
                ]

                # Обработка координат профиля
                coordinates = None
                if profile.coordinates:
                    try:
                        geometry = to_shape(profile.coordinates)
                        if isinstance(geometry, Point):
                            coordinates = [geometry.x, geometry.y]
                        elif isinstance(geometry, MultiPoint):
                            coordinates = [[point.x, point.y] for point in geometry.geoms]
                        # logger.info(f"Координаты профиля {profile.id}: {coordinates}")
                    except Exception as e:
                        logger.error(f"Ошибка при обработке координат профиля {profile.id}: {str(e)}")

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
                    "hashtags": profile_hashtags,  # Только хэштеги текущего профиля
                }

                # logger.info(f"Профиль {profile.id}: {profile_data}")
                result.append(profile_data)

            await cache_profiles_in_redis(result)
            logger.info("Профили успешно закешированы в Redis.")
            return result

    except Exception as e:
        logger.error(f"Ошибка при выборке профилей: {str(e)}")
        raise


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
    # Добавляем решетку к хэштегу, если её нет
    if not hashtag.startswith("#"):
        hashtag = f"#{hashtag}"

    cache_key = f"profiles_hashtag_{hashtag}_page_{page}_per_page_{per_page}_sort_{sort_by}"

    # Проверка наличия данных в кэше
    cached_data = await redis_client.get(cache_key)
    if cached_data:
        logger.info(f"Данные найдены в кэше для ключа {cache_key}.")
        return json.loads(cached_data)  # Декодируем данные из JSON

    try:
        async with get_db_session_for_worker() as db:  # Открываем сессию внутри функции
            # Строим запрос для получения профилей по хэштегам
            query = (
                select(UserProfiles)
                .join(UserProfiles.profile_hashtags)  # Связь с profile_hashtags
                .join(ProfileHashtag.hashtag)  # Связь с hashtags
                .filter(Hashtag.tag.ilike(hashtag))  # Игнорируем регистр хэштега
                .options(
                    selectinload(UserProfiles.profile_hashtags)
                    .selectinload(ProfileHashtag.hashtag)
                )
            )

            # Применяем сортировку
            if sort_by == "newest":
                query = query.order_by(desc(UserProfiles.created_at))  # Сортировка по новизне (дате создания)
            elif sort_by == "popularity":
                query = query.order_by(desc(UserProfiles.followers_count))  # Сортировка по популярности (количеству подписчиков)

            # Пагинация
            offset = (page - 1) * per_page
            query = query.offset(offset).limit(per_page)

            # Выполняем запрос
            result = await db.execute(query)
            profiles = result.scalars().all()

            # Логируем количество найденных профилей
            logger.info(f"Найдено профилей: {len(profiles)}")

            # Получаем общее количество профилей
            total_query = (
                select(func.count())
                .select_from(UserProfiles)
                .join(UserProfiles.profile_hashtags)
                .join(ProfileHashtag.hashtag)
                .filter(Hashtag.tag.ilike(hashtag))
            )
            total = (await db.execute(total_query)).scalar()

            # Логируем общее количество профилей
            logger.info(f"Общее количество профилей: {total}")

            # Формируем данные профилей для ответа
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