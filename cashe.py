""" Модуль для описания работы с кэшем: кэш счетчика подписчиков и кэш добавленных в избранное,
    получение 50 профилей на первоначальную отдачу клиентам.
    Описание логики актуализации данных в кэше Redis и актуализации данных в БД """

import redis.asyncio as redis
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import Depends

from logging_config import get_logger
from database import get_db_session
from models import UserProfiles, Favorite
import random

logger = get_logger()

# Настраиваем соединение с Redis
redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True) # Надо ли этот тут?


# ЛОГИКА РАБОТЫ С ИЗБРАННЫМ

# Увеличить счётчик подписчиков
async def increment_subscribers_count(profile_id: int):
    """
    Увеличить количество подписчиков на 1.

    :param profile_id: ID профиля.
    """
    try:
        new_count = await redis_client.incr(f'subscribers_count:{profile_id}')
        logger.info(f"Incremented subscribers count for profile {profile_id}. New count: {new_count}")
        return new_count
    except Exception as e:
        logger.error(f"Failed to increment subscribers count for profile {profile_id}: {str(e)}")
        raise


# Уменьшить счётчик подписчиков
async def decrement_subscribers_count(profile_id: int):
    """
    Уменьшить количество подписчиков на 1.

    :param profile_id: ID профиля.
    """
    try:
        new_count = await redis_client.decr(f'subscribers_count:{profile_id}')
        # Защита от отрицательных значений
        if new_count < 0:
            await redis_client.set(f'subscribers_count:{profile_id}', 0)
            logger.warning(f"Subscribers count for profile {profile_id} went negative. Reset to 0.")
            new_count = 0
        logger.info(f"Decremented subscribers count for profile {profile_id}. New count: {new_count}")
        return new_count
    except Exception as e:
        logger.error(f"Failed to decrement subscribers count for profile {profile_id}: {str(e)}")
        raise


# Получение текущего значения счётчика подписчиков
async def get_subscribers_count_from_cache(profile_id: int):
    """
    Получение количества подписчиков из кэша.

    :param profile_id: ID профиля.
    :return: Количество подписчиков или None, если данных нет.
    """
    try:
        subscribers_count = await redis_client.get(f'subscribers_count:{profile_id}')
        if subscribers_count:
            logger.info(f"Retrieved subscribers count for profile {profile_id}: {int(subscribers_count)}")
            return int(subscribers_count)
        logger.info(f"No subscribers count found in cache for profile {profile_id}")
        return 0
    except Exception as e:
        logger.error(f"Failed to retrieve subscribers count for profile {profile_id}: {str(e)}")
        raise


# Добавить элемент в список избранного
async def add_to_favorites(user_id: int, profile_id: int):
    """
    Добавить профиль в избранное пользователя.

    :param user_id: ID пользователя.
    :param profile_id: ID профиля для добавления.
    """
    try:
        # Проверка, существует ли уже профиль в избранном
        already_in_favorites = await redis_client.sismember(f'favorites:{user_id}', profile_id)
        if already_in_favorites:
            logger.info(f"Profile {profile_id} is already in favorites of user {user_id}.")
            return  # Или можешь вернуть какое-то значение, если нужно
        await redis_client.sadd(f'favorites:{user_id}', profile_id)
        logger.info(f"Added profile {profile_id} to favorites of user {user_id}.")
    except Exception as e:
        logger.error(f"Failed to add profile {profile_id} to favorites of user {user_id}: {str(e)}")
        raise


# Удалить элемент из списка избранного
async def remove_from_favorites(user_id: int, profile_id: int):
    """
    Удалить профиль из избранного пользователя.

    :param user_id: ID пользователя.
    :param profile_id: ID профиля для удаления.
    """
    try:
        # Проверка, существует ли профиль в избранном
        exists_in_favorites = await redis_client.sismember(f'favorites:{user_id}', profile_id)
        if not exists_in_favorites:
            logger.info(f"Profile {profile_id} is not in favorites of user {user_id}.")
            return  # Или можешь вернуть какое-то значение, если нужно
        await redis_client.srem(f'favorites:{user_id}', profile_id)
        logger.info(f"Removed profile {profile_id} from favorites of user {user_id}.")
    except Exception as e:
        logger.error(f"Failed to remove profile {profile_id} from favorites of user {user_id}: {str(e)}")
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
    try:
        # Создаём асинхронную сессию для работы с БД
        async with get_db_session() as session:

            # Получаем все ключи профилей из Redis (например: subscribers_count:1, subscribers_count:2, ... )
            profile_keys = await redis_client.keys('subscribers_count:*')

            for profile_key in profile_keys:
                profile_id = int(profile_key.decode().split(':')[-1])
                subscribers_count = await redis_client.get(profile_key)

                if subscribers_count:
                    # Находим профиль по ID и обновляем количество подписчиков
                    profile_stmt = await session.execute(select(UserProfiles).filter_by(id=profile_id))
                    profile = profile_stmt.scalar_one_or_none()
                    if profile:
                        profile.followers_count = int(subscribers_count)
                        session.add(profile)

            # Получаем все ключи избранных профилей из Redis
            user_keys = await redis_client.keys('favorites:*')

            for user_key in user_keys:
                user_id = int(user_key.decode().split(':')[-1])
                favorite_profiles = await redis_client.smembers(user_key)

                if favorite_profiles:
                    for profile_id in favorite_profiles:
                        profile_id = int(profile_id)

                        # Проверяем, есть ли такая связь в базе данных
                        exists_stmt = await session.execute(
                            select(Favorite).filter_by(user_id=user_id, profile_id=profile_id)
                        )
                        exists = exists_stmt.scalar_one_or_none()

                        if not exists:
                            # Добавляем новый избранный профиль
                            new_favorite = Favorite(user_id=user_id, profile_id=profile_id)
                            session.add(new_favorite)

            # Сохраняем все изменения в базе данных
            await session.commit()

        logger.info("Данные успешно синхронизированы из кеша в базу данных.")

    except Exception as e:
        logger.error(f"Ошибка синхронизации данных из кеша в базу данных: {str(e)}")


# ЛОГИКА РАБОТЫ С ПЕРВОНАЧАЛЬНОЙ ОТДАЧЕЙ 50 ПРОФИЛЕЙ

# Кэширование 50 профилей в Redis
async def cache_profiles_in_redis(profiles):
    """
    Кэширует список профилей в Redis с уникальным ключом и временем жизни 60 секунд.
    :param profiles: Список профилей, которые нужно закэшировать.
    :raises RedisError: Ошибка подключения или работы с Redis.
    :raises Exception: Ошибка в случае некорректных данных.
    """
    try:
        # Преобразуем список профилей в JSON
        cache_data = json.dumps(profiles)

        # Используем уникальный ключ для кэширования
        await redis_client.setex("profiles_cache", 63, cache_data)  # TTL = 60 секунд

        logger.info("Профили успешно закэшированы в Redis с TTL 63 сек.")

    except RedisError as redis_e:
        logger.error(f"Ошибка при работе с Redis: {str(redis_e)}")
        raise RedisError("Не удалось выполнить операцию с Redis.") from redis_e

    except Exception as e:
        logger.error(f"Ошибка при кэшировании профилей: {str(e)}")
        raise Exception("Не удалось закэшировать профили.") from e


# Получение 50 профилей на первоначальную отдачу клиенту
async def get_sorted_profiles(session: AsyncSession = Depends(get_db_session)):
    """
    Получает 50 профилей по следующим критериям:
    1. 10 самых популярных профилей по количеству подписчиков.
    2. 10 самых новых профилей по дате создания.
    3. 10 профилей с наличием MLM.
    4. 10 случайных профилей.
    5. 10 профилей без видео.
    :param session: SQLAlchemy сессия для работы с базой данных.
    :return: Список словарей с полными данными профилей.
    """
    try:
        logger.info("Запуск выборки профилей из базы данных.")

        # 10 самых популярных (исключаем инкогнито)
        popular_profiles_stmt = (
            select(UserProfiles)
            .options(
                joinedload(UserProfiles.user),
                joinedload(UserProfiles.hashtags)
            )
            .filter(UserProfiles.is_incognito == False)  # Фильтруем по инкогнито
            .order_by(UserProfiles.followers_count.desc())
            .limit(10)
        )
        popular_profiles = await session.execute(popular_profiles_stmt)
        popular_profiles = popular_profiles.scalars().all()

        logger.info(f"Получено {len(popular_profiles)} популярных профилей.")

        # 10 самых новых (исключаем инкогнито)
        new_profiles_stmt = (
            select(UserProfiles)
            .options(
                joinedload(UserProfiles.user),
                joinedload(UserProfiles.hashtags)
            )
            .filter(UserProfiles.is_incognito == False)  # Фильтруем по инкогнито
            .order_by(UserProfiles.created_at.desc())
            .limit(10)
        )
        new_profiles = await session.execute(new_profiles_stmt)
        new_profiles = new_profiles.scalars().all()

        logger.info(f"Получено {len(new_profiles)} новых профилей.")

        # 10 с наличием MLM (исключаем инкогнито)
        mlm_profiles_stmt = (
            select(UserProfiles)
            .options(
                joinedload(UserProfiles.user),
                joinedload(UserProfiles.hashtags)
            )
            .filter(UserProfiles.is_in_mlm == True)
            .filter(UserProfiles.is_incognito == False)  # Фильтруем по инкогнито
            .limit(10)
        )
        mlm_profiles = await session.execute(mlm_profiles_stmt)
        mlm_profiles = mlm_profiles.scalars().all()

        logger.info(f"Получено {len(mlm_profiles)} профилей с MLM.")

        # 10 случайных (исключаем инкогнито)
        random_profiles_stmt = (
            select(UserProfiles)
            .options(
                joinedload(UserProfiles.user),
                joinedload(UserProfiles.hashtags)
            )
            .filter(UserProfiles.is_incognito == False)  # Фильтруем по инкогнито
            .order_by(func.random())
            .limit(10)
        )
        random_profiles = await session.execute(random_profiles_stmt)
        random_profiles = random_profiles.scalars().all()

        logger.info(f"Получено {len(random_profiles)} случайных профилей.")

        # 10 профилей без видео (где video_url пустое)
        no_video_profiles_stmt = (
            select(UserProfiles)
            .options(
                joinedload(UserProfiles.user),
                joinedload(UserProfiles.hashtags)
            )
            .filter(UserProfiles.video_url == None)  # Профили без видео
            .filter(UserProfiles.is_incognito == False)  # Фильтруем по инкогнито
            .limit(10)
        )
        no_video_profiles = await session.execute(no_video_profiles_stmt)
        no_video_profiles = no_video_profiles.scalars().all()

        logger.info(f"Получено {len(no_video_profiles)} профилей без видео.")

        # Объединяем все профили
        profiles = popular_profiles + new_profiles + mlm_profiles + random_profiles + no_video_profiles
        logger.info(f"Общее количество полученных профилей: {len(profiles)}")

        # Преобразование профилей в словари для отдачи всех данных на фронт
        result = [
            {
                "id": profile.id,
                "created_at": profile.created_at,
                "name": profile.name,
                "user_logo_url": profile.user_logo_url,
                "video_url": profile.video_url,
                "preview_url": profile.preview_url,
                "activity_and_hobbies": profile.activity_and_hobbies,
                "is_moderated": profile.is_moderated,
                "is_incognito": profile.is_incognito,
                "is_in_mlm": profile.is_in_mlm,
                "adress": profile.adress,
                "coordinates": profile.coordinates,
                "followers_count": profile.followers_count,
                "website_or_social": profile.website_or_social,  # Добавляем поле website_or_social
                "user": {
                    "id": profile.user.id,
                    "wallet_number": profile.user.wallet_number,
                },
                "hashtags": [hashtag.tag for hashtag in profile.hashtags],
            }
            for profile in profiles
        ]

        # Кэшируем результат для будущих запросов
        await cache_profiles_in_redis(result)

        return result

    except Exception as e:
        logger.error(f"Ошибка при выборке профилей: {str(e)}")
        raise Exception("Не удалось получить профили из базы данных.") from e



# Получение кэшированных профилей из Redis
async def get_cached_profiles():
    """
    Получение кэшированных профилей из Redis.
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
        raise RedisError("Не удалось выполнить операцию с Redis.") from redis_e

    except Exception as e:
        logger.error(f"Ошибка при получении кэшированных профилей: {str(e)}")
        raise Exception("Не удалось получить данные из кэша.") from e





# Получение профилей по хэштегам с кэшированием и сортировкой
async def get_profiles_by_hashtag(
        hashtag: str, page: int, per_page: int, sort_by: Optional[str], db: AsyncSession
):
    """ Получение профилей по хэштегам с кэшированием и сортировкой """
    cache_key = f"profiles_hashtag_{hashtag}_page_{page}_per_page_{per_page}_sort_{sort_by}"

    # Проверка наличия данных в кэше
    cached_data = await redis_client.get(cache_key)
    if cached_data:
        return json.loads(cached_data)  # Декодируем данные из JSON

    try:
        # Строим запрос для получения профилей по хэштегам
        query = select(UserProfiles).filter(UserProfiles.hashtags.any(Hashtag.name == hashtag))

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

        # Получаем общее количество профилей
        total = await db.execute(
            select([func.count()]).select_from(UserProfiles).filter(UserProfiles.hashtags.any(Hashtag.name == hashtag)))
        total = total.scalar()

        # Формируем ответ
        response_data = {
            "page": page,
            "per_page": per_page,
            "total": total,
            "profiles": profiles,
        }

        # Сохраняем данные в кэш с TTL в 2 часа
        await redis_client.setex(cache_key, timedelta(hours=2), json.dumps(response_data))

        return response_data

    except Exception as e:
        raise Exception(f"Ошибка получения профилей по хэштегу {hashtag}: {e}")
