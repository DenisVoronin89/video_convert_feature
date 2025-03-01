import os
import asyncio
import json
from pydantic import HttpUrl
from datetime import timedelta
from fastapi import FastAPI, UploadFile, HTTPException, File, Depends, Query, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as redis
from redis.exceptions import RedisError
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload, subqueryload, selectinload
from geoalchemy2.functions import ST_DWithin, ST_MakePoint
from geoalchemy2.shape import from_shape
from shapely import wkt
from shapely.geometry import Point, MultiPoint
import hashlib
from typing import Optional, List
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from fake_profiles import generate_profiles

from database import init_db, engine, get_db_session
from logging_config import get_logger
from video_handle.video_handler_publisher import publish_task
from views import (
    save_video_to_temp,
    save_image_to_temp,
    create_directories,
    move_image_to_user_logo,
    get_profiles_by_city,
    get_all_profiles,
    get_profile_by_wallet_number,
    get_profile_by_username,
    fetch_nearby_profiles,
    grant_admin_rights,
    get_profiles_for_moderation,
    moderate_profile
)
from schemas import FormData, TokenResponse, UserProfileResponse, UserResponse, is_valid_image, is_valid_video, serialize_form_data, validate_and_process_form
from models import User, UserProfiles, Favorite, Hashtag, ProfileHashtag
from cashe import (
    increment_subscribers_count,
    decrement_subscribers_count,
    get_subscribers_count_from_cache,
    add_to_favorites,
    remove_from_favorites,
    get_favorites_from_cache,
    sync_data_to_db,
    cache_profiles_in_redis,
    get_profiles_by_hashtag,
    get_sorted_profiles,
    get_cached_profiles,
    fetch_and_cache_profiles,
    get_all_profiles_from_cache,
    get_profiles_by_ids
)
from tokens import TokenData, create_tokens, verify_access_token, verify_refresh_token
from utils import delete_old_files_task, parse_coordinates, process_coordinates_for_response, datetime_to_str


logger = get_logger()



# Словарь необходимых директорий для работы (прилетает в функцию create_directories)
directories_to_create = {
    "video_temp": "./video_temp",
    "image_temp": "./image_temp",
    "output_video": "./output_video",
    "output_preview": "./output_preview",
    "user_logo": "./user_logo"
}

app = FastAPI()

# Используем HTTPBearer, так как нам нужен только токен, а не полноценный OAuth2
oauth2_scheme = HTTPBearer()

# Настройка CORS (Это Максу - разрешить доступ фронту, разрешить отправлять мне запросы)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Разрешенные источники
    allow_credentials=True,
    allow_methods=["*"],  # Разрешенные методы
    allow_headers=["*"],  # Разрешенные заголовки
)

# Раздача файлов из папки user_logo (ПОТОМ С СЕРВАКА КОГДА ОТДАВАТЬ БУДЕМ СДЕЛАТЬ ПРАВИЛЬНЫЙ КОНФИГ!!!!!!)
# TODO в функции def move_image_to_user_logo (вьюхи) тоже поставить правильный конфиг в переменной!!!!!!
app.mount("/user_logo", StaticFiles(directory="user_logo"), name="user_logo")


# Установка зависимости для подключения Редиса, чтобы прокидывать потом в нужные эндпоинты
async def get_redis_client(redis_client: redis.Redis = Depends(lambda: app.state.redis_client)) -> redis.Redis:
    return redis_client


# Настройка APScheduler для выполнения задач по расписанию
async def start_scheduler():
    """Запуск планировщика задач."""
    scheduler = AsyncIOScheduler()

    # Получаем redis_client из состояния приложения
    redis_client = app.state.redis_client

    # Задача, которая выполняется каждые 30 секунд (обновление профилей в Redis)
    scheduler.add_job(
        fetch_and_cache_profiles,  # Функция
        IntervalTrigger(seconds=30),  # Триггер (интервал 30 секунд)
        args=[redis_client]  # Аргументы для функции
    )
    logger.info("Задача fetch_and_cache_profiles добавлена в расписание (каждые 30 секунд).")

    # Старт планировщика
    scheduler.start()
    logger.info("Планировщик задач успешно запущен.")


@app.on_event("startup")
async def startup():
    """Функция запуска приложения"""
    try:
        # Логирование начала процесса
        logger.info("Запуск приложения и инициализация базы данных...")

        # Инициализация БД
        await init_db()
        logger.info("Приложение успешно запущено. Соединение с базой данных установлено.")

        # Создание директорий
        created_dirs = await create_directories(directories_to_create)

        # Сохранение директорий в состояние приложения
        app.state.created_dirs = created_dirs

        # Подключение к Redis
        redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
        app.state.redis_client = redis_client  # Сохранить Redis в состояние приложения для использования везде

        # Логирование итогов
        dirs_created = {dir: path for dir, path in created_dirs.items() if path == "успешно создана"}
        if dirs_created:
            logger.info(f"Директории для использования успешно созданы: {dirs_created}")

        # Запуск планировщика задач
        await start_scheduler()

    except RuntimeError as e:
        logger.error(f"Ошибка при старте приложения: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка при старте приложения: {str(e)}")

    except SQLAlchemyError as e:
        logger.error(f"Ошибка при инициализации базы данных: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка при инициализации базы данных: {str(e)}")

    except Exception as e:
        logger.error(f"Неизвестная ошибка: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Неизвестная ошибка: {str(e)}")



@app.on_event("shutdown")
async def shutdown():
    """Функция завершения работы приложения"""
    await engine.dispose()
    redis_client = app.state.get("redis_client")
    if redis_client:
        await redis_client.close()  # Закрыть соединение с Redis
    logger.info("Приложение завершило работу. Соединение с базой данных и Redis закрыты.")





# Зависимость для проверки токена в заголовке
async def check_user_token(credentials: HTTPAuthorizationCredentials = Depends(oauth2_scheme)) -> TokenData:
    try:
        # Извлечение токена из заголовка
        token = credentials.credentials
        if not token:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token is missing")

        # Валидация access токена
        return await verify_access_token(token)
    except IndexError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token format")
    except Exception as e:
        logger.error(f"Ошибка при валидации access токена: {str(e)}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired access token")


# Эндпоинт для регистрации/авторизации пользователя, отдача избранного и информации о профиле на фронт, генерация токенов
@app.post("/user/login")
async def login(
    wallet_number: str,
    session: AsyncSession = Depends(get_db_session),
    redis_client: redis.Redis = Depends(get_redis_client)
):
    try:
        # Хешируем номер кошелька
        logger.info("Начало хеширования номера кошелька.")
        hashed_wallet_number = hashlib.sha256(wallet_number.encode()).hexdigest()
        logger.info(f"Хеш номера кошелька: {hashed_wallet_number}")

        # Ищем пользователя в БД
        logger.info("Поиск пользователя в базе данных.")
        stmt = select(User).filter(User.wallet_number == hashed_wallet_number)
        result = await session.execute(stmt)
        user = result.scalar_one_or_none()

        if user:
            # Если пользователь найден, получаем профиль и избранное
            logger.info(f"Пользователь найден с ID {user.id}")
            profile_stmt = select(UserProfiles).filter(UserProfiles.user_id == user.id).options(
                selectinload(UserProfiles.profile_hashtags).selectinload(ProfileHashtag.hashtag)
            )
            profile_result = await session.execute(profile_stmt)
            profile = profile_result.scalar_one_or_none()

            # Формирование данных профиля для ответа
            profile_data = None
            if profile:
                # Обработка координат
                coordinates = await process_coordinates_for_response(profile.coordinates) if profile.coordinates else None

                # Преобразование даты в строку
                created_at_str = await datetime_to_str(profile.created_at) if profile.created_at else None

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
                    "coordinates": coordinates,  # Обработанные координаты
                    "followers_count": profile.followers_count,
                    "created_at": created_at_str,  # Преобразованная дата
                    "hashtags": [ph.hashtag.tag for ph in profile.profile_hashtags],  # Хэштеги текущего профиля
                    "website_or_social": profile.website_or_social,  # Добавляем недостающие поля
                    "is_admin": profile.is_admin,
                    "language": profile.language,
                }

            # Попытка получить избранное из Redis
            logger.info("Попытка получить избранное из кэша Redis.")
            favorite_ids = await get_favorites_from_cache(user.id)

            if not favorite_ids:
                logger.info("Избранное не найдено в кэше. Загружаем из базы данных.")
                favorite_stmt = select(Favorite).filter(Favorite.user_id == user.id)
                favorite_result = await session.execute(favorite_stmt)
                favorites = favorite_result.scalars().all()
                favorite_ids = [favorite.profile_id for favorite in favorites]
                logger.info(f"Избранное загружено из БД: {favorite_ids}")
            else:
                logger.info(f"Избранное получено из кэша: {favorite_ids}")

            # Генерация токенов
            logger.info("Генерация токенов для пользователя.")
            tokens = await create_tokens(user.id)
            logger.info("Токены успешно сгенерированы.")

            # Формируем ответ вручную
            response_data = {
                "id": user.id,
                "profile": profile_data,
                "favorites": favorite_ids,
                "tokens": tokens,
            }

            return response_data

        else:
            # Если пользователя нет, создаем нового
            logger.info("Пользователь не найден, создаем нового.")
            user = User(wallet_number=hashed_wallet_number)
            session.add(user)
            await session.commit()  # Сохраняем пользователя в БД
            await session.refresh(user)  # Обновляем данные пользователя
            logger.info(f"Создан новый пользователь с ID {user.id}")

            # Профиль и избранное не запрашиваются, так как пользователь только что создан.
            profile_data = None
            favorite_ids = []

            # Генерация токенов
            logger.info("Генерация токенов для пользователя.")
            tokens = await create_tokens(user.id)
            logger.info("Токены успешно сгенерированы.")

            # Формируем ответ вручную
            response_data = {
                "id": user.id,
                "profile": profile_data,
                "favorites": favorite_ids,
                "tokens": tokens,
            }

            return response_data

    except SQLAlchemyError as e:
        logger.error(f"Ошибка работы с БД: {str(e)}")
        raise HTTPException(status_code=500, detail="Ошибка работы с базой данных.")

    except Exception as e:
        logger.error(f"Ошибка при обработке запроса: {str(e)}")
        raise HTTPException(status_code=400, detail="Ошибка при обработке запроса.")


# Эндпоинт для загрузки изображения
@app.post("/upload_image/")
async def upload_image(file: UploadFile = File(...), current_user: TokenData = Depends(check_user_token)):
    try:
        logger.info(f"Получен запрос на загрузку изображения от пользователя с ID: {current_user.user_id}")

        # Получение директорий из состояния приложения
        created_dirs = app.state.created_dirs

        # Проверка валидности изображения
        if not is_valid_image(file):
            logger.warning(f"Неверный формат изображения: {file.filename}")
            raise HTTPException(status_code=400, detail="Неверный формат изображения")

        # Сохранение изображения
        image_path = await save_image_to_temp(file, created_dirs)

        logger.info(f"Изображение успешно загружено: {image_path}")
        return {"message": "Изображение успешно загружено", "image_path": image_path}

    except HTTPException as e:
        logger.error(f"Ошибка валидации изображения: {str(e)}")
        raise e
    except Exception as e:
        logger.error(f"Произошла ошибка при загрузке изображения: {str(e)}")
        raise HTTPException(status_code=500, detail="Ошибка на сервере")


# Эндпоинт для загрузки видео
@app.post("/upload_video/")
async def upload_video(file: UploadFile = File(...), current_user: TokenData = Depends(check_user_token)):
    try:
        logger.info("Получен запрос на загрузку видео от пользователя с ID: {current_user.user_id}")

        # Получение директории из состояния приложения
        created_dirs = app.state.created_dirs

        # Проверка валидности видео
        if not is_valid_video(file):
            logger.warning(f"Неверный формат видео: {file.filename}")
            raise HTTPException(status_code=400, detail="Неверный формат видео")

        # Сохранение видео
        video_path = await save_video_to_temp(file, created_dirs)

        logger.info(f"Видео успешно загружено: {video_path}")
        return {"message": "Видео успешно загружено", "video_path": video_path}

    except HTTPException as e:
        logger.error(f"Ошибка валидации видео: {str(e)}")
        raise e
    except Exception as e:
        logger.error(f"Произошла ошибка при загрузке видео: {str(e)}")
        raise HTTPException(status_code=500, detail="Ошибка при загрузке видео")


# Эндпоинт валидации формы
@app.post("/check_form/")
async def check_form(data: FormData, current_user: TokenData = Depends(check_user_token)):
    logger.debug(f"Получены данные: {data}")
    try:
        logger.info("Получены данные формы  от пользователя с ID {current_user.user_id}: %s", data.dict())

        # Валидация и обработка данных формы
        form_result = await validate_and_process_form(data)

        logger.info("Данные формы успешно обработаны: %s", form_result)

        # Результат успешной обработки (Максу на фронт)
        return JSONResponse(
            status_code=200,
            content={
                "status": "form_validated",
                "form_validation": form_result
            }
        )

    except HTTPException as e:
        # Лог ошибки уровня HTTP
        logger.error(f"Ошибка HTTP при обработке формы: {e.detail}")
        raise e

    except Exception as e:
        logger.error(f"Неожиданная ошибка при обработке данных формы: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка при обработке данных формы: {str(e)}"
        )


# Эндпоинт сохранения профиля
@app.post("/save_profile/")
async def save_profile(profile_data: FormData, image_data: dict, video_data: dict, _: TokenData = Depends(check_user_token)):
    """
    Получение данных профиля из формы, пути к изображению и видео (в виде JSON),
    проверка путей и отправка задачи на обработку в Redis.
    """
    # Преобразование данных формы в словарь
    form_data_dict = profile_data.dict()

    # Сериализация данных формы (HttpUrl в строку)
    form_data_dict = await serialize_form_data(form_data_dict)

    # Лог полученных данных(смотреть что полетит в канал)
    logger.info(f"Получены данные профиля: {form_data_dict}")
    logger.info(f"Получены данные о изображении: {image_data}")
    logger.info(f"Получены данные о видео: {video_data}")

    # Извлечение номера кошелька и хэширование (кошелек с фронта летит)
    try:
        wallet_number = form_data_dict.get("wallet_number")
        if not wallet_number:
            raise ValueError("Номер кошелька не указан.")

        hashed_wallet_number = hashlib.sha256(wallet_number.encode()).hexdigest()
        logger.info(f"Номер кошелька захэширован: {hashed_wallet_number}")
    except Exception as e:
        logger.error(f"Ошибка при хэшировании номера кошелька: {str(e)}")
        raise HTTPException(status_code=400, detail="Ошибка при обработке номера кошелька.")

    # Извлечение путей к файлам из JSON
    try:
        image_path = image_data.get("image_path")
        video_path = video_data.get("video_path")

        if not image_path or not video_path:
            raise ValueError("Пути к файлам не были найдены в данных JSON.")

        logger.info(f"Путь к изображению: {image_path}")
        logger.info(f"Путь к видео: {video_path}")

    except Exception as e:
        logger.error(f"Ошибка при извлечении путей из JSON: {str(e)}")
        raise HTTPException(status_code=400, detail="Ошибка при извлечении путей из JSON.")

    # Получение директорий из состояния приложения
    created_dirs = app.state.created_dirs
    if not created_dirs:
        logger.error("Каталоги для сохранения файлов не были инициализированы.")
        raise HTTPException(status_code=500, detail="Ошибка при инициализации каталогов.")

    # Преобразование путей в абсолютные, если они относительные
    absolute_image_path = os.path.abspath(image_path)
    absolute_video_path = os.path.abspath(video_path)

    logger.info(f"Абсолютный путь к изображению: {absolute_image_path}")
    logger.info(f"Абсолютный путь к видео: {absolute_video_path}")

    # Проверка существования изображения
    if not os.path.isfile(absolute_image_path):
        logger.error(f"Путь к изображению не ведет к файлу: {absolute_image_path}")
        raise HTTPException(status_code=400, detail="Указанный путь к изображению не ведет к файлу.")

    # Проверка существования видео
    if not os.path.isfile(absolute_video_path):
        logger.error(f"Путь к видео не ведет к файлу: {absolute_video_path}")
        raise HTTPException(status_code=400, detail="Указанный путь к видео не ведет к файлу.")

    # Перенос изображения в постоянную папку "user_logo"
    try:
        # Перенос изображения и получение пути
        user_logo_path = await move_image_to_user_logo(absolute_image_path, created_dirs)
        logger.info(f"Изображение успешно перемещено в постоянную папку: {user_logo_path}")
    except Exception as e:
        logger.error(f"Ошибка при перемещении изображения: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка при перемещении изображения: {str(e)}")

    # Преобразование user_logo_path в строку, так как это объект HttpUrl (TODO объединить бы с сериализацией формы)
    if isinstance(user_logo_path, HttpUrl):
        user_logo_path = str(user_logo_path)

    try:
        # Лог начала обработки запроса
        logger.info("Обработка данных профиля...")

        # Подключение к Redis из состояния приложения
        redis_client = app.state.redis_client
        logger.info("Соединение с Redis  для публикации задачи в канал установлено.")

        # Лог задачи перед отправкой в Redis
        logger.info(f"Публикуемые данные в Redis: {{"
                     f"input_path: {absolute_video_path}, "
                     f"output_path: {created_dirs['output_video']}, "
                     f"preview_path: {created_dirs['output_preview']}, "
                     f"user_logo_url: {user_logo_path}, "
                     f"wallet_number: {hashed_wallet_number}, "
                     f"form_data: {form_data_dict}}}")

        # Публикация задачи в Redis
        await publish_task(
            redis_client,
            input_path=absolute_video_path,  # Путь к видео
            output_path=created_dirs["output_video"],  # Путь для итогового видео
            preview_path=created_dirs["output_preview"],  # Путь для превью
            user_logo_url=user_logo_path,  # Путь к изображению, которое переехало в постоянную папку на сервере
            wallet_number=hashed_wallet_number,  # Кошелек
            form_data=form_data_dict  # Данные формы для сохранения в БД
        )
        logger.info("Задача успешно отправлена в Redis.")

        # Ответ клиенту
        return {"message": "Ваш профиль успешно сохранен и отправлен на модерацию."}

    except redis.RedisError as e:
        # Лог ошибок при работе с Redis
        logger.error(f"Ошибка при подключении или публикации в Redis: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка при сохранении профиля в Redis: {str(e)}")

    except Exception as e:
        logger.error(f"Ошибка при сохранении профиля: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка при сохранении профиля: {str(e)}")


# Эндпоинт для сохранения юзера в БД без видео (нет смысла запускать фоновую задачу)
@app.post("/save_profile_without_video/")
async def create_or_update_user_profile(
    form_data: FormData,
    image_data: dict,
    session: AsyncSession = Depends(get_db_session),
    _: TokenData = Depends(check_user_token)
):
    """
    Эндпоинт для создания или обновления профиля пользователя без видео.
    Если какое-то поле не передано, оно перезаписывается в NULL в БД.
    """

    # Преобразование данных формы в словарь
    form_data_dict = form_data.dict()

    # Сериализация данных формы (HttpUrl в строку)
    form_data_dict = await serialize_form_data(form_data_dict)

    # Лог полученных данных
    logger.info(f"Получены данные профиля: {form_data_dict}")
    logger.info(f"Получены данные о изображении: {image_data}")

    # Извлечение номера кошелька и хэширование
    try:
        wallet_number = form_data_dict.get("wallet_number")
        if not wallet_number:
            raise ValueError("Номер кошелька не указан.")

        hashed_wallet_number = hashlib.sha256(wallet_number.encode()).hexdigest()
        logger.info(f"Номер кошелька захэширован: {hashed_wallet_number}")
    except Exception as e:
        logger.error(f"Ошибка при хэшировании номера кошелька: {str(e)}")
        raise HTTPException(status_code=400, detail="Ошибка при обработке номера кошелька.")

    # Извлечение пути к изображению из JSON
    try:
        image_path = image_data.get("image_path")
        if not image_path:
            raise ValueError("Путь к изображению не найден в данных JSON.")

        logger.info(f"Путь к изображению: {image_path}")
    except Exception as e:
        logger.error(f"Ошибка при извлечении путей из JSON: {str(e)}")
        raise HTTPException(status_code=400, detail="Ошибка при извлечении путей из JSON.")

    # Получение директорий из состояния приложения
    created_dirs = app.state.created_dirs
    if not created_dirs:
        logger.error("Каталоги для сохранения файлов не были инициализированы.")
        raise HTTPException(status_code=500, detail="Ошибка при инициализации каталогов.")

    # Преобразование пути к изображению в абсолютный
    absolute_image_path = os.path.abspath(image_path)
    logger.info(f"Абсолютный путь к изображению: {absolute_image_path}")

    # Проверка существования изображения
    if not os.path.isfile(absolute_image_path):
        logger.error(f"Путь к изображению не ведет к файлу: {absolute_image_path}")
        raise HTTPException(status_code=400, detail="Указанный путь к изображению не ведет к файлу.")

    # Перенос изображения в постоянную папку "user_logo"
    try:
        user_logo_path = await move_image_to_user_logo(absolute_image_path, created_dirs)
        logger.info(f"Изображение успешно перемещено в постоянную папку: {user_logo_path}")
    except Exception as e:
        logger.error(f"Ошибка при перемещении изображения: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка при перемещении изображения: {str(e)}")

    # Преобразование user_logo_path в строку, если это HttpUrl
    if isinstance(user_logo_path, HttpUrl):
        user_logo_path = str(user_logo_path)

    # Ищем пользователя в БД
    stmt = select(User).where(User.wallet_number == hashed_wallet_number)
    result = await session.execute(stmt)
    user = result.scalars().first()

    if not user:
        raise HTTPException(status_code=400, detail="Пользователь с данным кошельком не найден.")

    # Обрабатываем координаты через parse_coordinates
    multi_point_wkt = await parse_coordinates(form_data.coordinates)

    # Проверяем, есть ли уже профиль у пользователя
    stmt = select(UserProfiles).where(UserProfiles.user_id == user.id)
    result = await session.execute(stmt)
    profile = result.scalars().first()

    # Если профиль найден, обновляем его, если нет — создаем новый
    if profile:
        profile.name = form_data.name
        profile.website_or_social = form_data.website_or_social if form_data.website_or_social is not None else None
        profile.activity_and_hobbies = form_data.activity_hobbies if form_data.activity_hobbies is not None else None
        profile.user_logo_url = user_logo_path
        profile.adress = form_data.adress if form_data.adress is not None else None
        profile.city = form_data.city if form_data.city is not None else None
        profile.coordinates = multi_point_wkt if form_data.coordinates is not None else None
        profile.is_in_mlm = form_data.is_in_mlm if form_data.is_in_mlm is not None else None
        profile.is_incognito = form_data.is_incognito
        profile.language = form_data.language if form_data.language is not None else None

        session.add(profile)
        logger.info(f"Обновлен профиль пользователя {user.id}")
    else:
        # Создаем новый профиль
        new_profile = UserProfiles(
            name=form_data.name,
            website_or_social=form_data.website_or_social,
            activity_and_hobbies=form_data.activity_hobbies,
            user_logo_url=user_logo_path,
            adress=form_data.adress,
            city=form_data.city,
            coordinates=multi_point_wkt,
            is_in_mlm=form_data.is_in_mlm,
            is_incognito=form_data.is_incognito,
            is_moderated=False,
            is_admin=False,
            user_id=user.id,
            language=form_data.language
        )

        user.is_profile_created = True
        session.add(new_profile)
        await session.flush()  # Фиксируем изменения, чтобы получить ID нового профиля

        # Обновляем переменную profile, чтобы она ссылалась на новый профиль
        profile = new_profile
        logger.info(f"Создан профиль для пользователя {user.id}")

    # Работа с хэштегами
    if form_data.hashtags:  # Проверяем, что хэштеги переданы
        hashtags_list = [tag.strip().lower().lstrip("#") for tag in form_data["hashtags"] if tag.strip()]

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

    return {"message": "Профиль успешно сохранен"}


# ЭНДПОИНТЫ ДЛЯ РАБОТЫ С ИЗБРАННЫМ И СЧЕТЧИКАМИ ПОДПИСЧИКОВ

# Эндпоинт для добавления в избранное и увеличения счётчика подписчиков
@app.post("/favorites/add/")
async def add_to_favorites_and_increment(
    user_id: int,
    profile_id: int,
    redis_client: redis.Redis = Depends(get_redis_client),
    _: TokenData = Depends(check_user_token)
):
    """ Добавить профиль в избранное пользователя и увеличить счётчик подписчиков """
    try:
        add_status = await add_to_favorites(user_id=user_id, profile_id=profile_id)
        if add_status.get("status") == "already_in_favorites":
            return {"сообщение": f"Профиль {profile_id} уже в избранном"}

        new_count = await increment_subscribers_count(profile_id=profile_id)
        return {
            "сообщение": f"Профиль {profile_id} добавлен в избранное",
            "новое количество подписчиков": new_count,
        }
    except Exception as e:
        logger.error(f"Ошибка при добавлении профиля {profile_id} в избранное: {str(e)}")
        raise HTTPException(status_code=500, detail="Ошибка сервера")


# Эндпоинт для удаления из избранного и уменьшения счётчика подписчиков
@app.post("/favorites/remove/")
async def remove_from_favorites_and_decrement(
    user_id: int,
    profile_id: int,
    redis_client: redis.Redis = Depends(get_redis_client),
    _: TokenData = Depends(check_user_token)
):
    """ Удалить профиль из избранного пользователя и уменьшить счётчик подписчиков """
    try:
        remove_status = await remove_from_favorites(user_id=user_id, profile_id=profile_id)
        if remove_status.get("status") == "not_in_favorites":
            return {"сообщение": f"Профиль {profile_id} не был в избранном"}

        new_count = await decrement_subscribers_count(profile_id=profile_id)
        return {
            "сообщение": f"Профиль {profile_id} удалён из избранного",
            "новое количество подписчиков": new_count,
        }
    except Exception as e:
        logger.error(f"Ошибка при удалении профиля {profile_id} из избранного: {str(e)}")
        raise HTTPException(status_code=500, detail="Ошибка сервера")


# Эндпоинт для получения текущего счётчика подписчиков
@app.get("/subscribers/count/")
async def get_subscribers_count(
    profile_id: int,
    redis_client: redis.Redis = Depends(get_redis_client),
    _: TokenData = Depends(check_user_token)
):
    """ Получить текущее количество подписчиков профиля """
    try:
        count = await get_subscribers_count_from_cache(profile_id)
        if count is None:
            raise HTTPException(status_code=404, detail="Счётчик подписчиков не найден")
        return {"id_профиля": profile_id, "количество подписчиков": count}
    except Exception as e:
        logger.error(f"Ошибка при получении счётчика подписчиков профиля {profile_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Ошибка сервера")


# Эндпоинт для получения списка избранного пользователя
@app.get("/favorites/")
async def get_favorites(user_id: int, redis_client: redis.Redis = Depends(get_redis_client), _: TokenData = Depends(check_user_token)):
    """
    Получить список избранных профилей пользователя.

    :param user_id: ID пользователя.
    :return: Список ID избранных профилей.
    """
    try:
        favorites_list = await get_favorites_from_cache(user_id)  # Используем функцию из кэш-модуля
        logger.info(f"Список избранного для пользователя {user_id}: {favorites_list}")
        return {"id_пользователя": user_id, "favorites": favorites_list}
    except Exception as e:
        logger.error(f"Ошибка при получении избранного пользователя {user_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Ошибка сервера")


# ЭНДПОИНТЫ ДЛЯ ОТДАЧИ ПРОФИЛЕЙ

# Эндпоинт для получения всех профилей (Сначала ныряем в Редис, если там пусто - берем данные из БД)
@app.get("/profiles/all/")
async def get_all_profiles_to_client(
    page: int = Query(1, description="Номер страницы (начинается с 1).", ge=1),  # Страница (по умолчанию 1)
    sort_by: Optional[str] = Query(None, description="Параметр сортировки. Возможные значения: newest, popularity.", enum=["newest", "popularity"]),
    per_page: int = Query(50, description="Количество профилей на странице.", le=100),  # По умолчанию 50 профилей, максимум 100)
    user_id: Optional[int] = Query(None, description="ID авторизованного пользователя."),
    ip_address: Optional[str] = Query(None, description="IP-адрес неавторизованного пользователя."),
    redis_client: redis.Redis = Depends(get_redis_client)  # Зависимость для Redis
):
    """
    Получает все профили пользователей с пагинацией и сортировкой.

    :param page: Номер страницы (начинается с 1).
    :param sort_by: Параметр сортировки (опционально). Возможные значения: "newest", "popularity".
    :param per_page: Количество профилей на странице (максимум 100).
    :param user_id: ID авторизованного пользователя (обязательно, если не указан ip_address).
    :param ip_address: IP-адрес неавторизованного пользователя (обязательно, если не указан user_id).
    :return: Словарь с данными о профилях, включая пагинацию и общее количество.
    """
    # Проверяем, что передан хотя бы один из параметров (user_id или ip_address)
    if user_id is None and ip_address is None:
        raise HTTPException(status_code=400, detail="Необходимо указать либо user_id, либо ip_address.")

    try:
        # Сначала пытаемся получить данные из кэша
        cached_profiles = await get_all_profiles_from_cache(redis_client, page, sort_by, per_page, user_id, ip_address)
        if cached_profiles["profiles"]:  # Если в кэше есть данные
            logger.info("Профили получены из кэша.")
            return cached_profiles

        # Если в кэше ничего нет, обращаемся к базе данных
        logger.info("Кэш пуст, запрашиваем данные из базы данных.")
        profiles_data = await get_all_profiles(page, sort_by, per_page, user_id, ip_address)

        # Возвращаем данные
        return profiles_data
    except HTTPException:
        raise  # Пробрасываем HTTP-исключения без изменений
    except Exception as e:
        logger.error(f"Ошибка в эндпоинте /profiles/all/: {str(e)}")
        raise HTTPException(status_code=500, detail="Ошибка при получении профилей")


# Эндпоинт для получения профилей по городу
@app.get("/profiles/city/")
async def get_profiles(
    city: str,
    page: int = Query(1, ge=1),  # Стартовая страница по умолчанию 1, минимум 1
    per_page: int = Query(25, le=100),  # По умолчанию 25 профилей, максимум 100
    sort_by: Optional[str] = Query(None, enum=["newest", "popularity"])
):
    result = await get_profiles_by_city(city, page, sort_by, per_page)
    return result


# Эндпоинт получения пользователя по номеру кошелька
@app.get("/profile/by_wallet_number/")
async def get_profile_by_wallet_number_endpoint(wallet_number: str):
    """
    Получить профиль пользователя по номеру кошелька.

    :param wallet_number: Номер кошелька для поиска.
    :param _: Проверка токена пользователя.
    :return: Информация о профиле.
    """
    try:
        profile_data = await get_profile_by_wallet_number(wallet_number)
        return profile_data

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Ошибка при обработке запроса профиля по кошельку {wallet_number}: {e}")
        raise HTTPException(status_code=500, detail="Ошибка сервера при получении профиля.")


# Эндпоинт получения пользователя по имени
@app.get("/profile/by_username/")
async def get_profile_by_username_endpoint(username: str):
    """
    Получить профиль пользователя по имени.

    :param username: Имя пользователя для поиска.
    :param _: Проверка токена пользователя.
    :return: Информация о профиле.
    """
    try:
        profile_data = await get_profile_by_username(username)
        return profile_data

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Ошибка при обработке запроса профиля по имени {username}: {e}")
        raise HTTPException(status_code=500, detail="Ошибка сервера при получении профиля.")


# Эндпоинт для получения профилей по хэштегу с кэшированием и сортировкой
@app.get("/profiles/by-hashtag/")
async def get_profiles_by_hashtag_endpoint(
    hashtag: str,
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=10, ge=1, le=100),
    sort_by: Optional[str] = Query(None, enum=["newest", "popularity"]),
    redis_client: redis.Redis = Depends(get_redis_client)
):
    try:
        # Формируем ключ кэша на основе параметров запроса
        cache_key = f"profiles_hashtag_{hashtag}_page_{page}_per_page_{per_page}_sort_{sort_by}"

        # Проверяем, есть ли данные в кэше
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            logger.info(f"Данные найдены в кэше для ключа {cache_key}.")
            return JSONResponse(content=json.loads(cached_data))

        # Если кэш пуст, получаем данные из базы данных
        response_data = await get_profiles_by_hashtag(hashtag, page, per_page, sort_by)
        logger.info("Профили успешно загружены из базы данных.")

        return JSONResponse(content=response_data)

    except HTTPException:
        raise  # Пробрасываем HTTP-исключения без изменений
    except Exception as e:
        logger.error(f"Ошибка в эндпоинте /profiles/by-hashtag/: {str(e)}")
        raise HTTPException(status_code=500, detail="Ошибка при получении профилей")



# Эндпоинт для получения профилей по ID
@app.get("/profiles/", response_model=List[dict])
async def get_profiles(profile_ids: List[int] = Query(..., description="Список ID профилей")):
    """
    Получает данные профилей по их ID. Сначала проверяет кеш (Redis), затем базу данных (БД).
    """
    try:
        profiles = await get_profiles_by_ids(profile_ids)
        return profiles
    except HTTPException as http_e:
        # Если была ошибка HTTP (например, 500), пробрасываем её дальше
        raise http_e
    except Exception as e:
        # Логируем ошибку и возвращаем 500
        logger.error(f"Ошибка в эндпоинте /profiles/: {str(e)}")
        raise HTTPException(status_code=500, detail="Ошибка при получении профилей")


# Эндпоинт получения профилей пользователей в радиусе 10 км от клиента
@app.get("/profiles/nearby")
async def get_profiles_nearby(
    longitude: float = Query(..., description="Долгота пользователя, например: -175", ge=-180, le=180),
    latitude: float = Query(..., description="Широта пользователя, например: 85", ge=-90, le=90),
    radius: int = Query(10000, description="Радиус поиска в метрах (по умолчанию 10 км)", ge=1)
):
    """
    Получает список профилей пользователей, находящихся в радиусе N метров от заданных координат.

    Аргументы:
        longitude (float): Долгота пользователя (обязательный параметр).
        latitude (float): Широта пользователя (обязательный параметр).
        radius (int): Радиус поиска в метрах (по умолчанию 10 км).

    Возвращает:
        list[dict]: Список профилей пользователей, соответствующих заданным критериям.
    """
    try:
        return await fetch_nearby_profiles(longitude, latitude, radius)
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка при выполнении запроса: {str(e)}"
        )


# ЭНДПОИНТЫ ДЛЯ РАБОТЫ С JWT
# Генерация новых токенов после протухания
@app.post("/refresh-tokens", response_model=TokenResponse)
async def refresh_tokens_endpoint(refresh_token: str):
    try:
        # Валидируем refresh токен
        user_data = await verify_refresh_token(refresh_token)

        # Генерируем новые токены
        new_tokens = await create_tokens(user_data.user_id)
        return new_tokens

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Ошибка при генерации новых токенов: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка при генерации новых токенов")


# Ендпоинт для выдачи прав администратора
@app.post("/grant-admin-rights")
async def grant_admin_rights_endpoint(request_wallet: str, target_wallet: str, _: TokenData = Depends(check_user_token)):
    """
    Ендпоинт для выдачи прав администратора.

    Параметры:
        request_wallet (str): Кошелек, с которого поступил запрос.
        target_wallet (str): Кошелек, которому нужно дать права администратора.

    Возвращает:
        dict: Сообщение о результате операции.
    """
    try:
        success = await grant_admin_rights(request_wallet, target_wallet)
        if success:
            return {"message": f"Права администратора успешно выданы для кошелька: {target_wallet}"}
        else:
            raise HTTPException(status_code=403, detail="Неавторизованный запрос.")
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Ошибка в ендпоинте grant-admin-rights: {e}")
        raise HTTPException(status_code=500, detail="Ошибка сервера.")


# Ендпоинт для отправки профилей на модерацию
@app.get("/moderation")
async def moderation_endpoint(admin_wallet: str, page: int = 1):
    """
    Ендпоинт для получения профилей на модерацию.

    Параметры:
        admin_wallet (str): Кошелек администратора, который запрашивает профили.
        page (int): Номер страницы (начинается с 1).

    Возвращает:
        dict: Словарь с данными о профилях, включая пагинацию и общее количество.

    Исключения:
        HTTPException: Если запрос не от администратора или произошла ошибка.
    """
    try:
        # Просто вызываем функцию, сессия открывается внутри нее
        return await get_profiles_for_moderation(admin_wallet, page)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Ошибка в ендпоинте /moderation: {e}")
        raise HTTPException(status_code=500, detail="Ошибка сервера.")


# Ендпоинт для модерации профиля
@app.post("/moderate-profile")
async def moderate_profile_endpoint(
    admin_wallet: str,  # Нехэшированный кошелек администратора
    profile_id: int,  # ID профиля для модерации
    moderation: bool,  # True — профиль прошел модерацию, False — не прошел
):
    """
    Ендпоинт для модерации профилей.

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
        # Просто вызываем функцию, сессия открывается внутри нее
        return await moderate_profile(admin_wallet, profile_id, moderation)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Ошибка в ендпоинте /moderate-profile: {e}")
        raise HTTPException(status_code=500, detail="Ошибка сервера, да.")


# ЭНДПОИНТ ДЛЯ НАПОЛНЕНИЯ БД, ПОТОМ УДАЛИТЬ ЕГО И МОДУЛЬ ФЕЙК ПРОФИЛЕЙ!!!!!!!!!!!!!
@app.post("/fill-database")
async def fill_database(session: AsyncSession = Depends(get_db_session)):
    try:
        await generate_profiles()
        return {"message": "Database filled successfully!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



