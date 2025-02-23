import os
import asyncio
from pydantic import HttpUrl
from fastapi import FastAPI, UploadFile, HTTPException, File, Depends, Query, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as redis
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
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
    get_cached_profiles
)
from tokens import TokenData, create_tokens, verify_access_token
from utils import delete_old_files_task, parse_coordinates


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


# Настройка APScheduler для выполнения задач по расписанию
async def start_scheduler():
    """Запуск планировщика задач."""
    scheduler = AsyncIOScheduler()

    # Задача, которая выполняется каждые 3 минуты (слив каунтера звездочек и избранного из Редиски в БД)
    # Вместо использования asyncio.run() вызываем саму асинхронную функцию
    scheduler.add_job(sync_data_to_db, IntervalTrigger(minutes=3))
    logger.info("Задача sync_data_to_db добавлена в расписание (каждые 3 минуты).")

    # Задача, которая выполняется каждую минуту (получаем в Редиску 50 профилей на отгрузку при входе в приложение)
    scheduler.add_job(get_sorted_profiles, IntervalTrigger(minutes=1))
    logger.info("Задача get_sorted_profiles добавлена в расписание (каждую минуту).")

    # Задача очистки временных файлов, выполняемая ежедневно в 00:00
    scheduler.add_job(delete_old_files_task, CronTrigger(hour=0, minute=0, second=0))
    logger.info("Задача delete_old_files_task добавлена в расписание (ежедневно в 00:00).")

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


# Установка зависимости для подключения Редиса, чтобы прокидывать потом в нужные эндпоинты
async def get_redis_client(redis_client: redis.Redis = Depends(lambda: app.state.redis_client)) -> redis.Redis:
    return redis_client


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
@app.post("/user/login", response_model=UserResponse)
async def login(wallet_number: str, session: AsyncSession = Depends(get_db_session), redis_client: redis.Redis = Depends(get_redis_client)):
    try:
        # Хэшируем номер кошелька
        logger.info("Начало хеширования номера кошелька.")
        hashed_wallet_number = hashlib.sha256(wallet_number.encode()).hexdigest()
        logger.info(f"Хеш номера кошелька: {hashed_wallet_number}")

        # Ищем пользователя в БД
        logger.info("Поиск пользователя в базе данных.")
        stmt = select(User).filter(User.wallet_number == hashed_wallet_number)
        result = await session.execute(stmt)
        user = result.scalar_one_or_none()

        if not user:
            # 3. Если пользователя нет, создаем нового
            logger.info("Пользователь не найден, создаем нового.")
            user = User(wallet_number=hashed_wallet_number)
            session.add(user)
            await session.commit()
            await session.refresh(user)
            logger.info(f"Создан новый пользователь с ID {user.id}")
        else:
            logger.info(f"Пользователь найден с ID {user.id}")

        # Получаем профиль пользователя (если есть)
        logger.info("Загрузка профиля пользователя из базы данных.")
        profile_stmt = select(UserProfiles).filter(UserProfiles.user_id == user.id)
        profile_result = await session.execute(profile_stmt)
        profile = profile_result.scalar_one_or_none()

        # Преобразуем профиль в ответ
        profile_info = None
        if profile:
            # Обработка координат
            coordinates = None
            if profile.coordinates:
                try:
                    # Преобразуем строку WKT в геометрический объект
                    geometry = wkt.loads(str(profile.coordinates))
                    # Получаем список координат (если это Point, то просто вернем его координаты)
                    coordinates = [list(geometry.coords)[0]] if isinstance(geometry, Point) else [list(coord) for coord in geometry.coords]
                except Exception as e:
                    logger.error(f"Ошибка при парсинге координат: {str(e)}")

            profile_info = UserProfileResponse(
                id=profile.id,
                created_at=profile.created_at,
                name=profile.name,
                user_logo_url=profile.user_logo_url,
                video_url=profile.video_url,
                preview_url=profile.preview_url,
                activity_and_hobbies=profile.activity_and_hobbies,
                is_moderated=profile.is_moderated,
                is_incognito=profile.is_incognito,
                is_in_mlm=profile.is_in_mlm,
                website_or_social=profile.website_or_social,
                is_admin=profile.is_admin,
                adress=profile.adress if isinstance(profile.adress, list) else [],
                city=profile.city,
                coordinates=coordinates,  # Отдаем уже преобразованные координаты
                followers_count=profile.followers_count
            )
            logger.info(f"Профиль пользователя: {profile_info}")

        # Получаем хэштеги пользователя, если профиль существует
        hashtags_info = []
        if profile:
            logger.info("Загрузка хэштегов пользователя.")
            hashtags_stmt = select(Hashtag).join(ProfileHashtag).filter(ProfileHashtag.profile_id == profile.id)
            hashtags_result = await session.execute(hashtags_stmt)
            hashtags = hashtags_result.scalars().all()
            hashtags_info = [hashtag.tag for hashtag in hashtags] if hashtags else []
            logger.info(f"Хэштеги пользователя: {hashtags_info}")
        else:
            logger.info("Профиль пользователя не найден, хэштеги не будут загружены.")

        # Получаем избранное из кэша Redis
        logger.info("Попытка получить избранное из кэша Redis.")
        favorite_ids = await get_favorites_from_cache(user.id)

        # Если в Redis пусто
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

        # Формируем и возвращаем ответ
        logger.info(f"Возвращаем ответ для пользователя с ID {user.id}.")
        return UserResponse(
            id=user.id,
            profile=profile_info,
            favorites=favorite_ids,
            hashtags=hashtags_info,
            tokens=tokens,
        )

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

        session.add(profile)
        logger.info(f"Обновлен профиль пользователя {user.id}")
    else:
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
            user_id=user.id
        )

        user.is_profile_created = True
        session.add(new_profile)
        logger.info(f"Создан профиль для пользователя {user.id}")

    # Работа с хэштегами
    if form_data.hashtags:
        hashtags_list = [tag.strip().lower() for tag in form_data.hashtags if tag.strip()]
        if hashtags_list:
            # Поиск и проверка существующих хэштегов
            existing_hashtags_stmt = select(Hashtag).where(Hashtag.tag.in_(hashtags_list))
            existing_hashtags_result = await session.execute(existing_hashtags_stmt)
            existing_hashtags = {tag.tag: tag for tag in existing_hashtags_result.scalars().all()}

            for hashtag in hashtags_list:
                if hashtag not in existing_hashtags:
                    # Если хэштег отсутствует - добавляем
                    new_hashtag = Hashtag(tag=hashtag)
                    session.add(new_hashtag)
                    await session.flush()  # Дожидаемся генерации ID

                    # Добавление связи между профилем и хэштегом
                    profile_hashtag = ProfileHashtag(profile_id=profile.id,  # Используй profile для обновления
                                                     hashtag_id=new_hashtag.id)  # Связь с профилем
                    session.add(profile_hashtag)
                else:
                    # Привязка существующего хэштега к профилю через таблицу ProfileHashtag
                    profile_hashtag = ProfileHashtag(profile_id=profile.id,  # Используй profile для обновления
                                                     hashtag_id=existing_hashtags[hashtag].id)  # Связь с профилем
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
    _: TokenData = Depends(check_user_token)):
    """ Добавить профиль в избранное пользователя и увеличить счётчик подписчиков """
    try:
        # Добавляем профиль в избранное
        await add_to_favorites(user_id=user_id, profile_id=profile_id)

        # Увеличиваем счётчик подписчиков
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
    """
    Удалить профиль из избранного пользователя и уменьшить счётчик подписчиков.

    :param user_id: ID пользователя.
    :param profile_id: ID профиля.
    """
    try:
        # Удаляем профиль из избранного
        await remove_from_favorites(user_id=user_id, profile_id=profile_id)

        # Уменьшаем счётчик подписчиков
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
async def get_subscribers_count(profile_id: int, redis_client: redis.Redis = Depends(get_redis_client), _: TokenData = Depends(check_user_token)):
    """
    Получить текущее количество подписчиков профиля.

    :param profile_id: ID профиля.
    """
    try:
        count = await get_subscribers_count_from_cache(profile_id)  # Используем функцию из кэш-модуля
        logger.info(f"Текущий счётчик подписчиков для профиля {profile_id}: {count}")
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
        return {"id_пользователя": user_id, "избранное": favorites_list}
    except Exception as e:
        logger.error(f"Ошибка при получении избранного пользователя {user_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Ошибка сервера")


# ЭНДПОИНТЫ ДЛЯ ОТДАЧИ ПРОФИЛЕЙ

# Эндпоинт для получения первых 50 профилей
@app.get("/profiles/main")
async def get_profiles(
    session: AsyncSession = Depends(get_db_session),
    redis_client: redis.Redis = Depends(get_redis_client),
    _: TokenData = Depends(check_user_token)
):
    """
    Получение первых 50 профилей. Сначала пытаемся получить их из кэша,
    если кэша нет, загружаем из базы данных.
    """
    try:
        # Проверяем, есть ли кэшированные профили
        cached_profiles = await get_cached_profiles(redis)  # Передаем redis клиент
        if cached_profiles:
            # Если кэш есть, возвращаем его
            return JSONResponse(content=cached_profiles)

        # Если кэш пуст, получаем профили из базы данных
        profiles_from_db = await get_sorted_profiles(session)

        # Возвращаем полученные профили
        return JSONResponse(content=profiles_from_db)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении профилей: {str(e)}")


# Эндпоинт для получения всех профилей
@app.get("/profiles/all/", response_model=dict)
async def get_all_profiles_to_client(
    page: int = 1,  # Страница (по умолчанию 1)
    sort_by: Optional[str] = Query(None, enum=["newest", "popularity"]),
    per_page: int = Query(25, le=100),  # По умолчанию 25 профилей, максимум 100
    db: AsyncSession = Depends(get_db_session),
    _: TokenData = Depends(check_user_token)
):
    try:
        # Обращаемся к функции для получения профилей
        profiles_data = await get_all_profiles(page, sort_by, per_page, db)

        # Возвращаем данные
        return profiles_data
    except Exception as e:
        # Если произошла ошибка, возвращаем 500 ошибку
        raise HTTPException(status_code=500, detail=str(e))


# Эндпоинт для получения профилей по городу
@app.get("/profiles/city/")
async def get_profiles(
    city: str,
    page: int = Query(1, ge=1),  # Стартовая страница по умолчанию 1, минимум 1
    per_page: int = Query(25, le=100),  # По умолчанию 25 профилей, максимум 100
    sort_by: Optional[str] = Query(None, enum=["newest", "popularity"]),
    db: AsyncSession = Depends(get_db_session),
    _: TokenData = Depends(check_user_token)
):
    result = await get_profiles_by_city(city, page, sort_by, per_page, db)
    return result


# Эндпоинт для получения профилей по хэштегу
@app.get("/profiles/hashtag/{hashtag}")
async def fetch_profiles_by_hashtag(
    hashtag: str,
    page: int = 1,
    per_page: int = 25,  # Установлено 25 профилей на страницу
    sort_by: Optional[str] = Query(None, enum=["newest", "popularity"]),
    db: AsyncSession = Depends(get_db_session),
    _: TokenData = Depends(check_user_token)
):
    try:
        result = await get_profiles_by_hashtag(hashtag, page, per_page, sort_by, db)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении данных: {e}")


# Эндпоинт получения пользователя по номеру кошелька
@app.get("/profile/by_wallet_number/")
async def get_profile_by_wallet_number(
        wallet_number: str,
        _: TokenData = Depends(check_user_token),
        db: AsyncSession = Depends(get_db_session),
):
    """
    Получить профиль пользователя по номеру кошелька.

    :param wallet_number: Номер кошелька для поиска.
    :param _: Проверка токена пользователя.
    :param db: Сессия базы данных.
    :return: Информация о профиле.
    """
    try:
        profile_data = await get_profile_by_wallet_number(wallet_number, db)
        return profile_data

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Ошибка при обработке запроса профиля по кошельку {wallet_number}: {e}")
        raise HTTPException(status_code=500, detail="Ошибка сервера при получении профиля.")



# Эндпоинт получения пользователя по имени
@app.get("/profile/by_username/")
async def get_profile_by_username(
        username: str,
        _: TokenData = Depends(check_user_token),
        db: AsyncSession = Depends(get_db_session),
):
    """
    Получить профиль пользователя по имени.

    :param username: Имя пользователя для поиска.
    :param _: Проверка токена пользователя.
    :param db: Сессия базы данных.
    :return: Информация о профиле.
    """
    try:
        # Поиск профиля по имени
        profile = db.query(UserProfiles).filter(UserProfiles.name.ilike(f"%{username}%")).first()

        if not profile:
            raise HTTPException(status_code=404, detail="Профиль с таким именем не найден.")

        # Отдаем информацию профиля
        return {
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

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail="Ошибка сервера при получении профиля.")


# Эндпоинт получения профилей пользователей в радиусе 10 км от клиента
@app.get("/profiles/nearby", response_model=List[UserProfileResponse])
async def get_profiles_nearby(
    latitude: float = Query(..., description="Широта пользователя"),
    longitude: float = Query(..., description="Долгота пользователя"),
    radius: int = Query(10000, description="Радиус поиска в метрах (по умолчанию 10 км)"),
    db_session: AsyncSession = Depends(get_db_session)
):
    """
    Эндпоинт для получения профилей в радиусе N метров от текущего местоположения пользователя.
    """
    try:
        # Формируем запрос с использованием geoalchemy2
        query = (
            select(UserProfiles)
            .where(
                ST_DWithin(
                    UserProfiles.coordinates,  # Поле координат
                    ST_MakePoint(longitude, latitude),  # Точка поиска
                    radius  # Радиус в метрах
                )
            )
        )
        # Выполняем запрос
        result = await db_session.execute(query)
        profiles = result.scalars().all()

        if not profiles:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Нет профилей в заданном радиусе"
            )
        return profiles
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка при выполнении поиска: {str(e)}"
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



# ЭНДПОИНТ ДЛЯ НАПОЛНЕНИЯ БД, ПОТОМ УДАЛИТЬ ЕГО И МОДУЛЬ ФЕЙК ПРОФИЛЕЙ!!!!!!!!!!!!!
@app.post("/fill-database")
async def fill_database(session: AsyncSession = Depends(get_db_session)):
    try:
        await generate_profiles()
        return {"message": "Database filled successfully!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



