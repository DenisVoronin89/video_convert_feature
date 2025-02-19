import os
import asyncio
from pydantic import HttpUrl
from fastapi import FastAPI, UploadFile, HTTPException, File, Depends, Query
from fastapi.security import OAuth2PasswordBearer
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as redis
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from geoalchemy2.functions import ST_DWithin, ST_MakePoint
import hashlib
from typing import Optional, List
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger


from database import init_db, engine, get_db_session, SessionLocal
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
from schemas import FormData, TokenResponse, UserProfileResponse, validate_and_process_form, is_valid_image, is_valid_video, serialize_form_data
from models import User, UserProfiles, Favorite, Hashtag, VideoHashtag
from cashe import (
    increment_subscribers_count,
    decrement_subscribers_count,
    get_subscribers_count_from_cache,
    add_to_favorites,
    remove_from_favorites,
    get_favorites_from_cache,
    sync_data_to_db,
    get_profiles_from_db,
    cache_profiles_in_redis,
    get_profiles_by_hashtag
)
from tokens import TokenData, create_tokens, verify_access_token
from utils import delete_old_files_task


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

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

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
async def check_user_token(token: str = Depends(oauth2_scheme)) -> TokenData:
    try:
        # Валидация access токена
        return await verify_access_token(token)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Ошибка при валидации access токена: {str(e)}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired access token")



# Эндпоинт для регистрации/авторизации пользователя, отдача избранного и информации о профиле на фронт, генерация токенов
@app.post("/user/login", response_model=UserProfileResponse)
async def login(wallet_number: str, session: AsyncSession = Depends(get_db_session)):
    """
    Эндпоинт для регистрации/авторизации пользователя.
    Добавлена генерация токенов.
    """
    try:
        # Хэшируем номер кошелька
        hashed_wallet_number = hashlib.sha256(wallet_number.encode()).hexdigest()
        logger.info(f"Номер кошелька захэширован: {hashed_wallet_number}")

        # Ищем пользователя в базе данных
        stmt = select(User).filter(User.wallet_number == hashed_wallet_number)
        result = await session.execute(stmt)
        user = result.scalar_one_or_none()

        if not user:
            # Если пользователь не найден, создаем нового
            user = User(wallet_number=hashed_wallet_number)
            session.add(user)
            await session.commit()
            await session.refresh(user)
            logger.info(f"Создан новый пользователь с ID {user.id}")

        # Генерируем токены
        tokens = await create_tokens(user.id)

        # Ищем профиль пользователя (если он создан)
        profile_stmt = select(UserProfiles).filter(UserProfiles.user_id == user.id)
        profile_result = await session.execute(profile_stmt)
        profile = profile_result.scalar_one_or_none()

        profile_info = UserProfileResponse.model_validate(profile) if profile else None

        # Формируем список избранного пользователя
        favorites_list = [
            {"profile_id": favorite.profile_id, "created_at": favorite.created_at}
            for favorite in user.favorites
        ]

        # Возвращаем информацию о пользователе, его профиле, избранном и токенах
        return {
            "user_id": user.id,
            "profile": profile_info,
            "favorites": favorites_list,
            "access_token": tokens.access_token,
            "refresh_token": tokens.refresh_token,
        }

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


# Эндпоинт валидации формы TODO форму кэшировать надо пока проверка платежа проходит (Макса озадачить! ЭТО ВАЖНО!!!!!))))
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
@app.post("/save profile without video/")
async def create_or_update_user_profile(
        profile_data: FormData,
        image_data: dict,
        _: TokenData = Depends(check_user_token),
        db: AsyncSession = Depends(get_db_session),
):
    try:
        # 1. Хэшируем номер кошелька
        wallet_number = profile_data.get("wallet_number")
        if not wallet_number:
            raise HTTPException(status_code=400, detail="Номер кошелька обязателен.")

        hashed_wallet_number = hashlib.sha256(wallet_number.encode()).hexdigest()
        logger.info(f"Номер кошелька захэширован: {hashed_wallet_number}")

        # 2. Проверяем наличие пути к изображению в image_data
        image_path = image_data.get("image_path")
        if not image_path:
            raise HTTPException(status_code=400, detail="Путь к изображению не указан.")

        absolute_image_path = os.path.abspath(image_path)
        logger.info(f"Абсолютный путь к изображению: {absolute_image_path}")

        if not os.path.isfile(absolute_image_path):
            raise HTTPException(status_code=400, detail="Изображение не найдено.")

        # 3. Получаем каталоги из состояния приложения
        created_dirs = router.app.state.created_dirs
        if not created_dirs:
            raise HTTPException(status_code=500, detail="Ошибка инициализации каталогов.")

        # 4. Перемещаем изображение в постоянную папку
        try:
            user_logo_path = await move_image_to_user_logo(absolute_image_path, created_dirs)
            logger.info(f"Изображение перемещено в папку: {user_logo_path}")
        except Exception as e:
            logger.error(f"Ошибка при перемещении изображения: {str(e)}")
            raise HTTPException(status_code=500, detail="Ошибка сохранения изображения.")

        # 5. Проверяем существование профиля по хешу кошелька
        stmt = select(UserProfiles).where(UserProfiles.hashed_wallet_number == hashed_wallet_number)
        result = await db.execute(stmt)
        existing_user = result.scalars().first()

        if existing_user:
            logger.info(f"Обновление профиля для кошелька: {hashed_wallet_number}")

            for key, value in profile_data.items():
                if key in UserProfiles.__table__.columns and key != "wallet_number":
                    setattr(existing_user, key, value)

            existing_user.user_logo_url = str(user_logo_path)

            await db.commit()
            await db.refresh(existing_user)
            return {"status": "updated", "user_id": existing_user.id}

        logger.info(f"Создание нового профиля для кошелька: {hashed_wallet_number}")

        new_user = UserProfiles(
            hashed_wallet_number=hashed_wallet_number,
            user_logo_url=str(user_logo_path),
            **{k: v for k, v in profile_data.items() if k in UserProfiles.__table__.columns and k != "wallet_number"}
        )

        db.add(new_user)
        await db.commit()
        await db.refresh(new_user)

        return {"status": "created", "user_id": new_user.id}

    except HTTPException as http_error:
        raise http_error
    except Exception as e:
        logger.error(f"Ошибка обработки профиля: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Ошибка сервера при обработке профиля")


# ЭНДПОИНТЫ ДЛЯ РАБОТЫ С КЭШЕМ

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
        profiles_from_db = await get_profiles_from_db(session)

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


# ЗАПУСК ЗАДАЧ, ВЫПОЛНЯЮЩИХСЯ ПО РАСПИСАНИЮ

# Настройка APScheduler для выполнения задач по расписанию
def start_scheduler():
    scheduler = AsyncIOScheduler()

    # Задача, которая выполняется каждые 3 минуты
    scheduler.add_job(sync_data_to_db, IntervalTrigger(minutes=3))
    logger.info("Задача sync_data_to_db добавлена в расписание (каждые 3 минуты).")

    # Задача, которая выполняется каждую минуту
    scheduler.add_job(get_profiles_from_db, IntervalTrigger(minutes=1))
    logger.info("Задача get_profiles_from_db добавлена в расписание (каждую минуту).")

    # Задача очистки временных файлов, выполняемая ежедневно в 00:00
    scheduler.add_job(
        delete_old_files_task,
        CronTrigger(hour=0, minute=0, second=0)
    )
    logger.info("Задача delete_old_files_task добавлена в расписание (ежедневно в 00:00).")

    # Старт планировщика
    scheduler.start()
    logger.info("Планировщик запущен и настроен.")


# Запуск планировщика с асинхронным циклом
if __name__ == "__main__":
    start_scheduler()  # Запускаем планировщик
    try:
        asyncio.get_event_loop().run_forever()  # Запускаем текущий цикл для работы с асинхронными задачами
    except (KeyboardInterrupt, SystemExit):
        logger.info("Сервер остановлен.")

