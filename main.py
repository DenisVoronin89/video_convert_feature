import os
from pydantic import HttpUrl
from fastapi import FastAPI, UploadFile, HTTPException, File
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from aiobotocore.session import get_session
import redis.asyncio as redis


from database import init_db, engine
from logging_config import get_logger
from video_handle.video_handler_publisher import publish_task
from views import save_video_to_temp, save_image_to_temp, create_directories, move_image_to_user_logo
from schemas import FormData, validate_and_process_form, is_valid_image, is_valid_video, serialize_form_data
from authentification import get_random_wallet
from check_payment import check_payment

# Получаем логгер
logger = get_logger()

# Конфиги для разработки и тестирования (НЕ ДЛЯ ПРОДАКШЕНА)
S3_BUCKET_NAME = "video-service"
AWS_REGION = "us-east-1"
AWS_ACCESS_KEY_ID = "SuOpyKZ54797K7y9vvaJ"
AWS_SECRET_ACCESS_KEY = "6NBChpstlgkjvRTawqKRuNvGBVNRG8EWIPCu4Izl"

PREVIEW_DURATION = 5 # Длина превью

# Сессия для работы с AWS S3
s3_session = get_session()

# Словарь необходимых директорий для работы (прилетает в функцию create_directories)
directories_to_create = {
    "video_temp": "./video_temp",
    "image_temp": "./image_temp",
    "output_video": "./output_video",
    "output_preview": "./output_preview",
    "user_logo": "./user_logo"
}

app = FastAPI()

# Раздача файлов из папки user_logo (ПОТОМ С СЕРВАКА КОГДА ОТДАВАТЬ БУДЕМ СДЕЛАТЬ ПРАВИЛЬНЫЙ КОНФИГ!!!!!!)
#  в функции def move_image_to_user_logo (вьюхи) тоже поставить правильный конфиг в переменной!!!!!!
app.mount("/user_logo", StaticFiles(directory="user_logo"), name="user_logo")


@app.on_event("startup")
async def startup():
    """Функция запуска приложения"""
    try:
        # Инициализация БД
        await init_db()
        logger.info("Приложение успешно запущено. Соединение с базой данных установлено.")
        logger.info("Таблицы созданы успешно.")

        # Создание директорий
        created_dirs = await create_directories(directories_to_create)

        # Сохраняем директории в состояние приложения
        app.state.created_dirs = created_dirs

        # Логирование итогов
        dirs_created = {dir: path for dir, path in created_dirs.items() if path == "успешно создана"}
        if dirs_created:
            logger.info(f"Директории для использования успешно созданы: {dirs_created}")
    except Exception as e:
        logger.error(f"Ошибка при запуске приложения: {e}")

    except Exception as e:
        # Разделяем логи для ошибок
        if 'init_db' in str(e):
            logger.error(f"Ошибка при инициализации БД: {str(e)}")
        elif 'create_directories' in str(e):
            logger.error(f"Ошибка при создании директорий: {str(e)}")
        else:
            logger.error(f"Неизвестная ошибка: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка при запуске приложения: {str(e)}")


@app.on_event("shutdown")
async def shutdown():
    """Функция завершения работы приложения"""
    await engine.dispose()
    logger.info("Приложение завершило работу. Соединение с базой данных закрыто.")

# Доступ к путям через ключи (пока тут чтобы вытаскивать их, потом удалю)
# video_temp_path = created_dirs["video_temp"]
# image_temp_path = created_dirs["image_temp"]
# output_video_path = created_dirs["output_video"]
# output_preview_path = created_dirs["output_preview"]
# user_logo_path = created_dirs["user_logo"]


# Эндпоинт для загрузки изображения
@app.post("/upload_image/")
async def upload_image(file: UploadFile = File(...)):
    try:
        logger.info("Получен запрос на загрузку изображения.")

        # Получаем директории из состояния приложения
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
async def upload_video(file: UploadFile = File(...)):
    try:
        logger.info("Получен запрос на загрузку видео.")

        # Получаем директории из состояния приложения
        created_dirs = app.state.created_dirs

        # Проверка валидности видео
        if not is_valid_video(file):
            logger.warning(f"Неверный формат видео: {file.filename}")
            raise HTTPException(status_code=400, detail="Неверный формат видео")

        # Сохранение видео с использованием уже описанной функции
        video_path = await save_video_to_temp(file, created_dirs)

        logger.info(f"Видео успешно загружено: {video_path}")
        return {"message": "Видео успешно загружено", "video_path": video_path}

    except HTTPException as e:
        logger.error(f"Ошибка валидации видео: {str(e)}")
        raise e
    except Exception as e:
        logger.error(f"Произошла ошибка при загрузке видео: {str(e)}")
        raise HTTPException(status_code=500, detail="Ошибка при загрузке видео")


# Эндпоинт валидации формы TODO форму кэшировать надо пока проверка платежа проходит (фронт озадачить! ЭТО ВАЖНО!!!!!))))
@app.post("/check_form/")
async def check_form(data: FormData):
    logger.debug(f"Получены данные: {data}")
    try:
        # Логируем начало обработки данных формы
        logger.info("Получены данные формы: %s", data.dict())

        # Валидация и обработка данных формы
        form_result = await validate_and_process_form(data)

        # Логируем успешную обработку данных
        logger.info("Данные формы успешно обработаны: %s", form_result)

        # Возвращаем результат успешной обработки
        return JSONResponse(
            status_code=200,
            content={
                "status": "form_validated",
                "form_validation": form_result
            }
        )

    except HTTPException as e:
        # Логируем ошибку уровня HTTP
        logger.error(f"Ошибка HTTP при обработке формы: {e.detail}")
        raise e

    except Exception as e:
        # Логируем любую неожиданную ошибку
        logger.error(f"Неожиданная ошибка при обработке данных формы: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка при обработке данных формы: {str(e)}"
        )


# Проверка транзакции
@app.post("/transaction_check/")
async def process_payment_check():
    try:
        payment_result = await check_payment()
        if not payment_result:
            raise HTTPException(status_code=400, detail="Платеж не подтвержден")
        return {"payment_status": "Платеж подтвержден"}
    except Exception as e:
        logger.error(f"Ошибка при проверке платежа: {e}")
        raise HTTPException(status_code=500, detail="Ошибка при проверке платежа")



@app.post("/save_profile/")
async def save_profile(profile_data: FormData, image_data: dict, video_data: dict):
    """
    Получаем данные профиля, пути к изображению и видео (в виде JSON), проверяем пути
    и отправляем задачу на обработку в Redis.
    """
    # Преобразуем данные формы в словарь
    form_data_dict = profile_data.dict()

    # Сериализуем данные формы (преобразуем HttpUrl в строку)
    form_data_dict = await serialize_form_data(form_data_dict)

    # Логируем полученные данные
    logger.info(f"Получены данные профиля: {form_data_dict}")
    logger.info(f"Получены данные о изображении: {image_data}")
    logger.info(f"Получены данные о видео: {video_data}")

    # Извлекаем пути к файлам из JSON
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

    # Получаем директории из состояния приложения
    created_dirs = app.state.created_dirs
    if not created_dirs:
        logger.error("Каталоги для сохранения файлов не были инициализированы.")
        raise HTTPException(status_code=500, detail="Ошибка при инициализации каталогов.")

    # Преобразуем пути в абсолютные, если они относительные
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

    # Получаем кошелек для пользователя
    wallet_number = await get_random_wallet()

    # Переносим изображение в постоянную папку "user_logo"
    try:
        # Переносим изображение и получаем путь
        user_logo_path = await move_image_to_user_logo(absolute_image_path, created_dirs)
        logger.info(f"Изображение успешно перемещено в постоянную папку: {user_logo_path}")
    except Exception as e:
        logger.error(f"Ошибка при перемещении изображения: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка при перемещении изображения: {str(e)}")

    # Преобразуем user_logo_path в строку, если это объект HttpUrl (поизысканное решение)
    if isinstance(user_logo_path, HttpUrl):
        user_logo_path = str(user_logo_path)

    try:
        # Логируем начало обработки запроса
        logger.info("Обработка данных профиля...")

        # Подключаемся к Redis
        redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        logger.info("Соединение с Redis установлено.")

        # Логируем задачу перед отправкой в Redis
        logger.info(f"Публикуемые данные в Redis: {{"
                     f"input_path: {absolute_video_path}, "
                     f"output_path: {created_dirs['output_video']}, "
                     f"preview_path: {created_dirs['output_preview']}, "
                     f"user_logo_url: {user_logo_path}, "
                     f"wallet_number: {wallet_number}, "
                     f"form_data: {form_data_dict}}}")

        # Публикуем задачу в Redis
        await publish_task(
            redis_client,
            input_path=absolute_video_path,  # Путь к видео
            output_path=created_dirs["output_video"],  # Путь для итогового видео
            preview_path=created_dirs["output_preview"],  # Путь для превью
            user_logo_url=user_logo_path,  # Путь к изображению, которое переехало в постоянную папку на сервере
            wallet_number=wallet_number,  # Кошелек
            form_data=form_data_dict  # Данные формы для сохранения в БД
        )
        logger.info("Задача успешно отправлена в Redis.")

        # Ответ клиенту
        return {"message": "Ваш профиль успешно сохранен и отправлен на модерацию."}

    except redis.RedisError as e:
        # Логируем ошибку при работе с Redis
        logger.error(f"Ошибка при подключении или публикации в Redis: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка при сохранении профиля в Redis: {str(e)}")

    except Exception as e:
        # Логируем общие ошибки
        logger.error(f"Ошибка при сохранении профиля: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка при сохранении профиля: {str(e)}")
