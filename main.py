import ffmpeg
import os
import logging
from uuid import uuid4
from fastapi import FastAPI, UploadFile, HTTPException, BackgroundTasks
from aiobotocore.session import get_session
import aiofiles
import io
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table, Column, Integer, String
import time


# Настройка логов
log_level = logging.DEBUG
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_file = 'video_service.log'
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(log_formatter)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)
logger.addHandler(file_handler)

# Конфиги для разработки и тестирования (НЕ ДЛЯ ПРОДАКШЕНА)
DATABASE_URL = "postgresql+asyncpg://admin:admin1224@localhost/sst_video_app"
S3_BUCKET_NAME = "video-service"
AWS_REGION = "us-east-1"
AWS_ACCESS_KEY_ID = "SuOpyKZ54797K7y9vvaJ"
AWS_SECRET_ACCESS_KEY = "6NBChpstlgkjvRTawqKRuNvGBVNRG8EWIPCu4Izl"

PREVIEW_DURATION = 5 # Длина превью вынесена в переменную, так проще потом настраивать будет как нам надо

# Конфиги БД
engine = create_async_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
metadata = MetaData()

# Сессия для работы с AWS S3
s3_session = get_session()

# Таблица
videos = Table(
    "videos", metadata,
    Column("id", Integer, primary_key=True),
    Column("video_url", String(255)),
    Column("preview_url", String(255))
)


app = FastAPI()


@app.on_event("startup")
async def startup():
    """Функция запуска приложения"""
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    logger.info("Приложение успешно запущено. Соединение с базой данных установлено.")


@app.on_event("shutdown")
async def shutdown():
    """Функция завершения работы приложения"""
    await engine.dispose()
    logger.info("Приложение завершило работу. Соединение с базой данных закрыто.")


async def create_temp_dir():
    """Создание временной директории (temp) для файлов если ее нет"""
    try:
        temp_dir = "./temp"
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
            logger.info(f"Создана временная директория: {temp_dir}")
        else:
            logger.info(f"Временная директория уже существует: {temp_dir}")
    except Exception as e:
        logger.error(f"Ошибка при создании временной директории: {e}")
        raise RuntimeError("Не удалось создать временную директорию")


async def save_file_to_temp(file: UploadFile):
    """Сохранение видео в папку temp"""
    try:
        await create_temp_dir()
        temp_path = f"./temp/{uuid4()}_{file.filename}"
        async with aiofiles.open(temp_path, "wb") as out_file:
            await out_file.write(await file.read())
        file_size = get_file_size(temp_path)
        logger.info(f"Файл сохранён во временной директории: {temp_path} (Размер: {file_size:.2f} MB)")
        return temp_path
    except Exception as e:
        logger.error(f"Ошибка при сохранении файла во временной директории: {e}")
        raise RuntimeError("Не удалось сохранить файл во временной директории")


def get_file_size(path):
    """Получение веса файла (в Мб)"""
    size = os.path.getsize(path) / (1024 * 1024)
    logger.debug(f"Размер файла {path}: {size:.2f} MB")
    return size


def convert_to_vp9(input_path, output_path, logger):
    """Конвертация видео с сохранением качества и минимизацией увеличения размера"""
    start_time = time.time()
    try:
        logger.info(f"Запуск конвертации видео в VP9: {input_path} -> {output_path}")
        (
            ffmpeg
            .input(input_path)
            .output(output_path, vcodec='libvpx-vp9', acodec='libopus', crf=30, audio_bitrate='96k')
            .overwrite_output()
            .run(capture_stdout=True, capture_stderr=True)
        )
        elapsed_time = time.time() - start_time  # Замеряем время обработки
        output_size = get_file_size(output_path)
        logger.info(
            f"Конвертация в VP9 завершена: {output_path} (Размер: {output_size:.2f} MB, Время: {elapsed_time:.2f} сек.)")
    except ffmpeg.Error as e:
        logger.error(f"Ошибка FFmpeg при конвертации: {e.stderr.decode()}")
        raise RuntimeError("Не удалось конвертировать видео в VP9")
    logger.info(f"Видео успешно обработано: {output_path} (Размер: {output_size:.2f} MB)")


def extract_preview(input_path, output_path, duration=PREVIEW_DURATION, logger=None):
    """Извлечение превью"""
    start_time = time.time()
    try:
        logger.info(f"Извлечение превью из видео: {input_path} -> {output_path} (Длительность: {duration} сек.)")
        (
            ffmpeg
            .input(input_path, ss=0, t=duration)
            .output(output_path, vcodec='libvpx-vp9', acodec='libopus')
            .overwrite_output()
            .run(capture_stdout=True, capture_stderr=True)
        )
        elapsed_time = time.time() - start_time
        preview_size = get_file_size(output_path)
        logger.info(
            f"Извлечение превью завершено: {output_path} (Размер: {preview_size:.2f} MB, Время: {elapsed_time:.2f} сек.)")
    except ffmpeg.Error as e:
        logger.error(f"Ошибка FFmpeg при извлечении превью: {e.stderr.decode()}")
        raise RuntimeError("Не удалось извлечь превью")
    logger.info(f"Превью успешно извлечено: {output_path} (Размер: {preview_size:.2f} MB)")


async def upload_to_s3(file_path, s3_key, logger):
    """Стримминговая загрузка видео в S3"""
    try:
        logger.info(f"Загрузка файла в S3: {file_path} -> {s3_key}")

        async with aiofiles.open(file_path, 'rb') as f:
            file_stream = io.BytesIO(await f.read())

        logger.debug(f"Тип данных file_stream: {type(file_stream)}")

        # Создание сессии для работы с S3
        async with get_session().create_client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url="http://localhost:9000",
        ) as s3_client:
            # Стриминговая загрузка файла
            response = await s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=file_stream,
            )

            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise RuntimeError("Ошибка при загрузке файла в S3: некорректный код ответа")

        logger.info(f"Файл успешно загружен в S3: {s3_key}")
    except Exception as e:
        logger.error(f"Ошибка при загрузке файла в S3: {e}")
        raise RuntimeError("Не удалось загрузить файл в S3")


async def save_video_to_db(video_url, preview_url, logger):
    """Сохранение ссылок на видео и превью в БД"""
    try:
        async with SessionLocal() as session:
            video = videos.insert().values(video_url=video_url, preview_url=preview_url)
            await session.execute(video)
            await session.commit()
            logger.info(f"Видео и превью успешно сохранены в базе данных: {video_url}, {preview_url}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении видео в базе данных: {e}")
        raise RuntimeError("Не удалось сохранить видео в базе данных")


# Эндпоинт для загрузки видео
@app.post("/upload_video/")
async def upload_video(file: UploadFile, background_tasks: BackgroundTasks):
    """Загрузка и обработка видео."""
    logger.info("Начало обработки загрузки видео")
    temp_path = None
    converted_video_path = None
    preview_path = None
    try:
        # Сохранение видео
        temp_path = await save_file_to_temp(file)

        # Создание уникального имени для конвертированного видео и превью
        converted_video_path = f"./temp/converted_{uuid4()}.webm"
        preview_path = f"./temp/preview_{uuid4()}.webm"

        # Добавление задач в фон
        background_tasks.add_task(convert_to_vp9, temp_path, converted_video_path, logger)
        background_tasks.add_task(extract_preview, converted_video_path, preview_path, duration=PREVIEW_DURATION, logger=logger)

        # Загрузка в S3 (работа в фоне)
        video_s3_key = f"videos/{uuid4()}_converted.webm"
        preview_s3_key = f"previews/{uuid4()}_preview.webm"
        background_tasks.add_task(upload_to_s3, converted_video_path, video_s3_key, logger)
        background_tasks.add_task(upload_to_s3, preview_path, preview_s3_key, logger)

        # Сохранение видео в БД (работа в фоне)
        video_url = f"http://localhost:9000/{S3_BUCKET_NAME}/{video_s3_key}"
        preview_url = f"http://localhost:9000/{S3_BUCKET_NAME}/{preview_s3_key}"
        background_tasks.add_task(save_video_to_db, video_url, preview_url, logger)

        # Добавление задачи для удаления временных файлов в фон (после завершения всех операций)
        background_tasks.add_task(delete_temp_files, temp_path, converted_video_path, preview_path)

        # Немедленно возвращает ответ клиенту TODO после успешной конвертации клиенту пуш уведомление какое то надо выкидывать (Спросить у Евгения на созвоне)
        logger.info(f"Задача по обработке видео добавлена в фон: {video_url}, {preview_url}")
        return {"message": "Видео успешно загружено и обрабатывается в фоне", "video_url": video_url, "preview_url": preview_url}

    except Exception as e:
        logger.error(f"Ошибка при обработке видео: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при обработке видео: {e}")


async def delete_temp_files(temp_path, converted_video_path, preview_path):
    """Удаление временных файлов после завершения всех операций."""
    try:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
            logger.info(f"Удален временный файл: {temp_path}")
        if converted_video_path and os.path.exists(converted_video_path):
            os.remove(converted_video_path)
            logger.info(f"Удален временный файл конвертированного видео: {converted_video_path}")
        if preview_path and os.path.exists(preview_path):
            os.remove(preview_path)
            logger.info(f"Удален временный файл превью: {preview_path}")
    except Exception as e:
        logger.error(f"Ошибка при удалении временных файлов: {e}")

