import subprocess
import os
import logging
import time
from uuid import uuid4
from fastapi import FastAPI, BackgroundTasks, HTTPException, UploadFile, File
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from databases import Database
from aiobotocore.session import get_session
from celery import Celery
import asyncio
import psutil
import aiofiles
import io

# Логирование
logging.basicConfig(level=logging.INFO, handlers=[
    logging.FileHandler("logs.log"),  # Запись логов в файл
    logging.StreamHandler()  # Также выводим в консоль
])
logger = logging.getLogger(__name__)

# Для разработки: конфигурации из переменных окружения
# В продакшене эти переменные будут вынесены в безопасное место
# Доступ к AWS будет осуществляться через IAM (Identity and Access Management)
DATABASE_URL = "mysql+aiomysql://admin:admin1224@localhost:3306/video_service"
S3_BUCKET_NAME = "video-service"
AWS_DEFAULT_REGION = "us-east-1"
REDIS_BROKER_URL = "redis://localhost:6379/0"
AWS_ACCESS_KEY_ID = "fTVbK2m426XPEQh6lSkQ"
AWS_SECRET_ACCESS_KEY = "wp005zugmCacEqqsCkHIWZaszU1yXFGNEQXW8y8T"

# БД конфиги (для асинхронной работы)
database = Database(DATABASE_URL)
metadata = MetaData()

# Асинк подключение к БД
engine = create_async_engine(DATABASE_URL, echo=True)

SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# Конфиги для AWS S3
s3_session = get_session()

# Конфиги для Celery, брокер Редиска
celery_app = Celery("tasks", broker=REDIS_BROKER_URL)

app = FastAPI()

# Модель для хранения ссылок на видео и превью в БД
videos_table = Table(
    "videos",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("video_url", String(255), index=True),
    Column("preview_url", String(255)),
)

# Создание таблицы, если она не существует
@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)


async def run_ffmpeg_command(command: list):
    """ Команда FFmpeg с мониторингом использования ресурсов системы"""
    start_time = time.time() # Начальное время выполнения
    cpu_start = psutil.cpu_percent() # Загрузка CPU до
    mem_start = psutil.virtual_memory().percent # Загрузка памяти до

    try:
        logger.info(f"Running FFmpeg command: {' '.join(command)}") # Лог запуска команды
        # Запуск команды FFmpeg
        process = await asyncio.create_subprocess_exec(*command, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # Распаковка списка аргументов в subprocess, захват станд вывода и ошибок
        stdout, stderr = await process.communicate() # Ожидание завершения процесса и захват вывода

        if process.returncode != 0: # Проверка завершения процесса (0 - успех)
            logger.error(f"FFmpeg error: {stderr.decode()}") # Лог ошибки и выброс исключения
            raise HTTPException(status_code=500, detail=f"FFmpeg error: {stderr.decode()}")

        logger.info(f"FFmpeg output: {stdout.decode()}") # Лог вывода команды
        end_time = time.time() # Показания времени после выполнения команды
        cpu_end = psutil.cpu_percent() # Загрузка CPU после
        mem_end = psutil.virtual_memory().percent # Загрузка памяти после

        logger.info(f"Execution time: {end_time - start_time} seconds") # Лог времени выполнения
        logger.info(f"CPU Usage: {cpu_end - cpu_start}%") # Лог разницы в загрузке CPU
        logger.info(f"Memory Usage: {mem_end - mem_start}%") # Лог разницы в загрузке памяти

        return stdout.decode()

    except Exception as e:
        logger.error(f"Error executing FFmpeg command: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error executing FFmpeg command: {str(e)}")


async def convert_to_h264(input_file: str, output_file: str):
    """Конвертация видео в формат H264"""
    logger.info(f"Converting {input_file} to H264 format.") # Лог начала конвертации
    command = [
        "ffmpeg", # Обращение к FFmpeg
        "-i", input_file, # Входной файл
        "-vcodec", "libx264", # Видеокодек H264
        "-acodec", "aac", # Аудиокодек AAC
        output_file # Путь для сохранения выходного файла
    ]
    await run_ffmpeg_command(command) # Запуск команды


async def extract_preview(input_file: str, output_file: str, start_time: float, duration: float):
    """Извлечение превью"""
    logger.info(f"Extracting preview from {input_file} starting at {start_time} seconds for {duration} seconds.")
    command = [
        "ffmpeg",
        "-i", input_file,
        "-ss", str(start_time), # Время начала превью
        "-t", str(duration), # Длительность
        "-c:v", "libx264",
        "-c:a", "aac",
        output_file
    ]
    await run_ffmpeg_command(command)


async def upload_to_s3(file_path: str, s3_key: str):
    """Загрузка видео на AWS S3"""
    try:
        logger.info(f"Uploading {file_path} to S3 as {s3_key}.")
        start_time = time.time()
        cpu_start = psutil.cpu_percent()
        mem_start = psutil.virtual_memory().percent

        # Стриминговая загрузка через на MinIO (локальный тестовый сервак AWS S3)
        async with s3_session.create_client(
                "s3", # Обязательный параметр, указывающий на то что работаем именно с S3
                region_name=AWS_DEFAULT_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                endpoint_url="http://localhost:9000"  # Указываем правильный endpoint для MinIO (запускаем тестово MinIO сервак потому пока так)
        ) as client:
            # Открытие файла с использованием aiofiles
            async with aiofiles.open(file_path, 'rb') as file_data:
                # Чтение содержимого файла в буфер
                file_buffer = io.BytesIO(await file_data.read())

                # Стриминговая загрузка файла на S3
                await client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=s3_key,
                    Body=file_buffer,
                )
            logger.info(f"Uploaded {file_path} to S3 as {s3_key}")

        end_time = time.time()
        cpu_end = psutil.cpu_percent()
        mem_end = psutil.virtual_memory().percent

        logger.info(f"Execution time: {end_time - start_time} seconds")
        logger.info(f"CPU Usage: {cpu_end - cpu_start}%")
        logger.info(f"Memory Usage: {mem_end - mem_start}%")

    except Exception as e:
        logger.error(f"Error uploading {file_path} to S3: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error uploading to S3: {str(e)}")


@app.post("/upload_video/") # Эндпоинт для загрузки видео
async def upload_video(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    try:
        # Проверка типа файла (должен быть видео)
        if not file.content_type.startswith('video/'):
            logger.warning(f"Uploaded file {file.filename} is not a video.")
            raise HTTPException(status_code=422, detail="Uploaded file is not a video.")

        # Сохранение видео на сервере
        file_path = f"/tmp/{file.filename}"
        async with aiofiles.open(file_path, "wb") as f:
            await f.write(await file.read())

        # Запуск фона для обработки видео
        background_tasks.add_task(process_video_task, file_path)
        return {"message": "Video is being processed in the background."}

    except Exception as e:
        logger.error(f"Error uploading video: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error uploading video: {str(e)}")

async def process_video_task(input_file: str):
    """Запуск задач"""
    try:
        output_file = f"{str(uuid4())}.mp4"
        preview_file = f"{str(uuid4())}_preview.mp4"

        # Запуск асинхронных задач через asyncio
        logger.info(f"Processing video: {input_file}")
        await convert_to_h264(input_file, output_file)
        await extract_preview(input_file, preview_file, 0, 4)

        s3_key_video = f"videos/{output_file}"
        s3_key_preview = f"previews/{preview_file}"

        await upload_to_s3(output_file, s3_key_video)
        await upload_to_s3(preview_file, s3_key_preview)

        # Очистка временных файлов после успешной загрузки
        os.remove(output_file)
        os.remove(preview_file)
        os.remove(input_file)

        logger.info(f"Temporary files {output_file}, {preview_file}, and {input_file} deleted.")

        # Сохранение информации в БД
        await save_video_to_db(s3_key_video, s3_key_preview)

    except Exception as e:
        logger.error(f"Error processing video: {str(e)}")


async def save_video_to_db(s3_key_video: str, s3_key_preview: str):
    """Сохранение ссылок на AWS S3 в БД"""
    try:
        async with SessionLocal() as session:
            new_video = videos_table.insert().values(
                video_url=f"https://{S3_BUCKET_NAME}.s3.{AWS_DEFAULT_REGION}.amazonaws.com/{s3_key_video}",
                preview_url=f"https://{S3_BUCKET_NAME}.s3.{AWS_DEFAULT_REGION}.amazonaws.com/{s3_key_preview}"
            )
            await session.execute(new_video)
            await session.commit()
            logger.info(f"Video data saved to DB: {s3_key_video} and {s3_key_preview}")

    except Exception as e:
        logger.error(f"Error saving video data to DB: {str(e)}")
        raise HTTPException(status_code=500, detail="Error saving video data to DB.")


