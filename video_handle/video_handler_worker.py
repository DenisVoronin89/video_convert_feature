""" Модуль реализации асинхронных функций для обработки видео и сохранения профиля в БД """

import time
import ffmpeg
import aiofiles
import io
from prettyconf import config
import os

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException
from aiobotocore.session import get_session
from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape
from shapely.geometry import Point, MultiPoint

from models import UserProfiles, Hashtag, VideoHashtag, User, Favorite
from schemas import FormData
from utils import get_file_size



from logging_config import get_logger

logger = get_logger()

# Конфиги для разработки и тестирования облачного хранилища(НЕ ДЛЯ ПРОДАКШЕНА)
CHANNEL = config("CHANNEL", default="video_tasks")
REDIS_HOST = "redis"
PREVIEW_DURATION = 5  # Длительность превью
S3_BUCKET_NAME = "video-service"
AWS_REGION = "us-east-1"
AWS_ACCESS_KEY_ID = "StdyLLebBVvhXA47msMm"
AWS_SECRET_ACCESS_KEY = "xOGYPd6V8FH7XfzFur4PcPpwLdhynTMWTgz40FH8"


async def convert_to_vp9(input_path, output_path, logger):
    """Конвертация видео с сохранением качества и минимизацией увеличения размера, юзаем кодек гугла VP9"""
    start_time = time.time()

    # Извлечение имени файла из пути
    input_filename = os.path.basename(input_path)

    # Проверка расширения выходного файла
    if not output_path.endswith('.webm'):
        output_path = os.path.join(os.path.dirname(output_path), f"{input_filename}_converted.webm")

    try:
        logger.info(f"Запуск конвертации видео в VP9: {input_path} -> {output_path}")
        (
            ffmpeg
            .input(input_path)
            .output(output_path, vcodec='libvpx-vp9', acodec='libopus', crf=30, audio_bitrate='96k')
            .overwrite_output()
            .run(capture_stdout=True, capture_stderr=True)
        )
        elapsed_time = time.time() - start_time
        output_size = get_file_size(output_path)
        logger.info(
            f"Конвертация в VP9 завершена: {output_path} (Размер: {output_size:.2f} MB, Время: {elapsed_time:.2f} сек.)")
    except ffmpeg.Error as e:
        logger.error(f"Ошибка FFmpeg при конвертации: {e.stderr.decode()}")
        raise RuntimeError("Не удалось конвертировать видео в VP9")

    logger.info(f"Видео успешно обработано: {output_path} (Размер: {output_size:.2f} MB)")

    # Лог пути к конвертированному файлу (для проверки перед сохранением в БД)
    logger.info(f"Путь к конвертированному видео: {output_path}")

    # Возврат пути к конвертированному файлу
    return output_path


async def extract_preview(input_path, preview_path, duration=PREVIEW_DURATION, logger=None):
    """Извлечение превью"""
    start_time = time.time()

    # Извлечение имени файла из пути
    input_filename = os.path.basename(input_path)

    # Проверка расширения выходного файла
    if not preview_path.endswith('.webm'):
        preview_path = os.path.join(os.path.dirname(preview_path), f"{input_filename}_preview.webm")

    try:
        logger.info(f"Извлечение превью из видео: {input_path} -> {preview_path} (Длительность: {duration} сек.)")
        (
            ffmpeg
            .input(input_path, ss=0, t=duration)
            .output(preview_path, vcodec='libvpx-vp9', acodec='libopus')
            .overwrite_output()
            .run(capture_stdout=True, capture_stderr=True)
        )
        elapsed_time = time.time() - start_time
        preview_size = get_file_size(preview_path)
        logger.info(
            f"Извлечение превью завершено: {preview_path} (Размер: {preview_size:.2f} MB, Время: {elapsed_time:.2f} сек.)")
    except ffmpeg.Error as e:
        logger.error(f"Ошибка FFmpeg при извлечении превью: {e.stderr.decode()}")
        raise RuntimeError("Не удалось извлечь превью")

    logger.info(f"Превью успешно извлечено: {preview_path} (Размер: {preview_size:.2f} MB)")

    # Лог пути к извлеченному превью (для проверки перед сохранением в БД)
    logger.info(f"Путь к извлеченному превью: {preview_path}")

    # Возврат пути к превью (улетает в ф-ию загрузки в облако)
    return preview_path


# TODO в проде нужно не забыть сюда поставить нужные парметры такие как урлы и прочее!!!
async def check_s3_connection(logger):
    """ Проверка соединения с MinIO перед загрузкой. """
    try:
        async with get_session().create_client(
                "s3",
                region_name=AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                endpoint_url="http://minio:9000",  # TODO minio из контейнера, в проде сменить!!!
        ) as s3_client:
            # Проверка подключения, попытка получить список объектов из бакета
            await s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME)
            logger.info("Соединение с MinIO установлено успешно.")
    except Exception as e:
        logger.error(f"Не удалось установить соединение с MinIO: {e}")
        raise RuntimeError(f"Не удалось подключиться к MinIO: {e}")


async def upload_to_s3(converted_video, preview_video, logger):
    """
    Загрузка видео и превью в S3 с предварительной проверкой соединения.

    :param converted_video: Локальный путь к конвертированному видео.
    :param preview_video: Локальный путь к превью.
    :param logger: Логгер для записи логов.
    :return: Ссылки на загруженные файлы.
    """
    try:
        # Проверка соединения с MinIO
        await check_s3_connection(logger)

        # Просто используются имена файлов без ключей, так как они получают уникальные имена и ключи для S3 это лишее (модуль вьюх)
        video_filename = os.path.basename(converted_video)
        preview_filename = os.path.basename(preview_video)

        # Проверка наличия папки в бакете и создание ее при необходимости
        async with get_session().create_client(
                "s3",
                region_name=AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                endpoint_url="http://minio:9000",  # TODO minio из контейнера, в проде сменить!!!
        ) as s3_client:
            # Загрузка видео
            logger.info(f"Загрузка видео в S3: {converted_video} -> {video_filename}")
            async with aiofiles.open(converted_video, "rb") as video_file:
                video_stream = io.BytesIO(await video_file.read())
            response_video = await s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=video_filename,  # Имя файла как ключ (см. выше)
                Body=video_stream,
            )
            if response_video["ResponseMetadata"]["HTTPStatusCode"] != 200:
                raise RuntimeError(f"Ошибка при загрузке видео: некорректный код ответа")

            logger.info(f"Видео успешно загружено в S3: {video_filename}")

            # Загрузка превью
            logger.info(f"Загрузка превью в S3: {preview_video} -> {preview_filename}")
            async with aiofiles.open(preview_video, "rb") as preview_file:
                preview_stream = io.BytesIO(await preview_file.read())
            response_preview = await s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=preview_filename,  # Имя файла как ключ (см. выше)
                Body=preview_stream,
            )
            if response_preview["ResponseMetadata"]["HTTPStatusCode"] != 200:
                raise RuntimeError(f"Ошибка при загрузке превью: некорректный код ответа")

            logger.info(f"Превью успешно загружено в S3: {preview_filename}")

        # Возврат ссылок на загруженные файлы
        video_url = f"http://minio:9000/{S3_BUCKET_NAME}/{video_filename}"  # TODO ИЗМЕНИТЬ В ПРОДЕ НА КОРРЕКТНОЕ!!!!
        preview_url = f"http://minio:9000/{S3_BUCKET_NAME}/{preview_filename}"  # TODO ИЗМЕНИТЬ В ПРОДЕ НА КОРРЕКТНОЕ!!!!
        logger.info(f"Загруженные файлы: Видео - {video_url}, Превью - {preview_url}")

        return video_url, preview_url

    except Exception as e:
        logger.error(f"Ошибка при загрузке файлов в S3: {e}")
        raise RuntimeError(f"Не удалось загрузить файлы в S3: {e}")


async def save_profile_to_db(session: AsyncSession, form_data: FormData, video_url: str, preview_url: str, user_logo_url: str, wallet_number: str,logger):
    """
    Сохранение или обновление данных пользователя, логотипа и хэштегов в БД.

    :param session: Сессия работы с БД.
    :param form_data: Данные формы (валидация Pydantic см. модкль схем).
    :param video_url: URL загруженного видео.
    :param preview_url: URL превью видео.
    :param user_logo_url: URL логотипа пользователя.
    :param logger: Объект логгера.
    :param wallet_number: номер кошелька юзера (прилетает уже хэшированный)
    """
    try:
        async with session.begin():
            # 1. Получаем пользователя по кошельку
            stmt = select(User).where(User.wallet_number == wallet_number)
            result = await session.execute(stmt)
            user = result.scalars().first()

            if not user:
                raise HTTPException(status_code=400, detail="Пользователь с данным кошельком не найден.")

            # Преобразуем каждую пару координат в объект Point, а затем создаём MultiPoint
            points = [Point(coord["lng"], coord["lat"]) for coord in coordinates]
            multi_point = MultiPoint(points)

            # 2. Проверка флага is_profile_created
            if not user.is_profile_created:
                # Если флаг False, создаем новый профиль
                new_profile = UserProfiles(
                    name=form_data["name"],
                    activity_and_hobbies=form_data["activity_hobbies"],
                    video_url=video_url,
                    preview_url=preview_url,
                    user_logo_url=user_logo_url,
                    adress=form_data["adress"],
                    city=form_data["city"],
                    coordinates=multi_point,
                    is_incognito=False,
                    is_moderated=False,
                    is_admin=False,
                    is_in_mlm=form_data["is_in_mlm"],
                    user_id=user.id
                )

                # Обновляем флаг is_profile_created в таблице User на True
                user.is_profile_created = True
                else:
                # Если флаг True, просто обновляем профиль
                stmt = select(UserProfiles).where(UserProfiles.user_id == user.id)
                result = await session.execute(stmt)
                profile = result.scalars().first()

                # Сохраняем старое значение is_admin
                current_is_admin = profile.is_admin

                # Обновляем данные профиля
                profile.name = form_data["name"]
                profile.activity_and_hobbies = form_data["activity_hobbies"]
                profile.video_url = video_url
                profile.preview_url = preview_url
                profile.user_logo_url = user_logo_url
                profile.adress = form_data["adress"]
                profile.city = form_data["city"]
                profile.coordinates = multi_point
                profile.is_incognito = False
                profile.is_moderated = False
                profile.is_in_mlm = form_data["is_in_mlm"]

                # Возвращаем старое значение is_admin
                profile.is_admin = current_is_admin

                logger.info(f"Обновлены данные профиля для кошелька {wallet_number}")

            # Работа с хэштегами
            hashtags_list = [tag.strip().lower() for tag in form_data["hashtags"].split('#') if tag.strip()]
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
                        # Добавление связи в таблицу VideoHashtag
                        if existing_user:
                            video_hashtag = VideoHashtag(video_url=video_url, hashtag_id=new_hashtag.id)  # Связь с video_url
                            session.add(video_hashtag)
                    else:
                        # Привязка существующего хэштега к видео через таблицу VideoHashtag
                        if existing_user:
                            video_hashtag = VideoHashtag(video_url=video_url, hashtag_id=existing_hashtags[hashtag].id)  # Привязка через video_url
                            session.add(video_hashtag)

        # Сохранение изменений в БД
        await session.commit()
        logger.info(f"Видео, пользователь, логотип и хэштеги успешно сохранены для кошелька {wallet_number}")

    except SQLAlchemyError as db_error:
        # Обработка ошибок SQLAlchemy
        logger.error(f"Ошибка базы данных: {db_error}")
        await session.rollback()
        raise HTTPException(status_code=500, detail="Ошибка базы данных при сохранении видео")

    except Exception as e:
        # Обработка общих ошибок
        logger.exception(f"Непредвиденная ошибка: {e}")
        await session.rollback()
        raise HTTPException(status_code=500, detail="Ошибка сохранения данных в базе")
