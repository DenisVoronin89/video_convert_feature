""" Модуль реализации асинхронных функций для обработки видео """

import time
import ffmpeg
import aiofiles
import io
from prettyconf import config

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException
from models import Video, Hashtag, VideoHashtag

from authentication_module import get_random_wallet

from logging_config import get_logger

logger = get_logger()


CHANNEL = config("CHANNEL", default="video_tasks")
REDIS_HOST = config("REDIS_HOST", default="redis")
PREVIEW_DURATION = 5  # Длительность превью


async def convert_to_vp9(input_path, output_path, logger):
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
        elapsed_time = time.time() - start_time
        output_size = get_file_size(output_path)
        logger.info(
            f"Конвертация в VP9 завершена: {output_path} (Размер: {output_size:.2f} MB, Время: {elapsed_time:.2f} сек.)")
    except ffmpeg.Error as e:
        logger.error(f"Ошибка FFmpeg при конвертации: {e.stderr.decode()}")
        raise RuntimeError("Не удалось конвертировать видео в VP9")
    logger.info(f"Видео успешно обработано: {output_path} (Размер: {output_size:.2f} MB)")

async def extract_preview(input_path, output_path, duration=PREVIEW_DURATION, logger=None):
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


async def save_profile_to_db(session: AsyncSession, form_data: FormData, video_url: str, preview_url: str, user_logo_url: str, logger):
    """
    Сохранение или обновление данных видео, пользователя, логотипа и хэштегов в БД.

    :param session: Сессия работы с БД.
    :param form_data: Данные формы, валидируемые Pydantic.
    :param video_url: URL загруженного видео.
    :param preview_url: URL превью видео.
    :param user_logo_url: URL логотипа пользователя.
    :param logger: Объект логгера.
    """
    # Генерируем случайный кошелек для пользователя
    wallet_number = get_random_wallet()

    try:
        # Проверяем, существует ли пользователь с данным кошельком
        stmt = select(Video).where(Video.wallet_number == wallet_number)
        result = await session.execute(stmt)
        existing_video = result.scalars().first()

        if existing_video:
            # Если видео существует, обновляем его данные
            existing_video.name = form_data.name
            existing_video.activity_and_hobbies = form_data.activity_hobbies
            existing_video.video_url = video_url
            existing_video.preview_url = preview_url
            existing_video.user_logo_url = user_logo_url
            existing_video.is_moderated = False  # Сбрасываем флаг модерации при обновлении данных
            logger.info(f"Обновлены данные для кошелька {wallet_number}")
        else:
            # Если юзера не существует, создаем новую запись
            new_user = Video(
                name=form_data.name,
                activity_and_hobbies=form_data.activity_hobbies,
                wallet_number=wallet_number,  # Используем сгенерированный кошелек
                video_url=video_url,
                preview_url=preview_url,
                user_logo_url=user_logo_url,
                is_moderated=False
            )
            session.add(new_user)
            logger.info(f"Создана новая запись для кошелька {wallet_number}")

        # Работа с хэштегами
        hashtags_list = [tag.strip().lower() for tag in form_data.hashtags.split('#') if tag.strip()]
        if hashtags_list:
            # Находим уже существующие хэштеги
            existing_hashtags_stmt = select(Hashtag).where(Hashtag.tag.in_(hashtags_list))
            existing_hashtags_result = await session.execute(existing_hashtags_stmt)
            existing_hashtags = {tag.tag: tag for tag in existing_hashtags_result.scalars().all()}

            for hashtag in hashtags_list:
                if hashtag not in existing_hashtags:
                    # Если хэштег отсутствует, добавляем его
                    new_hashtag = Hashtag(tag=hashtag)
                    session.add(new_hashtag)
                    if existing_video:
                        # Добавляем связь в таблицу VideoHashtag
                        video_hashtag = VideoHashtag(video_id=existing_video.id, hashtag_id=new_hashtag.id)
                        session.add(video_hashtag)
                else:
                    # Привязываем существующий хэштег к видео через таблицу VideoHashtag
                    if existing_video:
                        video_hashtag = VideoHashtag(video_id=existing_video.id, hashtag_id=existing_hashtags[hashtag].id)
                        session.add(video_hashtag)

        # Сохраняем изменения в БД
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

