""" Модуль реализации асинхронных функций для обработки видео """

import time
import ffmpeg
import aiofiles
import io
from prettyconf import config
import uuid

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException
from models import UserProfiles, Hashtag, VideoHashtag

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


async def upload_to_s3(video_path, preview_path, logger):
    """
    Загрузка видео и превью в S3.

    :param video_path: Локальный путь к видео.
    :param preview_path: Локальный путь к превью.
    :param logger: Логгер для записи логов.
    :return: Ссылки на загруженные файлы.
    """
    try:
        # Генерация ключей для видео и превью
        video_key = f"videos/{uuid.uuid4()}_{video_path.split('/')[-1]}"
        preview_key = f"previews/{uuid.uuid4()}_{preview_path.split('/')[-1]}"

        async with get_session().create_client(
                "s3",
                region_name=AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                endpoint_url="http://localhost:9000",
        ) as s3_client:
            # Загрузка видео
            logger.info(f"Загрузка видео в S3: {video_path} -> {video_key}")
            async with aiofiles.open(video_path, "rb") as video_file:
                video_stream = io.BytesIO(await video_file.read())
            response_video = await s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=video_key,
                Body=video_stream,
            )
            if response_video["ResponseMetadata"]["HTTPStatusCode"] != 200:
                raise RuntimeError(f"Ошибка при загрузке видео: некорректный код ответа")

            logger.info(f"Видео успешно загружено в S3: {video_key}")

            # Загрузка превью
            logger.info(f"Загрузка превью в S3: {preview_path} -> {preview_key}")
            async with aiofiles.open(preview_path, "rb") as preview_file:
                preview_stream = io.BytesIO(await preview_file.read())
            response_preview = await s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=preview_key,
                Body=preview_stream,
            )
            if response_preview["ResponseMetadata"]["HTTPStatusCode"] != 200:
                raise RuntimeError(f"Ошибка при загрузке превью: некорректный код ответа")

            logger.info(f"Превью успешно загружено в S3: {preview_key}")

        # Возвращаем ссылки на загруженные файлы
        video_url = f"http://localhost:9000/{S3_BUCKET_NAME}/{video_key}"
        preview_url = f"http://localhost:9000/{S3_BUCKET_NAME}/{preview_key}"
        logger.info(f"Загруженные файлы: Видео - {video_url}, Превью - {preview_url}")

        return video_url, preview_url

    except Exception as e:
        logger.error(f"Ошибка при загрузке файлов в S3: {e}")
        raise RuntimeError(f"Не удалось загрузить файлы в S3: {e}")


async def save_profile_to_db(session: AsyncSession, form_data: FormData, video_url: str, preview_url: str, user_logo_url: str, wallet_number, logger):
    """
    Сохранение или обновление данных пользователя, логотипа и хэштегов в БД.

    :param session: Сессия работы с БД.
    :param form_data: Данные формы, валидируемые Pydantic.
    :param video_url: URL загруженного видео.
    :param preview_url: URL превью видео.
    :param user_logo_url: URL логотипа пользователя.
    :param logger: Объект логгера.
    :param wallet_number: номер кошелька юзера
    """
    try:
        # Проверяем, существует ли пользователь с данным кошельком
        stmt = select(UserProfiles).where(UserProfiles.wallet_number == wallet_number)
        result = await session.execute(stmt)
        existing_user = result.scalars().first()

        if existing_user:
            # Если пользователь существует, обновляем его данные
            existing_user.name = form_data.name
            existing_user.activity_and_hobbies = form_data.activity_hobbies
            existing_user.video_url = video_url
            existing_user.preview_url = preview_url
            existing_user.user_logo_url = user_logo_url
            existing_user.is_moderated = False  # Сбрасываем флаг модерации при обновлении данных
            logger.info(f"Обновлены данные для кошелька {wallet_number}")
        else:
            # Если пользователя не существует, создаем новую запись
            new_user = UserProfiles(
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
                    # Добавляем связь в таблицу VideoHashtag
                    if existing_user:
                        video_hashtag = VideoHashtag(video_url=video_url, hashtag_id=new_hashtag.id)  # Теперь связываем с video_url
                        session.add(video_hashtag)
                else:
                    # Привязываем существующий хэштег к видео через таблицу VideoHashtag
                    if existing_user:
                        video_hashtag = VideoHashtag(video_url=video_url, hashtag_id=existing_hashtags[hashtag].id)  # Привязка через video_url
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



