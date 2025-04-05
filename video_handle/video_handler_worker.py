""" Модуль реализации асинхронных функций для обработки видео и сохранения профиля в БД """

import time
import ffmpeg
from pathlib import Path
import aiofiles
import io
import urllib.parse
from uuid import uuid4
from prettyconf import config
import os
from dotenv import load_dotenv

from fastapi import HTTPException
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException
from aiobotocore.session import get_session
from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape
from shapely.geometry import Point, MultiPoint
from typing import Optional

from models import UserProfiles, Hashtag, ProfileHashtag, User
from schemas import FormData
from utils import get_file_size, generate_unique_link



from logging_config import get_logger

logger = get_logger()

# Конфиги для канала редис
CHANNEL = config("CHANNEL", default="video_tasks")
REDIS_HOST = "redis"

PREVIEW_DURATION = 5  # Длительность превью


load_dotenv()

# Конфиги для облака
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


async def convert_to_h264(input_path, output_path, logger):
    """Конвертация в H.264 с оптимальным балансом качества/размера"""
    start_time = time.time()
    filename = os.path.splitext(os.path.basename(input_path))[0]
    video_folder = os.path.join(output_path, filename)
    os.makedirs(video_folder, exist_ok=True)
    output_file = os.path.join(video_folder, f"{filename}.mp4")

    try:
        # Проверка исходного файла
        input_size = os.path.getsize(input_path) / (1024 * 1024)  # в MB
        logger.info(f"Начало конвертации: {input_path} (размер: {input_size:.2f} MB)")

        # Получаем метаданные для адаптивного сжатия
        probe = ffmpeg.probe(input_path)
        video_stream = next((s for s in probe['streams'] if s['codec_type'] == 'video'), None)
        width = int(video_stream['width']) if video_stream else 1280

        # Адаптивные параметры сжатия
        crf = 22 if width >= 1280 else 24  # Более агрессивное сжатие для HD+
        audio_bitrate = '128k' if width >= 1280 else '96k'

        # Оптимизированные параметры
        args = {
            'vcodec': 'libx264',
            'preset': 'medium',  # Оптимальный баланс скорости/качества
            'crf': crf,  # 22-24 - хороший баланс
            'pix_fmt': 'yuv420p',
            'movflags': '+faststart',
            'acodec': 'aac',
            'b:a': audio_bitrate,
            'x264-params': 'ref=5:deblock=-1,-1:me=hex:subme=7:merange=16',
            'threads': '0',  # Автовыбор количества потоков
            'loglevel': 'error'
        }

        # Дополнительная оптимизация для маленьких файлов
        if input_size < 10:  # Если исходник меньше 10MB
            args.update({
                'crf': 24,  # Немного больше сжатия
                'preset': 'fast'  # Ускоряем конвертацию
            })

        # Запуск конвертации
        (
            ffmpeg
            .input(input_path)
            .output(output_file, **args)
            .overwrite_output()
            .run()
        )

        # Проверка результата
        output_size = os.path.getsize(output_file) / (1024 * 1024)
        duration = time.time() - start_time

        compression_ratio = input_size / output_size
        logger.info(
            f"Конвертация завершена за {duration:.2f} сек | "
            f"Размер: {output_size:.2f} MB | "
            f"Коэффициент сжатия: {compression_ratio:.2f}x | "
            f"Параметры: CRF={crf}, preset={args['preset']}"
        )

        return {
            "video_path": output_file,
            "folder_path": video_folder,
            "original_size": input_size,
            "converted_size": output_size
        }

    except ffmpeg.Error as e:
        error_msg = e.stderr.decode('utf-8', errors='replace') if e.stderr else str(e)
        logger.error(f"Ошибка конвертации: {error_msg}")
        raise RuntimeError(f"Ошибка конвертации: {error_msg}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {str(e)}")
        raise


async def create_hls_playlist(conversion_result: dict, logger):
    """Генерация HLS с согласованными именами файлов"""
    input_video_path = conversion_result["converted_path"]
    video_folder = conversion_result["video_folder"]
    hls_dir = os.path.join(video_folder, "hls")
    os.makedirs(hls_dir, exist_ok=True)

    try:
        # Базовое имя (без расширения)
        base_name = os.path.splitext(os.path.basename(input_video_path))[0]

        # Именование всех элементов по шаблону
        master_playlist = f"{base_name}.m3u8"
        segment_pattern = f"{base_name}_%03d.ts"
        playlist_path = os.path.join(hls_dir, master_playlist)

        # Параметры генерации HLS
        (
            ffmpeg
            .input(input_video_path)
            .output(
                playlist_path,
                format='hls',
                hls_time=5,
                hls_list_size=0,
                hls_segment_filename=os.path.join(hls_dir, segment_pattern),
                vcodec='copy',
                acodec='copy',
                start_number=0,
                hls_flags='independent_segments',
                loglevel='warning'
            )
            .overwrite_output()
            .run()
        )

        # Проверка результатов
        if not os.path.exists(playlist_path):
            raise RuntimeError("HLS плейлист не был создан")

        # Проверка хотя бы одного сегмента
        first_segment = os.path.join(hls_dir, f"{base_name}_000.ts")
        if not os.path.exists(first_segment):
            raise RuntimeError("Не созданы TS сегменты")

        logger.info(f"HLS успешно сгенерирован: {playlist_path}")
        return {
            "hls_dir": hls_dir,
            "master_playlist": playlist_path,
            "segment_pattern": f"{base_name}_*.ts"
        }

    except ffmpeg.Error as e:
        error_msg = e.stderr.decode('utf-8', errors='replace') if e.stderr else str(e)
        logger.error(f"FFmpeg error: {error_msg}")
        raise HTTPException(status_code=500, detail=f"HLS generation failed: {error_msg}")
    except Exception as e:
        logger.error(f"HLS generation error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"HLS processing error: {str(e)}")


# Извлечение картинки из видео (для отображения постера на фронте)
async def extract_frame(video_path, posters_folder="user_video_posters", frame_time=2, logger=None):
    """
    Извлекает кадр из видео на указанной секунде, сохраняет его как изображение
    в папку `posters_folder` и возвращает путь к изображению.

    :param video_path: Путь к исходному видео.
    :param posters_folder: Папка для сохранения изображения (по умолчанию "user_video_posters").
    :param frame_time: Время в секундах, на котором нужно извлечь кадр (по умолчанию 2 секунды).
    :param logger: Логгер для записи сообщений.
    :return: Путь к сохранённому изображению в папке `posters_folder`.
    """
    start_time = time.time()

    # Создаем папку для постеров, если она не существует
    os.makedirs(posters_folder, exist_ok=True)

    # Генерируем уникальное имя файла с помощью uuid4
    unique_filename = f"{uuid4().hex}.jpg"  # Используем hex, чтобы убрать дефисы
    poster_path = os.path.join(posters_folder, unique_filename)  # Полный путь для сохранения

    try:
        if logger:
            logger.info(f"Извлечение кадра из видео: {video_path} -> {poster_path} (Время: {frame_time} сек.)")
        (
            ffmpeg
            .input(video_path, ss=frame_time)  # Указываем время, на котором нужно извлечь кадр
            .output(poster_path, vframes=1)  # Сохраняем только один кадр
            .overwrite_output()
            .run(capture_stdout=True, capture_stderr=True)
        )
        elapsed_time = time.time() - start_time
        if logger:
            logger.info(f"Извлечение кадра завершено: {poster_path} (Время: {elapsed_time:.2f} сек.)")
    except ffmpeg.Error as e:
        if logger:
            logger.error(f"Ошибка FFmpeg при извлечении кадра: {e.stderr.decode()}")
        raise RuntimeError("Не удалось извлечь кадр из видео")

    if logger:
        logger.info(f"Кадр успешно извлечён и сохранён: {poster_path}")

    # Возврат пути к изображению в папке `posters_folder`
    return poster_path


# Проверка соединения с AWS S3 перед загрузкой
async def check_s3_connection(logger):
    """ Проверка соединения с AWS S3 перед загрузкой. """
    try:
        async with get_session().create_client(
                "s3",
                region_name=AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        ) as s3_client:
            # Проверка подключения, попытка получить список объектов из бакета
            await s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME)
            logger.info("Соединение с AWS S3 установлено успешно.")  # Лог об успешном соединении
            return True  # Возвращаем True, если соединение успешно
    except Exception as e:
        logger.error(f"Не удалось установить соединение с AWS S3: {e}")
        raise RuntimeError(f"Не удалось подключиться к AWS S3: {e}")


async def upload_to_s3(processing_data: dict, logger) -> dict:
    """Рекурсивная загрузка всей папки (видео + HLS) в S3"""
    if processing_data.get("status") != "success":
        raise ValueError("Нет данных для загрузки")

    video_folder = processing_data["video_folder"]
    folder_name = os.path.basename(video_folder)

    try:
        await check_s3_connection(logger)
        base_s3_path = f"videos/{folder_name}"

        async with get_session().create_client(
                "s3",
                region_name=AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        ) as s3_client:
            # 1. Загружаем основное видео (не из папки hls)
            video_files = [f for f in os.listdir(video_folder)
                           if not f.startswith('.') and f != 'hls']

            if not video_files:
                raise FileNotFoundError("Основной видеофайл не найден")

            video_file = video_files[0]
            video_path = os.path.join(video_folder, video_file)

            await s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=f"{base_s3_path}/{video_file}",
                Body=open(video_path, 'rb')
            )

            # 2. Рекурсивная загрузка папки hls
            hls_dir = os.path.join(video_folder, "hls")
            if os.path.exists(hls_dir):
                for root, _, files in os.walk(hls_dir):
                    for file in files:
                        local_path = os.path.join(root, file)
                        relative_path = os.path.relpath(local_path, video_folder)
                        s3_key = f"{base_s3_path}/{relative_path.replace(os.sep, '/')}"

                        await s3_client.put_object(
                            Bucket=S3_BUCKET_NAME,
                            Key=s3_key,
                            Body=open(local_path, 'rb')
                        )

            # 3. Формируем URL (используем реальное имя файла из папки hls)
            hls_files = os.listdir(hls_dir)
            master_playlist = next((f for f in hls_files if f.endswith('.m3u8')), None)

            if not master_playlist:
                raise FileNotFoundError("HLS master playlist not found")

            return {
                "video_url": f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{base_s3_path}/{video_file}",
                "preview_url": f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{base_s3_path}/hls/{master_playlist}"
            }

    except Exception as e:
        logger.error(f"Ошибка загрузки: {str(e)}", exc_info=True)
        raise RuntimeError(f"Ошибка загрузки в S3: {str(e)}")


# Логика удаления старых файлов с облака
async def delete_video_folder(video_url: str, logger) -> bool:
    """Удаляет все файлы по префиксу с детальным логгированием"""
    try:
        parsed = urllib.parse.urlparse(video_url)
        prefix = '/'.join(parsed.path.lstrip('/').split('/')[:-1]) + '/'

        logger.info(f"Начинаем удаление по префиксу: {prefix}")

        async with get_session().create_client(
                "s3",
                region_name=AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        ) as s3:
            # Логируем запрос
            logger.info(f"Запрашиваем объекты для префикса: {prefix}")

            objects = await s3.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix=prefix
            )

            if not objects.get('Contents'):
                logger.warning(f"Не найдено объектов для удаления по префиксу: {prefix}")
                return False

            # Детальный лог объектов
            file_list = "\n".join([f" - {obj['Key']} ({obj['Size']} bytes)"
                                   for obj in objects['Contents']])
            logger.info(f"Найдены объекты для удаления:\n{file_list}")

            # Удаление с подтверждением
            response = await s3.delete_objects(
                Bucket=S3_BUCKET_NAME,
                Delete={
                    'Objects': [{'Key': obj['Key']} for obj in objects['Contents']],
                    'Quiet': False  # Получаем подробный ответ
                }
            )

            # Логируем результат
            if 'Deleted' in response:
                deleted_files = "\n".join([f" - {item['Key']}" for item in response['Deleted']])
                logger.info(f"Успешно удалены:\n{deleted_files}")

            if 'Errors' in response:
                errors = "\n".join([f" - {item['Key']}: {item['Message']}"
                                    for item in response['Errors']])
                logger.error(f"Ошибки при удалении:\n{errors}")
                return False

            return True

    except Exception as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА: {str(e)}", exc_info=True)
        return False


# Логика удаления старой аватарки юзера и постера к видео
async def delete_old_media_files(
        old_logo_url: Optional[str],
        old_poster_url: Optional[str],
        logger
):
    """
    Удаление старых медиафайлов (логотипа и постера) из локальных папок

    :param old_logo_url: Локальный путь к старому логотипу
    :param old_poster_url: Локальный путь к старому постеру
    :param logger: Логгер для записи событий
    """
    try:
        if old_logo_url:
            logo_path = Path(old_logo_url)
            if logo_path.exists():
                os.unlink(logo_path)
                logger.info(f"Локальный файл логотипа удален: {logo_path}")
            else:
                logger.warning(f"Файл логотипа не найден: {logo_path}")

        if old_poster_url:
            poster_path = Path(old_poster_url)
            if poster_path.exists():
                os.unlink(poster_path)
                logger.info(f"Локальный файл постера удален: {poster_path}")
            else:
                logger.warning(f"Файл постера не найден: {poster_path}")

    except Exception as e:
        logger.error(f"Ошибка при удалении локальных файлов: {e}")
        # Не прерываем выполнение, если не удалось удалить файлы


async def save_profile_to_db(session: AsyncSession, form_data: FormData, video_url: str, preview_url: str, poster_path: str, user_logo_url: str, wallet_number: str, logger):
    """
    Сохранение или обновление данных пользователя, логотипа и хэштегов в БД.
    """
    try:
        async with session.begin():
            # 1. Получаем пользователя по кошельку
            stmt = select(User).where(User.wallet_number == wallet_number)
            result = await session.execute(stmt)
            user = result.scalars().first()

            if not user:
                raise HTTPException(status_code=400, detail="Пользователь с данным кошельком не найден.")

            # 2. Удаление старых файлов из облака (если профиль уже существует)
            existing_profile_stmt = select(UserProfiles).where(UserProfiles.user_id == user.id)
            existing_profile_result = await session.execute(existing_profile_stmt)
            existing_profile = existing_profile_result.scalars().first()

            if existing_profile and existing_profile.video_url:
                try:
                    await delete_video_folder(existing_profile.video_url, logger)
                    logger.info(f"Старые файлы удалены из облака для {wallet_number}")
                except Exception as e:
                    logger.error(f"Ошибка удаления старых файлов: {e}")
                    # Не прерываем выполнение, если не удалось удалить файлы

            # 3. Получаем координаты из form_data
            coordinates = form_data.get("coordinates")

            # Преобразуем координаты в строку WKT, если они есть
            multi_point_wkt = None
            if coordinates:
                points = [Point(coord[0], coord[1]) for coord in coordinates]
                multi_point = MultiPoint(points)
                multi_point_wkt = str(multi_point)

            # 4. Проверка флага is_profile_created
            if not user.is_profile_created:
                # Генерируем уникальную ссылку только при создании нового профиля
                unique_link = await generate_unique_link()

                new_profile = UserProfiles(
                    name=form_data["name"],
                    website_or_social=form_data["website_or_social"],
                    activity_and_hobbies=form_data["activity_hobbies"],
                    video_url=video_url,
                    preview_url=preview_url,
                    user_logo_url=user_logo_url,
                    poster_url=poster_path,
                    adress=form_data["adress"],
                    city=form_data["city"],
                    coordinates=multi_point_wkt,
                    is_incognito=False,
                    is_moderated=False,
                    is_admin=False,
                    is_in_mlm=form_data["is_in_mlm"],
                    user_id=user.id,
                    language=form_data["language"],
                    user_link=unique_link  # Добавляем новую ссылку
                )

                user.is_profile_created = True
                session.add(new_profile)
                await session.flush()
                profile = new_profile

                logger.info(f"Создан новый профиль с уникальной ссылкой: {unique_link}")
            else:
                # Если профиль уже существует - получаем его текущие данные
                stmt = select(UserProfiles).where(UserProfiles.user_id == user.id)
                result = await session.execute(stmt)
                profile = result.scalars().first()

                # Проверяем, изменились ли медиафайлы
                old_logo_url = profile.user_logo_url
                old_poster_url = profile.poster_url

                if (old_logo_url and old_logo_url != user_logo_url) or \
                        (old_poster_url and old_poster_url != poster_path):
                    try:
                        await delete_old_media_files(
                            old_logo_url if old_logo_url != user_logo_url else None,
                            old_poster_url if old_poster_url != poster_path else None,
                            logger
                        )
                    except Exception as e:
                        logger.error(f"Ошибка при удалении старых медиафайлов: {e}")
                        # Продолжаем выполнение даже если не удалось удалить файлы

                # Сохраняем ВСЕ неизменяемые объекты при обновлении поля
                current_unique_link = profile.user_link
                current_is_admin = profile.is_admin
                current_is_moderated = profile.is_moderated

                # Обновляем только разрешенные для изменения поля
                profile.name = form_data["name"]
                profile.website_or_social = form_data["website_or_social"] if form_data["website_or_social"] is not None else None
                profile.activity_and_hobbies = form_data["activity_hobbies"] if form_data["activity_hobbies"] is not None else None
                profile.video_url = video_url
                profile.preview_url = preview_url
                profile.user_logo_url = user_logo_url
                profile.poster_url = poster_path
                profile.adress = form_data["adress"] if form_data["adress"] is not None else None
                profile.city = form_data["city"] if form_data["city"] is not None else None
                profile.coordinates = multi_point_wkt if coordinates is not None else None
                profile.is_incognito = False
                profile.is_in_mlm = form_data["is_in_mlm"] if form_data["is_in_mlm"] is not None else None
                profile.language = form_data["language"] if form_data["language"] is not None else None

                # Возвращаем неизменяемые поля
                profile.user_link = current_unique_link  # Сохраняем старую ссылку
                profile.is_admin = current_is_admin
                profile.is_moderated = current_is_moderated

                session.add(profile)
                logger.info(f"Профиль обновлен, уникальная ссылка сохранена: {current_unique_link}")

            # 5. Полная синхронизация хэштегов
            if form_data.get("hashtags"):
                # Получаем текущие хэштеги профиля
                current_hashtags_stmt = select(Hashtag).join(ProfileHashtag).where(
                    ProfileHashtag.profile_id == profile.id)
                current_hashtags_result = await session.execute(current_hashtags_stmt)
                current_hashtags = {tag.tag: tag for tag in current_hashtags_result.scalars().all()}

                # Нормализуем новые хэштеги
                new_tags = {tag.strip().lower().lstrip("#") for tag in form_data["hashtags"] if tag.strip()}

                # Удаляем отсутствующие хэштеги
                for tag_name, tag_obj in current_hashtags.items():
                    if tag_name not in new_tags:
                        delete_stmt = delete(ProfileHashtag).where(
                            ProfileHashtag.profile_id == profile.id,
                            ProfileHashtag.hashtag_id == tag_obj.id
                        )
                        await session.execute(delete_stmt)
                        logger.debug(f"Удалена связь с хэштегом: {tag_name}")

                # Добавляем новые хэштеги
                existing_tags_stmt = select(Hashtag).where(Hashtag.tag.in_(new_tags))
                existing_tags_result = await session.execute(existing_tags_stmt)
                existing_tags = {tag.tag: tag for tag in existing_tags_result.scalars().all()}

                for tag_name in new_tags:
                    if tag_name not in existing_tags:
                        new_hashtag = Hashtag(tag=tag_name)
                        session.add(new_hashtag)
                        await session.flush()
                        existing_tags[tag_name] = new_hashtag

                    # Проверяем и создаем связь при необходимости
                    link_stmt = select(ProfileHashtag).where(
                        ProfileHashtag.profile_id == profile.id,
                        ProfileHashtag.hashtag_id == existing_tags[tag_name].id
                    )
                    link_result = await session.execute(link_stmt)
                    if not link_result.scalars().first():
                        session.add(ProfileHashtag(
                            profile_id=profile.id,
                            hashtag_id=existing_tags[tag_name].id
                        ))

                logger.info(f"Обновлено хэштегов: {len(new_tags)}")

        await session.commit()
        logger.info(f"Данные успешно сохранены для кошелька {wallet_number}")

    except SQLAlchemyError as db_error:
        logger.error(f"Ошибка базы данных: {db_error}")
        await session.rollback()
        raise HTTPException(status_code=500, detail="Ошибка базы данных при сохранении видео")

    except Exception as e:
        logger.exception(f"Непредвиденная ошибка: {e}")
        await session.rollback()
        raise HTTPException(status_code=500, detail="Ошибка сохранения данных в базе")




