"""
Модуль, который подписывается на канал Redis, получает сообщения и
вызывает в работу функции с модуля video_handler_worker.py
"""

import json
import asyncio
import os
from datetime import datetime, timedelta
from sqlalchemy.future import select
from database import get_db_session_for_worker
from redis.asyncio import Redis
from video_handle.video_handler_worker import (
    convert_to_h264,
    upload_to_s3,
    save_profile_to_db,
    check_s3_connection,
    create_hls_playlist,
    extract_frame
)
from logging_config import get_logger
from models import User

logger = get_logger()

CHANNEL = "video_tasks"  # Канал для видео задач
REDIS_HOST = "redis"


# Функция для установки статуса обработки и сохранения данных о пользователе
async def set_profile_status(wallet_number: str, status: str, logger):
    """Устанавливает статус профиля, открывая сессию только на время операции"""
    try:
        async with get_db_session_for_worker() as session:
            stmt = select(User).where(User.wallet_number == wallet_number)
            result = await session.execute(stmt)
            user = result.scalars().first()

            if user:
                user.profile_creation_status = status
                session.add(user)
                await session.commit()
                logger.info(f"Статус профиля для {wallet_number} установлен в '{status}'")
    except Exception as e:
        logger.error(f"Ошибка при установке статуса профиля: {str(e)}")
        raise


# Функция обработки задач на микросервисе
async def handle_task(task_data):
    """Обработка всех задач последовательно с генерацией HLS"""
    logger.info(f"Получена задача для обработки: {task_data}")
    wallet_hash = task_data["wallet_number"]

    try:
        # Устанавливаем статус "await" перед началом обработки
        await set_profile_status(wallet_hash, "await", logger)

        # Извлечение входных данных
        input_video = task_data["input_path"]
        output_path = task_data["output_path"]["path"]
        form_data = task_data["form_data"]
        user_logo = task_data["user_logo_url"]

        # 1. Конвертация видео
        logger.info(f"Конвертация видео: {input_video}")
        conversion_result = await convert_to_h264(
            input_path=input_video,
            output_path=output_path,
            logger=logger
        )
        video_file_path = conversion_result["video_path"]
        video_folder = conversion_result["folder_path"]
        logger.info(f"Видео сконвертировано: {video_file_path}")

        # 2. Генерация HLS
        logger.info("Генерация HLS плейлиста")
        hls_result = await create_hls_playlist(
            conversion_result={"converted_path": video_file_path, "video_folder": video_folder},
            logger=logger
        )
        logger.info(f"HLS создан: {hls_result['master_playlist']}")

        # 3. Извлечение постера
        logger.info("Извлечение постера из видео")
        poster_path = await extract_frame(
            video_path=video_file_path,
            posters_folder="user_video_posters",
            frame_time=2,
            logger=logger
        )
        logger.info(f"Постер сохранен: {poster_path}")

        # 4. Загрузка в облачное хранилище
        logger.info("Загрузка файлов в S3")
        upload_result = await upload_to_s3(
            processing_data={
                "status": "success",
                "video_folder": video_folder,
                "filename": os.path.splitext(os.path.basename(video_file_path))[0]
            },
            logger=logger
        )
        logger.info(f"Файлы загружены: {upload_result['video_url']}")

        # 5. Сохранение данных в БД
        logger.info("Сохранение профиля в базе данных")
        async with get_db_session_for_worker() as db_session:
            await save_profile_to_db(
                session=db_session,
                form_data=form_data,
                video_url=upload_result["video_url"],
                preview_url=upload_result["preview_url"],
                poster_path=poster_path,
                user_logo_url=user_logo,
                wallet_number=wallet_hash,
                logger=logger
            )
        logger.info("Профиль успешно сохранен")

    except Exception as e:
        logger.error(f"ОШИБКА: {e}")
        # Устанавливаем статус "false" при ошибке
        try:
            await set_profile_status(wallet_hash, "false", logger)
        except Exception as status_error:
            logger.error(f"Не удалось установить статус ошибки: {status_error}")
        raise RuntimeError(f"Ошибка обработки задачи: {str(e)}")

    finally:
        task_data.clear()
        logger.info("Задача завершена, данные очищены")


# Запуск подписчика в работу, проверка соединения с облаком
async def main():
    """Основная функция для подписки и обработки задач с ретрай-логикой"""
    logger.info("Запуск подписки на канал Redis для получения задач")
    retries = 5  # Количество ретраев для Redis и S3
    retry_delay = 5  # Задержка между ретраями (в секундах)

    redis = Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

    last_error_time = None  # Время последней ошибки
    error_cooldown = timedelta(seconds=10)  # Задержка между логами об ошибках

    # Проверка доступности Redis
    try:
        await redis.ping()
        logger.info("Redis доступен.")
    except Exception as e:
        logger.error(f"Ошибка подключения к Redis: {e}")
        return  # Завершение выполнения, если Redis недоступен

    # Проверка соединения с S3 с ретраями
    logger.info("Проверка соединения с AWS S3...")  # Лог перед проверкой S3
    s3_connected = False
    for attempt in range(retries):
        try:
            await check_s3_connection(logger)  # Вызов функции проверки соединения с S3
            s3_connected = True
            break  # Выход из цикла, если соединение успешно
        except Exception as e:
            if attempt == retries - 1:  # Если это последняя попытка
                logger.error(f"Не удалось установить соединение с AWS S3 после {retries} попыток: {e}")
                return  # Завершение выполнения, если S3 недоступен
            logger.warning(f"Попытка {attempt + 1} из {retries}: Ошибка соединения с AWS S3. Повторная попытка через {retry_delay} секунд...")
            await asyncio.sleep(retry_delay)  # Задержка перед повторной попыткой

    if not s3_connected:
        return  # Завершение выполнения, если S3 недоступен после всех попыток

    logger.info("Соединение с AWS S3 установлено успешно.")  # Лог об успешном соединении

    # Основной цикл подписчика
    while True:
        try:
            pubsub = redis.pubsub()
            logger.info("Подключаемся к Redis...")
            await pubsub.subscribe(CHANNEL)
            logger.info(f"Подписались на канал {CHANNEL}")

            while True:
                try:
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=5.0)
                    if message:
                        logger.info(f"Получено сообщение от Redis: {message}")
                        task_data = json.loads(message["data"])  # Декодировка данных задачи
                        await handle_task(task_data)

                except Exception as e:
                    current_time = datetime.now()

                    # Логируем ошибку только если прошло больше error_cooldown с момента последней ошибки
                    if last_error_time is None or (current_time - last_error_time) > error_cooldown:
                        logger.error(f"Ошибка при получении сообщения: {e}")

                        last_error_time = current_time  # Обновляем время последней ошибки

        except Exception as e:
            logger.error(f"Ошибка при подписке на канал: {e}")
            await asyncio.sleep(retry_delay)  # Задержка перед повторной попыткой

if __name__ == "__main__":
    asyncio.run(main())