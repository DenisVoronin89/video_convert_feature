"""
Модуль, который подписывается на канал Redis, получает сообщения и
вызывает в работу функции с модуля video_handler_worker.py
"""

import json
import asyncio
from sqlalchemy.exc import SQLAlchemyError
from database import get_db_session_for_worker
from redis.asyncio import Redis
from video_handle.video_handler_worker import (
    convert_to_vp9,
    extract_preview,
    upload_to_s3,
    save_profile_to_db,
    PREVIEW_DURATION
)
from logging_config import get_logger


logger = get_logger()

CHANNEL = "video_tasks"  # Канал для видео задач
REDIS_HOST = "redis"
S3_BUCKET_NAME = "video-service"
AWS_REGION = "us-east-1"
AWS_ACCESS_KEY_ID = "pkZKH9pAVimC5SqmBf1r"
AWS_SECRET_ACCESS_KEY = "NO7zSwNyYrNXOiFcBAL2gRYbaZ3kgngdtD8qUEjd"


async def handle_task(task_data):
    """Обработка всех задач последовательно"""
    logger.info(f"Получена задача для обработки: {task_data}")

    try:
        # Извлечение строк путей (потому что созданные директории это словарь)
        input_path = task_data["input_path"]
        output_path = task_data["output_path"]["path"]
        preview_path = task_data["preview_path"]["path"]
        user_logo_url = task_data["user_logo_url"]

        # Последовательное выполнение всех шагов
        # 1. Конвертация видео
        logger.info(f"Начинаю конвертацию видео {task_data['input_path']} в VP9")
        converted_video = await convert_to_vp9(
            input_path=task_data["input_path"],
            output_path=output_path,
            logger=logger,
        )

        logger.info(f"Задача конвертации для {task_data['input_path']} завершена успешно")

        # 2. Извлечение превью
        logger.info(f"Начинаю извлечение превью для {task_data['input_path']}")
        preview_video = await extract_preview(
            input_path=task_data["input_path"],
            preview_path=preview_path,
            duration=PREVIEW_DURATION,
            logger=logger,
        )

        logger.info(f"Извлечение превью для {task_data['input_path']} завершено успешно")

        # 3. Загрузка в хранилище (пока по дефолту юзаем AWS S3, потом в проде переписать в зависимости от хранилища)
        logger.info(f"Начинаю загрузку видео {task_data['output_path']} и превью {task_data['preview_path']} в S3")
        video_url, preview_url = await upload_to_s3(
            converted_video,
            preview_video,
            logger=logger,
        )

        logger.info(f"Загрузка видео {task_data['output_path']} в S3 завершена успешно. URL: {video_url}")
        logger.info(f"Загрузка превью {task_data['preview_path']} в S3 завершена успешно. URL: {preview_url}")

        # 4. Сохранение профиля в БД
        logger.info(f"Начинаю сохранение профиля в БД для пользователя {task_data['form_data'].get('name', 'Неизвестно')}")
        try:
            async with get_db_session_for_worker() as session:
                # Передача сессии в ф-ию save_profile_to_db
                await save_profile_to_db(
                    session=session,
                    form_data=task_data["form_data"],
                    video_url=video_url,
                    preview_url=preview_url,
                    user_logo_url=task_data["user_logo_url"],
                    wallet_number=task_data["wallet_number"],
                    logger=logger
                )

            logger.info(f"Профиль с именем {task_data['form_data'].get('name', 'Неизвестно')} успешно сохранен в БД")

        except SQLAlchemyError as e:
            logger.error(f"Ошибка при сохранении профиля в БД: {e}")
            raise

        logger.info(f"Весь цикл задач для {task_data['input_path']} выполнен успешно!")

    except Exception as e:
        logger.error(f"Ошибка при обработке задачи для видео {task_data['input_path']}: {e}")
        raise

    finally:
        # Очистка данных задачи после выполнения или ошибки
        task_data.clear()
        logger.info("Данные задачи очищены.")


async def main():
    """Основная функция для подписки и обработки задач с ретрай-логикой"""
    logger.info("Запуск подписки на канал Redis для получения задач")
    retries = 5
    retry_delay = 5

    redis = Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

    # Проверка доступности Redis
    try:
        await redis.ping()
        logger.info("Redis доступен.")
    except Exception as e:
        logger.error(f"Ошибка подключения к Redis: {e}")
        return  # Завершение выполнения, если Redis недоступен

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
                    logger.error(f"Ошибка при получении сообщения: {e}")

        except Exception as e:
            logger.error(f"Ошибка при подписке на канал: {e}")
            await asyncio.sleep(retry_delay)  # Задержка перед повторной попыткой

if __name__ == "__main__":
    asyncio.run(main())
