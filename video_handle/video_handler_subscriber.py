""" Модуль, который подписывается на канал Redis, получает сообщения и вызывает асинхронные функции """

""" Модуль, который подписывается на канал Redis, получает сообщения и вызывает асинхронные функции """

import json
import asyncio
from redis.asyncio import Redis
from video_handler_worker import (
    convert_to_vp9,
    extract_preview,
    upload_to_s3,
    save_profile_to_db,
    PREVIEW_DURATION
)
from logging_config import get_logger

logger = get_logger()

CHANNEL = "video_tasks"  # Канал для видео задач
REDIS_HOST = "localhost"  # Адрес Redis сервера


async def handle_task(task_data):
    """Обработка всех задач последовательно"""
    logger.info(f"Получена задача для обработки: {task_data}")

    try:
        # Последовательное выполнение всех шагов
        logger.info(f"Начинаю конвертацию видео {task_data['input_path']} в VP9")
        await convert_to_vp9(
            input_path=task_data["input_path"],
            output_path=task_data["output_path"],
            logger=logger,
        )

        logger.info(f"Задача конвертации для {task_data['input_path']} завершена успешно")

        logger.info(f"Начинаю извлечение превью для {task_data['input_path']}")
        await extract_preview(
            input_path=task_data["input_path"],
            output_path=task_data["preview_path"],
            duration=PREVIEW_DURATION,
            logger=logger,
        )

        logger.info(f"Извлечение превью для {task_data['input_path']} завершено успешно")

        logger.info(f"Начинаю загрузку видео {task_data['output_path']} и превью {task_data['preview_path']} в S3")
        await upload_to_s3(
            video_path=task_data["output_path"],
            preview_path=task_data["preview_path"],
            logger=logger,
        )

        logger.info(f"Загрузка файла {task_data['output_path']} в S3 завершена успешно")

        logger.info(f"Начинаю сохранение профиля в БД для пользователя {task_data['form_data'].get('name', 'Неизвестно')}")
        async with get_db_session() as session:
            await save_profile_to_db(
                session=session,
                form_data=task_data["form_data"],
                video_url=f"http://localhost:9000/{S3_BUCKET_NAME}/{task_data['s3_key']}",
                preview_url=f"http://localhost:9000/{S3_BUCKET_NAME}/{task_data['s3_key']}",
                user_logo_url=task_data["user_logo_url"],
                wallet_number=task_data["wallet_number"],
                logger=logger,
            )

        logger.info(f"Профиль с именем {task_data['form_data'].get('name', 'Неизвестно')} успешно сохранен в БД")

        user_name = task_data["form_data"].get("name", "Неизвестно")
        user_wallet = task_data["wallet_number"]

        logger.info(f"Весь цикл задач для {task_data['input_path']} выполнен успешно!")

        task_data.clear()  # Очищаем данные задачи после завершения

    except Exception as e:
        logger.error(f"Ошибка при обработке задачи для видео {task_data['input_path']}: {e}")
        raise


async def main():
    """Основная функция для подписки и обработки задач с ретрай-логикой"""
    logger.info("Запуск подписки на канал Redis для получения задач")
    retries = 5
    retry_delay = 5  # Задержка между попытками (в секундах)

    redis = Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

    while True:
        try:
            pubsub = redis.pubsub()
            logger.info("Подключаемся к Redis...")
            await pubsub.subscribe(CHANNEL)
            logger.info(f"Подписались на канал {CHANNEL}")

            while True:
                try:
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if message:
                        logger.info(f"Получено сообщение от Redis: {message}")
                        task_data = json.loads(message["data"])  # Декодируем данные задачи
                        logger.info(f"Данные задачи: {task_data}")

                        # Обрабатываем задачу
                        await handle_task(task_data)

                        retries = 5  # Сбрасываем количество попыток после успешной обработки
                    else:
                        logger.debug("Сообщение не получено в течение таймаута")

                except Exception as e:
                    logger.error(f"Ошибка при обработке сообщения: {e}")
                    retries -= 1
                    if retries > 0:
                        logger.info(f"Попытка повторить через {retry_delay} секунд...")
                        await asyncio.sleep(retry_delay)
                    else:
                        logger.error("Превышено количество попыток, работа прекращена")
                        break

                await asyncio.sleep(1)  # Пауза для избежания чрезмерных запросов

        except Exception as e:
            logger.error(f"Ошибка при подключении к Redis: {e}")
            await asyncio.sleep(retry_delay)  # Подождать перед новой попыткой подключения

if __name__ == "__main__":
    asyncio.run(main())
