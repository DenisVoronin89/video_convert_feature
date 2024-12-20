""" Модуль, который подписывается на канал Redis, получает сообщения и вызывает асинхронные функции """

import redis
import json
import asyncio
import time

from video_handler_worker import convert_to_vp9, extract_preview, upload_to_s3, save_profile_to_db  # Импортируем функции для задач
from logging_config import get_logger

logger = get_logger()

CHANNEL = "video_tasks"  # Канал для видео задач
REDIS_HOST = "localhost"  # Адрес Redis сервера

# Подключаемся к Redis
r = redis.Redis(host=REDIS_HOST, decode_responses=True)
p = r.pubsub()
p.subscribe(CHANNEL)  # Подписываемся на канал для получения задач

async def handle_task(task_data):
    """Обработка всех задач последовательно"""
    logger.info(f"Получена задача: {task_data['type']} для обработки")

    try:
        # Последовательное выполнение всех шагов
        # 1. Конвертация видео в VP9
        await convert_to_vp9(
            input_path=task_data["input_path"],
            output_path=task_data["output_path"],
            logger=logger,
        )
        logger.info(f"Задача конвертации для {task_data['input_path']} завершена успешно")

        # 2. Извлечение превью
        await extract_preview(
            input_path=task_data["input_path"],
            output_path=task_data["preview_output_path"],
            duration=task_data.get("duration", PREVIEW_DURATION),
            logger=logger,
        )
        logger.info(f"Извлечение превью для {task_data['input_path']} завершено успешно")

        # 3. Загрузка файла в S3
        await upload_to_s3(
            file_path=task_data["output_path"],
            s3_key=task_data["s3_key"],
            logger=logger,
        )
        logger.info(f"Загрузка файла {task_data['output_path']} в S3 завершена успешно")

        # 4. Сохранение профиля в БД
        async with get_db_session() as session:
            await save_profile_to_db(
                session=session,
                form_data=task_data["form_data"],
                video_url=task_data["video_url"],
                preview_url=task_data["preview_url"],
                user_logo_url=task_data["user_logo_url"],
                logger=logger,
            )

        # Извлекаем имя и кошелек из данных формы для человекочитаемого лога
        user_name = task_data["form_data"].get("name", "Неизвестно")
        user_wallet = task_data["form_data"].get("wallet", "Неизвестно")

        # Логируем успешное сохранение профиля
        logger.info(f"Профиль с именем {user_name} и кошельком {user_wallet} успешно сохранен в БД")

        # Логируем успешное выполнение всей очереди
        logger.info(f"Весь цикл задач {task_data['input_path']} выполнен успешно!")

        # Удаляем задачу из памяти (если требуется, например, очищаем объект task_data или другие ресурсы)
        task_data.clear()

    except Exception as e:
        logger.error(f"Ошибка при обработке задачи {task_data['type']} для видео {task_data['input_path']}: {e}")
        raise  # Пробрасываем исключение дальше для обработки на уровне подписки


ry:









def main():
    """Основная функция для подписки и обработки задач с ретрай-логикой"""
    logger.info("Запуск подписки на канал Redis для получения задач")

    while True:
        retries = 5  # Количество попыток
        retry_delay = 5  # Задержка между попытками (в секундах)

        while retries > 0:
            try:
                message = p.get_message()
                if message and message["type"] == "message":
                    task_data = json.loads(message["data"])  # Получаем данные задачи
                    asyncio.run(handle_task(task_data))  # Обрабатываем задачу асинхронно
                    retries = 5  # Сбрасываем количество попыток после успешной обработки
                    break  # Выходим из цикла ожидания

            except Exception as e:
                logger.error(f"Ошибка при получении сообщения из канала Redis: {e}")
                retries -= 1
                if retries > 0:
                    logger.info(f"Попытка повторить через {retry_delay} секунд...")
                    time.sleep(retry_delay)  # Ожидание перед повторной попыткой
                else:
                    logger.error("Превышено количество попыток, прекращаем работу.")
                    break  # Прерываем работу, если попытки не удались

        time.sleep(1)  # Пауза перед следующей итерацией для избежания чрезмерных запросов

if __name__ == "__main__":
    main()
