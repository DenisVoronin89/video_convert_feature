"""Модуль для публикации задач в Redis"""

import aioredis
import json
import time
import asyncio
from logging_config import get_logger
from authentication_module import get_random_wallet

# Настройка логгера
logger = get_logger()

CHANNEL = "video_tasks"


async def publish_task(redis: aioredis.Redis, input_path, output_path, preview_path, s3_key, form_data):
    """Функция для отправки задачи в канал Redis с обработкой ошибок и дополнительным логированием"""

    # Получаем случайный кошелек
    wallet_number = get_random_wallet()

    # Собираем данные задачи
    task_data = {
        "input_path": input_path,
        "output_path": output_path,
        "preview_path": preview_path,
        "s3_key": s3_key,
        "form_data": form_data,
        "wallet_number": wallet_number  # Потом не из мока прилетать будет! Не просохатить этот момент!
    }

    retries = 5  # Количество попыток
    retry_delay = 5  # Задержка между попытками (в секундах)

    while retries > 0:
        try:
            # Публикуем задачу в канал Redis
            await redis.publish(CHANNEL, json.dumps(task_data))
            logger.info(f"Задача успешно отправлена в канал {CHANNEL}: {task_data}")
            break  # Прерываем цикл после успешной публикации

        except aioredis.RedisError as e:
            logger.error(f"Ошибка при публикации задачи в Redis: {e}")
            retries -= 1  # Уменьшаем количество попыток
            if retries > 0:
                logger.info(f"Попытка повторить публикацию через {retry_delay} секунд...")
                await asyncio.sleep(retry_delay)  # Асинхронная задержка между попытками
            else:
                logger.error(f"Не удалось опубликовать задачу в Redis после {5 - retries} попыток.")
                raise RuntimeError(f"Ошибка при публикации задачи в Redis: {e}")  # Пробрасываем ошибку дальше


