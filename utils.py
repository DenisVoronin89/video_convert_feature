"""  Модуль вспомогательных функций  """

import os
import time
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from shapely.geometry import Point, MultiPoint

from logging_config import get_logger

logger = get_logger()

def get_file_size(file_path):
    """Получение размера файла"""
    try:
        return os.path.getsize(file_path) / (1024 * 1024)  # В мегабайтах
    except Exception as e:
        raise RuntimeError(f"Не удалось получить размер файла {file_path}: {e}")


async def delete_temp_files(temp_video_path, temp_image_path, converted_video_path, preview_path):
    """Удаление временных файлов после завершения всех операций."""
    try:
        current_time = time.time()

        # Проверка и удаление файлов старше 48 часов
        def remove_file_if_old(file_path):
            if file_path and os.path.exists(file_path):
                file_age = current_time - os.path.getmtime(file_path)
                if file_age > 48 * 3600:  # 48 часов в секундах
                    os.remove(file_path)
                    logger.info(f"Удален временный файл: {file_path}")
                else:
                    logger.info(f"Файл не удалён, так как он младше 48 часов: {file_path}")
            else:
                logger.warning(f"Файл не существует: {file_path}")

        # Проверка файлов на старость и удаление
        remove_file_if_old(temp_video_path)
        remove_file_if_old(temp_image_path)
        remove_file_if_old(converted_video_path)
        remove_file_if_old(preview_path)

    except Exception as e:
        logger.error(f"Ошибка при удалении временных файлов: {e}")


def schedule_file_cleanup():
    """Запланировать задачу удаления файлов каждый день в 00:00."""
    try:
        scheduler = AsyncIOScheduler()

        # Запускать задачу каждый день в 00:00
        scheduler.add_job(
            delete_old_files_task,
            CronTrigger(hour=0, minute=0, second=0)
        )

        logger.info("Задача удаления файлов запланирована на каждый день в 00:00")

        # Старт планировщика
        scheduler.start()

    except Exception as e:
        logger.error(f"Ошибка при планировании задачи очистки файлов: {e}")


async def delete_old_files_task():
    """Задача для удаления старых файлов"""
    try:
        # Пути файлов, которые нужно проверять
        temp_video_path = "./video_temp/temp_video_file.mp4"
        temp_image_path = "./video_temp/temp_image_file.jpg"
        converted_video_path = "./video_temp/converted_video_file.webm"
        preview_path = "./video_temp/preview_file.webm"

        logger.info("Запуск задачи удаления старых файлов.")

        # Удаление временных файлов
        await delete_temp_files(temp_video_path, temp_image_path, converted_video_path, preview_path)

        logger.info("Задача удаления старых файлов завершена.")

    except Exception as e:
        logger.error(f"Ошибка при удалении старых файлов: {e}")



async def parse_coordinates(coordinates):
    """Функция для парсинга координат в WKT строку"""
    if coordinates:
        # Преобразуем каждую пару координат (долгота, широта) в объект Point
        points = [Point(coord[1], coord[0]) for coord in coordinates]  # Долгота, Широта
        # Создаем MultiPoint из всех точек
        multi_point = MultiPoint(points)
        # Преобразуем MultiPoint в строку WKT
        return str(multi_point)
    return None