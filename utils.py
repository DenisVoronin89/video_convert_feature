"""  Модуль вспомогательных функций  """

import os
import time
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from shapely.geometry import Point, MultiPoint
from typing import Optional, List, Union
from geoalchemy2.shape import to_shape
from math import radians, sin, cos, sqrt, atan2

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
    """Функция для парсинга координат в WKT строку (Для сохранения в БД)"""
    if coordinates:
        # Преобразуем каждую пару координат (долгота, широта) в объект Point
        points = [Point(coord[1], coord[0]) for coord in coordinates]  # Долгота, Широта
        # Создаем MultiPoint из всех точек
        multi_point = MultiPoint(points)
        # Преобразуем MultiPoint в строку WKT
        return str(multi_point)
    return None


# Функция для преобразования datetime в строку
async def datetime_to_str(value):
    if isinstance(value, datetime):
        return value.isoformat()  # Преобразуем в строку ISO 8601
    return value


async def process_coordinates_for_response(coordinates) -> Optional[Union[List[float], List[List[float]]]]:
    """
    Обрабатывает координаты профиля.

    :param coordinates: Координаты из базы данных (WKT или геометрический объект).
    :return: Список координат в формате [долгота, широта] или список списков для MultiPoint.
             Возвращает None, если координаты отсутствуют или произошла ошибка.
    """
    if not coordinates:
        return None

    try:
        geometry = to_shape(coordinates)
        if isinstance(geometry, Point):
            return [geometry.x, geometry.y]  # [долгота, широта]
        elif isinstance(geometry, MultiPoint):
            return [[point.x, point.y] for point in geometry.geoms]  # Список списков
        else:
            logger.warning(f"Неизвестный тип геометрии: {type(geometry)}")
            return None
    except Exception as e:
        logger.error(f"Ошибка при обработке координат: {str(e)}")
        return None


# Вычисление расстояния между точками (юзаем во вьюхе при отдаче точек в радиусе для проверки ответа)
async def calculate_distance(lat1, lon1, lat2, lon2):
    """
    Вычисляет расстояние (в метрах) между двумя точками на Земле,
    используя формулу Хаверсина и координаты в градусах.

    :param lat1: Широта первой точки в градусах
    :param lon1: Долгота первой точки в градусах
    :param lat2: Широта второй точки в градусах
    :param lon2: Долгота второй точки в градусах
    :return: Расстояние между точками в метрах
    """
    # Радиус Земли в метрах
    R = 6371000

    # Преобразуем координаты из градусов в радианы
    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)

    # Разница в координатах
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    # Применение формулы Хаверсина
    a = sin(dlat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    # Расстояние в метрах
    distance = R * c
    return distance


