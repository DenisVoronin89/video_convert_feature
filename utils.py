"""  Модуль вспомогательных функций  """

import os
import time
import asyncio
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


# Логика удаления старых файлов
async def delete_old_files(directories):
    """Асинхронное удаление временных файлов старше 48 часов в заданных директориях."""
    try:
        current_time = time.time()
        total_deleted = 0  # Общее количество удалённых файлов

        def remove_file_if_old(file_path):
            """Проверяет возраст файла и удаляет, если он старше 48 часов."""
            if os.path.exists(file_path) and os.path.isfile(file_path):
                file_age = current_time - os.path.getmtime(file_path)
                if file_age > 48 * 3600:  # 48 часов в секундах
                    os.remove(file_path)
                    return 1  # Возвращаем 1, если файл удалён
            return 0  # Возвращаем 0, если файл не удалён

        for directory in directories:
            if os.path.exists(directory):
                deleted_count = 0  # Количество удалённых файлов в текущей директории
                for file_name in os.listdir(directory):
                    file_path = os.path.join(directory, file_name)
                    deleted_count += remove_file_if_old(file_path)

                if deleted_count > 0:
                    logger.info(f"В директории {directory} удалено {deleted_count} файлов.")
                else:
                    logger.info(f"В директории {directory} файлов для удаления не найдено.")
                total_deleted += deleted_count
            else:
                logger.warning(f"Директория не существует: {directory}")

        if total_deleted > 0:
            logger.info(f"Все временные файлы старше 48 часов успешно удалены. Всего удалено: {total_deleted} файлов.")
        else:
            logger.info("Файлов для удаления не найдено.")

    except Exception as e:
        logger.error(f"Ошибка при удалении временных файлов: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Ошибка при удалении временных файлов")


# Задача для планировщика (удаление старых файлов)
async def scheduled_cleanup_task():
    """Асинхронная задача для удаления старых файлов."""
    directories = [
        "./video_temp/",
        "./image_temp/",
        "./output_video/",
        "./output_preview/"
    ]

    logger.info("Запуск задачи очистки временных файлов.")
    await delete_old_files(directories)
    logger.info("Задача очистки временных файлов завершена.")


# Логика для парсинга координат из БД
async def parse_coordinates(coordinates):
    """Функция для парсинга координат в WKT строку (Для сохранения в БД)"""
    if coordinates:
        # Преобразуем каждую пару координат (долгота, широта) в объект Point
        points = [Point(coord[0], coord[1]) for coord in coordinates]  # Долгота, Широта
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


# Обработка координат для ответа
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


# Очистка старых логов
async def clean_old_logs(log_file: str, max_age_minutes: int = 10):
    """
    Очищает логи старше указанного времени (в минутах).
    Удаляет строки, которые не соответствуют формату даты.
    """
    try:
        current_time = time.time()
        max_age_seconds = max_age_minutes * 60

        # Читаем все строки из файла
        with open(log_file, "r") as file:
            lines = file.readlines()

        # Фильтруем строки, оставляя только те, которые моложе max_age_seconds и соответствуют формату
        new_lines = []
        for line in lines:
            # Проверяем, начинается ли строка с даты в формате "2025-03-01 22:00:00,001"
            if " - " in line:
                log_time_str = line.split(" - ")[0]
                try:
                    log_time = time.mktime(time.strptime(log_time_str, "%Y-%m-%d %H:%M:%S,%f"))
                    if current_time - log_time <= max_age_seconds:
                        new_lines.append(line)
                except ValueError:
                    # Если строка не соответствует формату, пропускаем её (не добавляем в new_lines)
                    continue

        # Перезаписываем файл только актуальными строками
        with open(log_file, "w") as file:
            file.writelines(new_lines)

        logger.info(f"Логи старше {max_age_minutes} минут и строки с неправильным форматом удалены из файла {log_file}.")

    except Exception as e:
        logger.error(f"Ошибка при очистке логов: {e}", exc_info=True)

