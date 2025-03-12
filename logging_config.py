""" Модуль для настройки логирования работы приложения """

import logging
import inspect
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Получаем значение DEBUG_MODE из .env
DEBUG_MODE = os.getenv("DEBUG_MODE", "False").lower() == "true"

# Настройка основного логгера
log_level = logging.DEBUG if DEBUG_MODE else logging.ERROR  # Логи только в режиме разработки
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')  # Формат для основных логов
log_file = 'video_service.log'

# Основной логгер
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(log_level)
logger.addHandler(file_handler)

# Логгер для ошибок (всегда активен, независимо от DEBUG_MODE)
error_logger = logging.getLogger("error_logger")
error_logger.setLevel(logging.ERROR)

# Обработчик для записи ошибок в отдельный файл
error_handler = logging.FileHandler("application_errors.log")
error_handler.setFormatter(logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"  # Формат для ошибок
))
error_logger.addHandler(error_handler)

def get_logger():
    """
    Возвращает конфигурированный логгер с расширенным функционалом.
    """
    def custom_error(msg, *args, **kwargs):
        """
        Кастомный метод для логирования ошибок с дополнительной информацией.
        """
        # Получаем кадр стека, где произошла ошибка
        frame = inspect.currentframe()
        while frame:
            # Пропускаем кадры, связанные с логгерами
            if frame.f_code.co_name in ("get_logger", "custom_error"):
                frame = frame.f_back
                continue
            # Останавливаемся на первом подходящем кадре
            break

        if frame:
            module_name = frame.f_globals.get("__name__", "unknown")
            func_name = frame.f_code.co_name
            line_no = frame.f_lineno
        else:
            module_name = "unknown"
            func_name = "unknown"
            line_no = 0

        # Добавляем информацию о модуле, функции и строке
        msg = f"{msg}\nModule: {module_name}, Function: {func_name}, Line: {line_no}"

        # Логируем ошибку в video_service.log (основной логгер)
        logger._log(logging.ERROR, msg, args, **kwargs)

        # Дублируем ошибку в application_errors.log
        error_logger._log(logging.ERROR, msg, args, **kwargs)

    # Привязываем кастомный метод к логгеру
    logger.error = custom_error

    return logger