""" Модуль для настройки логирования работы приложения """

import logging
from logging.handlers import RotatingFileHandler
import inspect

# Настройка основного логгера
log_level = logging.DEBUG
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_file = 'video_service.log'

# Основной логгер
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(log_level)
logger.addHandler(file_handler)

# Настройка логгера для ошибок
error_logger = logging.getLogger("error_logger")
error_logger.setLevel(logging.ERROR)

# Обработчик для записи ошибок в отдельный файл
error_handler = RotatingFileHandler(
    "application_errors.log",
    maxBytes=10 * 1024 * 1024,  # 10 MB
    backupCount=5,  # Храним до 5 файлов с ошибками
)
error_handler.setFormatter(logging.Formatter(
    "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - Line %(lineno)d - %(message)s"
))
error_logger.addHandler(error_handler)

def get_logger():
    """
    Возвращает конфигурированный логгер с расширенным функционалом.
    """
    # Добавляем кастомный метод error к объекту логгера
    def custom_error(logger, msg, *args, **kwargs):
        """
        Кастомный метод для логирования ошибок с дополнительной информацией.
        """
        # Получаем информацию о месте, где произошла ошибка
        frame = inspect.currentframe().f_back
        module_name = frame.f_globals['__name__']
        func_name = frame.f_code.co_name
        line_no = frame.f_lineno

        # Добавляем информацию о модуле, функции и строке
        msg = f"{msg}\nModule: {module_name}, Function: {func_name}, Line: {line_no}"

        # Если есть информация о запросе, добавляем её
        if 'request_info' in kwargs:
            msg = f"{msg}\nRequest Info: {kwargs.pop('request_info')}"

        # Логируем ошибку
        error_logger.error(msg, *args, **kwargs)

    # Привязываем кастомный метод к логгеру
    logger.error = lambda msg, *args, **kwargs: custom_error(logger, msg, *args, **kwargs)
    return logger

