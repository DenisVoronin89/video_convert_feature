""" Модуль для настройки логирования работы приложения """

import logging

# Настройка логов
log_level = logging.DEBUG
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_file = 'video_service.log'
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(log_formatter)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)
logger.addHandler(file_handler)

def get_logger():
    """Возвращает конфигурированный логгер"""
    return logger
