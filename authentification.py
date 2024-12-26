""" Модуль для получения номера кошелька по которому в наш проект залогинился клиент"""

# Пока тестово и моково все написано, потом напишу скрипт который из запроса будет вытягивать номер кошелька клиента

import random

from logging_config import get_logger

logger = get_logger()

# Моковые метамаск кошельки
mock_wallets = {
    "0x6a2b35e8f8b790e41333d9e28f4e96c4168b34f8",  # Пример 1
    "0x50fe23b6a98a8c5f10b98c48b3e44e4c2baf42e9",  # Пример 2
    "0x9f9b2c710385f71f497f41b37b5c8b9c163ed06b",  # Пример 3
    "0x9b5d1054f6438b97d3974f4e19a77f98bcac5d3a",  # Пример 4
    "0xb3d1a18a924e1104d61d2137a9b62d1f202d5396",  # Пример 5
    "0xd6b3a5e208f570d0864b604597c5a2a9f14fc88e",  # Пример 6
    "0xf3547d0adf88ef55c8b9e1658cfb2a73e3db8f8c",  # Пример 7
    "0x1b671b03cc4ed7d1e8c627509517a3d79c1c2f4d",  # Пример 8
}


async def get_random_wallet():
    """Получения случайного метамаск кошелька для тестов"""
    wallet_number = random.choice(list(mock_wallets))

    logger.info(f"Получен кошелек с номером: {wallet_number}")

    return wallet_number


