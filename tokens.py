from authlib.jose import jwt
from datetime import datetime, timedelta
from typing import Optional
from pydantic import BaseModel
from fastapi import HTTPException, status

from logging_config import get_logger

logger = get_logger()

SECRET_KEY = "fasdklj133485u12nkasj9dkachasdn37TYUNVDWDHNcegn37"
ALGORITHM = "HS256"  # Алгоритм подписи
ACCESS_TOKEN_EXPIRE_MINUTES = 30  # Время жизни access токена (в минутах)
REFRESH_TOKEN_EXPIRE_DAYS = 1  # Время жизни refresh токена (в днях)


# Модели данных для передачи информации
class Token(BaseModel):
    access_token: str
    refresh_token: str


class TokenData(BaseModel):
    user_id: int


# Генерация access токена (асинхронно)
async def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    try:
        to_encode = data.copy()
        expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
        to_encode.update({"exp": expire})

        # Генерация токена с использованием правильных параметров
        encoded_jwt_access = jwt.encode(
            {"alg": ALGORITHM},
            to_encode,
            algorithm=ALGORITHM,
            key=SECRET_KEY
        )
        return encoded_jwt_access
    except Exception as e:
        logger.error(f"Ошибка при генерации access токена: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка при генерации access токена")


# Генерация refresh токена (асинхронно)
async def create_refresh_token(data: dict) -> str:
    try:
        to_encode = data.copy()
        expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
        to_encode.update({"exp": expire})

        # Генерация токена с использованием правильных параметров
        encoded_jwt_refresh = jwt.encode(
            {"alg": ALGORITHM},
            to_encode,
            algorithm=ALGORITHM,
            key=SECRET_KEY
        )
        return encoded_jwt_refresh
    except Exception as e:
        logger.error(f"Ошибка при генерации refresh токена: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка при генерации refresh токена")


# Генерация обоих токенов (access и refresh)
async def create_tokens(user_id: int) -> Token:
    try:
        data = {"user_id": user_id}
        access_token = await create_access_token(data)
        refresh_token = await create_refresh_token(data)
        return Token(access_token=access_token, refresh_token=refresh_token)
    except Exception as e:
        logger.error(f"Ошибка при создании токенов для пользователя с ID {user_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка при создании токенов")


# Валидация access токена (асинхронно)
async def verify_access_token(token: str) -> TokenData:
    try:
        # Используем authlib для декодирования токена
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return TokenData(**payload)
    except jwt.ExpiredSignatureError:
        logger.warning(f"Access token expired: {token}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Access token has expired")
    except jwt.JWTError as e:
        logger.error(f"Ошибка при валидации access токена: {e}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid access token")
    except Exception as e:
        logger.error(f"Неизвестная ошибка при валидации access токена: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка при валидации access токена")


# Валидация refresh токена (асинхронно)
async def verify_refresh_token(token: str) -> TokenData:
    try:
        # Используем authlib для декодирования токена
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return TokenData(**payload)
    except jwt.ExpiredSignatureError:
        logger.warning(f"Refresh token expired: {token}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Refresh token has expired")
    except jwt.JWTError as e:
        logger.error(f"Ошибка при валидации refresh токена: {e}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid refresh token")
    except Exception as e:
        logger.error(f"Неизвестная ошибка при валидации refresh токена: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка при валидации refresh токена")


# Обновление access токена (асинхронно)
async def refresh_access_token(refresh_token: str) -> Token:
    try:
        user_data = await verify_refresh_token(refresh_token)
        new_tokens = await create_tokens(user_data.user_id)
        return new_tokens
    except Exception as e:
        logger.error(f"Ошибка при обновлении access токена для пользователя: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка при обновлении access токена")
