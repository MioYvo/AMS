from functools import lru_cache

import redis.asyncio as redis
from databases import Database
from sanic import Sanic
from telethon import TelegramClient

from config import settings

redis_client = redis.Redis.from_url(settings.REDIS_URL)


db_url = f'mysql+aiomysql://{settings.DB_USER}:{settings.DB_PASSWD}@' \
         f'{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}'

database = Database(
    db_url,
    ssl=False,
    echo='error',
    min_size=settings.DB_MIN_CONN,
    max_size=settings.DB_MAX_CONN,
    pool_recycle=settings.DB_RECYCLE_SECONDS
)


@lru_cache(maxsize=1)
def bot() -> TelegramClient:
    return Sanic.get_app(settings.APP_NAME).ctx.tg_client
