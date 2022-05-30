from asyncio import sleep
from enum import Enum

from AMS.config import settings
from AMS.clients import redis_client, bot

msgs_key = settings.AMS_MSG_KEY_NAME
group_id = settings.AMS_BOT_GROUP


async def send_from_redis_to_telegram():
    msg = await redis_client.rpop(msgs_key, count=100)
    if msg:
        for m in msg:
            await bot().send_message(await bot().get_entity(group_id), m.decode())
            await sleep(.5)


class AMSWarningLevel(Enum):
    invalid_transaction = "无效交易"
    invalid_account = "无效账户"


async def send_msg(msg: str, level: AMSWarningLevel):
    msg = f"**{level.value}**\n" \
          f"{msg}"
    await redis_client.lpush(msgs_key, msg)
