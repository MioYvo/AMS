import sys
from pathlib import Path

from sanic import Sanic, Blueprint
from databases import Database
from sanic.handlers import ErrorHandler
from sqlalchemy.sql.ddl import DropTable, CreateTable, CreateIndex
from loguru import logger
import redis.asyncio as redis

from app.account.api import accounts_v1_bp
from app.transaction.api import transactions_v1_bp
from app.model import Transaction, Account
from config import settings
from core.log import LOGGING_CONFIG, fmt

logger.remove(0)    # remove default stderr sink
logger.add(sys.stderr, level='INFO', format=fmt, diagnose=False, backtrace=False)

logger.add(
    Path(".").absolute()/"log"/"ams.log", rotation="50 MB", encoding='utf-8', colorize=False, level='INFO',
    format=fmt, diagnose=False, backtrace=False
)


app = Sanic(settings.APP_NAME, log_config=LOGGING_CONFIG)
app.config.FALLBACK_ERROR_FORMAT = "json"

bp = Blueprint.group(accounts_v1_bp, transactions_v1_bp, url_prefix='/ams')
app.blueprint(bp)

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

redis_client = redis.Redis.from_url(settings.REDIS_URL)


@app.before_server_start
async def setup_db(app_, _):
    logger.info('db: connecting ...')
    await database.connect()
    logger.info(f'db: connection {database.is_connected}')
    app_.ctx.database = database
    async with database.connection() as conn:
        if settings.RECREATE_TABLES:
            await conn.execute(DropTable(Transaction, if_exists=True))
            await conn.execute(DropTable(Account, if_exists=True))
            print(CreateTable(Account))
            print(CreateTable(Transaction))
            await conn.execute(CreateTable(Account, if_not_exists=True))
            if Account.indexes:
                for index in Account.indexes:
                    await conn.execute(CreateIndex(index))
            await conn.execute(CreateTable(Transaction, if_not_exists=True))
            if Transaction.indexes:
                for index in Transaction.indexes:
                    await conn.execute(CreateIndex(index))


@app.after_server_stop
async def stop_db(app_, _):
    logger.info('db: disconnecting ...')
    await database.disconnect()
    logger.info(f'db: connection {database.is_connected}')
    app_.ctx.database = None


@app.before_server_start
async def ping_redis(app_, _):
    logger.info('redis: ping ...')
    logger.info(f"redis: ping successful: {await redis_client.ping()}")
    app_.ctx.redis = redis_client


@app.after_server_stop
async def stop_redis(app_, _):
    logger.info('redis: closing ...')
    await app_.ctx.redis.close()
    app_.ctx.redis = None


class AMSErrorHandler(ErrorHandler):
    def default(self, request, exception):
        self.log(request, exception)
        # You custom error handling logic...
        http_response = super(AMSErrorHandler, self).default(request, exception)
        http_response.status = 200
        return http_response


app.error_handler = AMSErrorHandler()


# @app.on_request
# async def decrypt_body(request: Request):
#     await request.receive_body()
#     request.ctx.decrypt_body = decrypt(request.body)
#
# @app.on_response
# async def encrypt_body(request: Request, response: HTTPResponse):
#     return text(encrypt(response.body))


if __name__ == "__main__":
    logger.info(db_url)
    app.run(host="0.0.0.0", port=8000, debug=False, access_log=False)
