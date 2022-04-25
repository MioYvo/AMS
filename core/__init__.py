from databases import Database
from sanic import Sanic

from config import settings


class AMSCore:
    @classmethod
    def db(cls) -> Database:
        app = Sanic.get_app(settings.APP_NAME)
        return app.ctx.database

    @classmethod
    def conn(cls):
        return cls.db().connection()
