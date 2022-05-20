import hashlib
import json
from copy import deepcopy
from decimal import Decimal
from typing import Optional

from arrow import Arrow
from databases import Database
from databases.core import Connection
from sanic import Sanic
from sanic.log import logger
from sqlalchemy import Table
from sqlalchemy.engine import Row
from sqlalchemy.sql.ddl import CreateTable

from app.model import Transaction
from config import settings


class AMSCoreClass:
    index = [5, 0, 1, 8, 4, 6, 2, 3, 9, 7]
    hash_index = [7, 12, 13, 16, 21, 26, 28, 34, 61, 63]
    new_hash_index = [7, 13, 15, 19, 25, 31, 34, 41, 69, 72]
    TABLE_SPLIT_FMT = '%Y_%m'

    def __init__(self):
        self.TABLE_SPLIT_FMT = '%Y_%m'
        self.model_mapping = {}

    @classmethod
    def db(cls) -> Database:
        app = Sanic.get_app(settings.APP_NAME)
        return app.ctx.database

    @classmethod
    def conn(cls):
        return cls.db().connection()

    @classmethod
    def format_query(cls, query: str, values: dict):
        for k, v in values.items():
            query = query.replace(f":{k}", str(v))

        return query

    @classmethod
    def build_txn_hash(cls, asset, from_addr, to_addr, amount, from_sequence, create_at, op=None):
        txn_raw = {
            "asset": asset, "from": from_addr, "to": to_addr,
            "amount": str(amount) if isinstance(amount, Decimal) else amount, "from_sequence": from_sequence,
            "create_at": create_at
        }
        if op:
            txn_raw['op'] = op
        return txn_raw, hashlib.sha256(json.dumps(txn_raw, separators=(',', ':')).encode()).hexdigest()

    @classmethod
    def build_txn(cls, asset, from_addr, to_addr, amount, from_sequence, op=None):
        create_at = int(Arrow.now().timestamp())
        txn_raw, txn_hash = cls.build_txn_hash(asset, from_addr, to_addr, amount, from_sequence, create_at, op=op)
        txn_ts_hash = cls.build_ts_hash(ts=create_at, txn_hash=txn_hash)
        return txn_ts_hash, txn_raw

    @classmethod
    def build_ts_hash(cls, ts: float, txn_hash: str):
        list_hash = list(txn_hash)
        t = list(str(ts))
        new_t = [t[i] for i in cls.index]
        # origin_t = [new_t[index.index(i)] for i in range(10)]

        for i, v in enumerate(cls.new_hash_index):
            list_hash.insert(v, new_t[i])

        # origin_list_hash = ''.join([list_hash.pop(i) for i in hash_index])
        return ''.join(list_hash)

    @classmethod
    def parse_hash(cls, txn_hash: str):
        assert len(txn_hash) == 74

        list_hash = list(txn_hash)
        list_ts = [list_hash.pop(i) for i in cls.hash_index]
        origin_hash = ''.join(list_hash)

        origin_ts = int(''.join([list_ts[cls.index.index(i)] for i in range(10)]))
        return origin_hash, origin_ts

    def get_model(self, table_name):
        return self.model_mapping.get(table_name)

    async def get_txn_model(self, txn_hash: str, conn: Connection) -> Table:
        table_name = await self._txn_table_name(txn_hash, conn)
        return self.model_mapping.get(table_name, Transaction)

    async def check_tables(self, table_name: str, conn: Connection, model: Table):
        row: Optional[Row] = await conn.fetch_one(f"SHOW tables like '{table_name}';")
        if not row:
            new_model = deepcopy(model)
            new_model.name = table_name
            self.model_mapping[table_name] = new_model

            logger.info(f"Create Table {table_name}")
            await conn.execute(CreateTable(self.model_mapping[table_name], if_not_exists=True))
        else:
            if not self.model_mapping.get(table_name):
                new_model = deepcopy(model)
                new_model.name = table_name
                self.model_mapping[table_name] = new_model

    @classmethod
    def origin_table_name(cls, model):
        return model.name.split('__')[0]

    def __txn_table_name(self, txn_hash: str, model: Table = Transaction):
        assert len(txn_hash) == 74
        _, origin_ts = self.parse_hash(txn_hash)
        local_dt = Arrow.fromtimestamp(origin_ts)
        local_dt_str = local_dt.strftime(self.TABLE_SPLIT_FMT)
        return f"{self.origin_table_name(model)}__{local_dt_str}", local_dt

    async def _txn_table_name(self, txn_hash: str, conn: Connection) -> str:
        table_name, local_dt = self.__txn_table_name(txn_hash=txn_hash)

        model = self.model_mapping.get(table_name)
        if model is None:
            await self.check_tables(table_name=table_name, conn=conn, model=Transaction)
            return table_name
        else:
            return table_name


AMSCore = AMSCoreClass()
