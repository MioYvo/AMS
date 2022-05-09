import hashlib
import json

from arrow import Arrow
from databases import Database
from sanic import Sanic

from config import settings


class AMSCore:
    index = [5, 0, 1, 8, 4, 6, 2, 3, 9, 7]
    hash_index = [7, 12, 13, 16, 21, 26, 28, 34, 61, 63]
    new_hash_index = [7, 13, 15, 19, 25, 31, 34, 41, 69, 72]

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
    def build_txn_hash(cls, asset, from_addr, to_addr, amount, from_sequence, create_at):
        txn_raw = {
            "asset": asset, "from": from_addr, "to": to_addr,
            "amount": str(amount), "from_sequence": from_sequence,
            "create_at": create_at
        }
        return txn_raw, hashlib.sha256(json.dumps(txn_raw, separators=(',', ':')).encode()).hexdigest()

    @classmethod
    def build_txn(cls, asset, from_addr, to_addr, amount, from_sequence):
        create_at = int(Arrow.now().timestamp())
        txn_raw, txn_hash = cls.build_txn_hash(asset, from_addr, to_addr, amount, from_sequence, create_at)
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
