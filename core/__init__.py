import hashlib
import json
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from typing import Optional, Type

from arrow import Arrow
from databases import Database
from databases.core import Connection
from sanic import Sanic
from sanic.exceptions import SanicException
from sanic.log import logger
from sqlalchemy import Table, select, update
from sqlalchemy.engine import Row
from sqlalchemy.sql.ddl import CreateTable, CreateIndex
from stellar_sdk import Keypair

from app.model import Transaction, Account
from app.telegram import send_msg, AMSWarningLevel
from config import settings
from core.encoder import MyEncoder
from exceptions import TransactionsBuildFailed, TransactionsExpired, InvalidTransaction, InvalidAccount


class AMSCoreClass:
    index = [5, 0, 1, 8, 4, 6, 2, 3, 9, 7]
    hash_index = [7, 12, 13, 16, 21, 26, 28, 34, 61, 63]
    new_hash_index = [7, 13, 15, 19, 25, 31, 34, 41, 69, 72]
    acc_hash_split_index = 20

    def __init__(self):
        self.TABLE_SPLIT_FMT = '%Y_%m'
        self.model_mapping = {}
        self.acc_table_num = 5

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
            "amount": str(amount.normalize()) if isinstance(amount, Decimal) else amount,
            "from_sequence": int(from_sequence),
            "create_at": create_at
        }
        if op:
            txn_raw['op'] = op
        return txn_raw, hashlib.sha256(json.dumps(txn_raw, separators=(',', ':'), cls=MyEncoder).encode()).hexdigest()

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

    @classmethod
    def build_acc_hash(cls, address: str, sequence: int, secret: str, balances: list, mnemonic: str,
                       transactions: list):
        acc_raw = {
            "address": address, "sequence": sequence, "secret": secret,
            "balances": balances, "mnemonic": mnemonic,
            "transactions": transactions,
        }
        origin_hash: str = hashlib.blake2s(json.dumps(acc_raw, separators=(',', ':')).encode()).hexdigest()

        _hash = origin_hash[-(len(origin_hash) - cls.acc_hash_split_index):] + origin_hash[:cls.acc_hash_split_index]
        return acc_raw, _hash, origin_hash

    @classmethod
    def parse_acc_hash(cls, acc_hash: str) -> str:
        return acc_hash[-cls.acc_hash_split_index:] + acc_hash[:len(acc_hash) - cls.acc_hash_split_index]

    @classmethod
    async def validate_acc_row(cls, row: Row):
        try:
            cls.validate_acc_hash(
                acc_hash=row.hash, addr=row.address, sequence=row.sequence,
                secret=row.secret, balances=row.balances, mnemonic=row.mnemonic,
                transactions=row.transactions
            )
        except InvalidAccount as e:
            await send_msg(f"Account: {row.hash}", level=AMSWarningLevel.invalid_account)
            raise e

    @classmethod
    async def validate_acc(cls, conn: Connection, address: str, model: Table):
        row: Optional[Row] = await conn.fetch_one(select(model).where(model.c.address == address))
        await cls.validate_acc_row(row)

    @classmethod
    def build_acc_hash_raw(cls, row: Row) -> str:
        return cls.build_acc_hash(
            address=row.address, sequence=row.sequence, secret=row.secret,
            balances=row.balances, mnemonic=row.mnemonic, transactions=row.transactions
        )[1]

    @classmethod
    async def acc_rehash(cls, conn: Connection, model: Table, address: str):
        row: Optional[Row] = await conn.fetch_one(query=select(model).where(model.c.address == address))
        await conn.execute(
            update(model).
            where(model.c.address == address).
            values(hash=cls.build_acc_hash_raw(row))
        )

    @classmethod
    def validate_acc_hash(cls, acc_hash: str, addr: str, sequence: int, secret: str, balances: list, mnemonic: str,
                          transactions: list):
        _target_origin_hash = cls.parse_acc_hash(acc_hash)
        _, _, _built_origin_hash = cls.build_acc_hash(addr, sequence, secret, balances, mnemonic, transactions,
                                                      )
        if _target_origin_hash != _built_origin_hash:
            raise InvalidAccount(extra=dict(addr=addr))

    @classmethod
    async def validate_txn_row(cls, row: Row):
        try:
            AMSCore.validate_hash(
                txn_hash=row.hash, asset=row.asset,
                from_addr=getattr(row, 'from'), to_addr=row.to,
                amount=row.amount, from_sequence=row.from_sequence, op=row.op,
                exception=InvalidTransaction, raise_expire=False
            )
        except InvalidTransaction as e:
            await send_msg(f"Transaction: {row.hash}", level=AMSWarningLevel.invalid_transaction)
            raise e

    @classmethod
    def validate_hash(cls, txn_hash: str, asset, from_addr, to_addr, amount, from_sequence, op=None,
                      exception: Type[SanicException] = TransactionsBuildFailed, raise_expire=True):
        if txn_hash:
            try:
                __txn_hash, create_at = AMSCore.parse_hash(txn_hash)
                _, _txn_hash = AMSCore.build_txn_hash(asset, from_addr, to_addr, amount, from_sequence, create_at,
                                                      op=op)
            except Exception as e:
                raise exception(extra=dict(error=e))
            else:
                if (Arrow.now() - Arrow.fromtimestamp(create_at)) > timedelta(seconds=settings.TXN_EXPIRED_SECONDS):
                    if raise_expire:
                        raise TransactionsExpired(extra=dict(txn_datetime=create_at))
                if __txn_hash != _txn_hash:
                    raise exception(extra=dict(txn_hash=f"not valid raw data {__txn_hash=} {_txn_hash=}"))
        else:
            txn_hash, txn_raw = AMSCore.build_txn(asset, from_addr, to_addr, amount, from_sequence, op=op)
            create_at = txn_raw['create_at']

        return txn_hash, create_at

    def get_model(self, table_name):
        return self.model_mapping.get(table_name)

    async def get_acc_model(self, address: str, conn: Connection) -> Table:
        return await self.acc_model(address, conn)

    async def check_tables(self, table_name: str, conn: Connection, model: Table):
        row: Optional[Row] = await conn.fetch_one(f"SHOW tables like '{table_name}';")
        if not row:
            new_model = deepcopy(model)
            new_model.name = table_name
            logger.info(f"Create Table: {CreateTable(new_model, if_not_exists=True)}")
            await conn.execute(CreateTable(new_model, if_not_exists=True))
            if new_model.indexes:
                for index in new_model.indexes:
                    logger.info(f"Create Index: {CreateIndex(index)}")
                    await conn.execute(CreateIndex(index))
            self.model_mapping[table_name] = new_model
        else:
            if self.model_mapping.get(table_name) is None:
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

    async def txn_model(self, txn_hash: str, conn: Connection) -> Table:
        table_name, local_dt = self.__txn_table_name(txn_hash=txn_hash)
        if self.model_mapping.get(table_name) is None:
            await self.check_tables(table_name=table_name, conn=conn, model=Transaction)
        return self.model_mapping.get(table_name)

    async def acc_model(self, address: str, conn: Connection) -> Table:
        assert Keypair.from_public_key(address)
        table_no = int(hashlib.blake2s(address.encode()).hexdigest(), 16) % self.acc_table_num + 1  # starts from 1
        table_name = f"{self.origin_table_name(Account)}__{table_no}"
        if self.model_mapping.get(table_name) is None:
            await self.check_tables(table_name=table_name, conn=conn, model=Account)
        return self.model_mapping.get(table_name)


AMSCore = AMSCoreClass()
