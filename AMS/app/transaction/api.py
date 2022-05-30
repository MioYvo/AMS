from decimal import Decimal
from typing import Optional, List
from json import dumps as json_dumps

from arrow import Arrow
from databases.core import Connection
from pymysql import IntegrityError, OperationalError
from redis.asyncio import Redis
from redis.exceptions import LockError
from sanic import Blueprint, Request, json
from sanic.views import HTTPMethodView
from sqlalchemy import select, Table
from sqlalchemy.engine import Row
from schema import Schema, SchemaError, Use, And, Optional as OptionalSchema
from stellar_sdk import Keypair

from AMS.app.model import TransactionRow
from AMS.config import settings
from AMS.core import AMSCore
from AMS.core.encoder import MyEncoder
from AMS.exceptions import TransactionNotFound, TransactionsBuildFailed, AssetNotTrusted, \
    InsufficientFunds, TransactionsSendFailed, TransactionsSelfTransfer, AddressNotFound, BulkTransactionsFromAddress, \
    BulkTransactionsLockFailed

DEM = settings.AMS_DECIMAL
lock_name = settings.AMS_BULK_TXN_LOCK_NAME

transactions_v1_bp = Blueprint("transactions", version=1, url_prefix='transactions')


create_txn_schema = Schema({
    "from": And(Use(lambda x: x[0]), str, Keypair.from_public_key),
    "to": And(Use(lambda x: x[0]), str, Keypair.from_public_key),
    "asset": And(Use(lambda x: x[0]), str),
    "amount": And(Use(lambda x: x[0]), str, Use(Decimal.__call__), Use(lambda x: x.normalize()), lambda x: x > 0 and x.as_tuple()[2] >= -7),
    "from_sequence": And(Use(lambda x: x[0]), Use(int), lambda n: n >= 0),
    OptionalSchema("hash", default=''): And(Use(lambda x: x[0]), str, lambda n: len(n) == 74, AMSCore.parse_hash),
    OptionalSchema('memo', default=''): And(Use(lambda x: x[0]), str, lambda n: len(n) <= 64),
})


class CreateTxnView(HTTPMethodView):
    @staticmethod
    def validate_request(request):
        try:
            d = create_txn_schema.validate(dict(request.form))
        except SchemaError as e:
            raise TransactionsBuildFailed(extra=dict(schema=str(e)))

        if d['from'] == d['to']:
            raise TransactionsSelfTransfer()
        txn_hash: str = d['hash']
        asset: str = d['asset']
        from_addr: str = d['from']
        to_addr: str = d['to']
        amount: Decimal = d['amount']
        from_sequence: int = d['from_sequence']
        memo: str = d['memo']

        txn_hash, create_at = AMSCore.validate_hash(txn_hash, asset, from_addr, to_addr, amount, from_sequence)

        return txn_hash, asset, from_addr, to_addr, amount, from_sequence, create_at, memo

    @staticmethod
    async def validate_account(from_addr: str, to_addr: str, conn: Connection, asset: str, amount: Decimal):
        from_acc_model = await AMSCore.acc_model(from_addr, conn=conn)
        to_acc_model = await AMSCore.acc_model(to_addr, conn=conn)
        query_asset = f"SELECT JSON_SEARCH(balances, 'one', ':asset') as asset " \
                      f"FROM :table where `address`=':addr';"
        asset_row: Optional[Row] = await conn.fetch_one(
            AMSCore.format_query(query_asset, values={"table": from_acc_model.name, "asset": asset, "addr": from_addr}))
        if not asset_row:
            raise AddressNotFound(extra=dict(address=from_addr))
        if not asset_row.asset:
            raise AssetNotTrusted(extra=dict(asset=asset, addr=from_addr))

        from_asset_pos: str = asset_row.asset.strip('"').rsplit('.asset')[0]

        from_asset_balance_query = f"""SELECT * FROM {from_acc_model.name} 
        WHERE address='{from_addr}' 
        AND cast(balances->>"{from_asset_pos}.balance" AS {DEM}) - CAST('{amount}' AS {DEM} ) >= 0;"""
        from_asset_balance_row: Optional[Row] = await conn.fetch_one(from_asset_balance_query)
        if not from_asset_balance_row:
            raise InsufficientFunds(extra=dict(amount=amount, addr=from_addr))

        to_asset_row: Optional[Row] = await conn.fetch_one(
            AMSCore.format_query(query_asset, values={"table": to_acc_model.name, "asset": asset, "addr": to_addr}))
        if not to_asset_row:
            raise AddressNotFound(extra=dict(address=to_addr))
        if not to_asset_row.asset:
            raise AssetNotTrusted(extra=dict(asset=asset, addr=to_addr))
        else:
            to_asset_pos: str = to_asset_row.asset.strip('"').rsplit('.asset')[0]

        await AMSCore.validate_acc(conn=conn, address=from_addr, model=from_acc_model)
        await AMSCore.validate_acc(conn=conn, address=to_addr, model=to_acc_model)

        return from_acc_model, from_asset_pos, to_acc_model, to_asset_pos

    @staticmethod
    async def transaction(conn: Connection,
                          from_addr: str,
                          from_acc_model: Table,
                          from_asset_pos: str,
                          from_sequence: int,
                          to_addr: str,
                          to_acc_model: Table,
                          to_asset_pos: str,
                          amount: Decimal,
                          txn_hash: str,
                          asset: str,
                          memo: str,
                          create_at: int):
        cost_query = f"""UPDATE {from_acc_model.name}
    SET
        balances=JSON_REPLACE(balances, 
        '{from_asset_pos}.balance', 
        CAST(CAST(balances->>"{from_asset_pos}.balance" AS {DEM}) - CAST('{amount}' AS {DEM}) AS CHAR )),
        `sequence`=`sequence`+1,
        `transactions`=IF(
            JSON_CONTAINS(`transactions`, CAST('"{txn_hash}"' AS JSON), '$') = 1, 
            `transactions`, 
            IFNULL(
                json_array_append(transactions, '$', CAST('"{txn_hash}"' AS JSON)), 
                CAST('["{txn_hash}"]' AS JSON)
            )
        )
    WHERE address='{from_addr}' 
    AND CAST(balances->>"{from_asset_pos}.balance" AS {DEM}) - CAST('{amount}' AS {DEM}) >= 0 
    AND `sequence`={from_sequence};"""

        add_query = f"""UPDATE {to_acc_model.name}
    SET
        balances=JSON_REPLACE(
            balances, 
            '{to_asset_pos}.balance', 
            CAST(CAST(balances->>"{to_asset_pos}.balance" AS {DEM}) + CAST('{amount}' AS {DEM}) AS CHAR )),
        `transactions`=IF(
            JSON_CONTAINS(`transactions`, CAST('"{txn_hash}"' AS JSON), '$') = 1, 
            `transactions`, 
            IFNULL(
                json_array_append(transactions, '$', CAST('"{txn_hash}"' AS JSON)), 
                CAST('["{txn_hash}"]' AS JSON)
            )
        )
    WHERE address='{to_addr}'"""

        transaction_model = await AMSCore.txn_model(txn_hash=txn_hash, conn=conn)
        txn_insert_query = transaction_model.insert()
        cost_row = await conn.execute(cost_query)
        if not cost_row:
            raise TransactionsSendFailed(extra=dict(sequence=from_sequence))
        add_row = await conn.execute(add_query)
        if not add_row:
            raise TransactionsSendFailed(extra=dict(to=to_addr))

        await AMSCore.acc_rehash(conn=conn, model=from_acc_model, address=from_addr)
        await AMSCore.acc_rehash(conn=conn, model=to_acc_model, address=to_addr)
        try:
            insert_row = await conn.execute(txn_insert_query, values={
                "hash": txn_hash,
                "asset": asset,
                "from": from_addr,
                "to": to_addr,
                "amount": Decimal(amount),
                "from_sequence": from_sequence,
                "is_success": True,
                "memo": memo,
                "is_bulk": False,
                "created_at": Arrow.fromtimestamp(create_at).to('utc').datetime
            })
        except IntegrityError as e:
            raise TransactionsSendFailed(extra=dict(e=e))

        if not insert_row:
            raise TransactionsSendFailed(extra=dict(txn=txn_hash))

        return transaction_model

    async def post(self, request: Request):
        (txn_hash, asset, from_addr, to_addr, amount,
         from_sequence, create_at, memo) = self.validate_request(request)
        async with AMSCore.conn() as conn:
            from_acc_model, from_asset_pos, to_acc_model, to_asset_pos = await self.validate_account(
                from_addr, to_addr, conn, asset, amount)
            async with conn.transaction():
                transaction_model = await self.transaction(
                    conn, from_addr, from_acc_model, from_asset_pos, from_sequence,
                    to_addr, to_acc_model, to_asset_pos,
                    amount, txn_hash, asset, memo, create_at
                )
            txn_row = await conn.fetch_one(select(transaction_model).where(transaction_model.c.hash == txn_hash))
            return json(TransactionRow.to_json(txn_row), dumps=json_dumps, cls=MyEncoder)


bulk_create_transaction_hash_schema = Schema({
    "op": [{
        "from": And(str, Keypair.from_public_key),
        "to": And(str, Keypair.from_public_key),
        "asset": str,
        "amount": And(str, Use(Decimal.__call__), Use(lambda x: x.normalize()), lambda x: x > 0 and x.as_tuple()[2] >= -7),
    }],
    "from": Keypair.from_public_key,
    "from_sequence": And(Use(int), lambda n: n >= 0),
    OptionalSchema("hash", default=''): And(str, lambda n: len(n) == 74, AMSCore.parse_hash),
    OptionalSchema('memo', default=''): And(str, lambda n: len(n) <= 64),
})


class BulkTransactionView(HTTPMethodView):
    @staticmethod
    def valid_request(request: Request):
        try:
            d = bulk_create_transaction_hash_schema.validate(request.json)
        except SchemaError as e:
            raise TransactionsBuildFailed(extra=dict(schema=str(e)))
        else:
            op = d['op']
            from_addr = d['from']
            from_sequence = d['from_sequence']
            memo = d['memo']
            txn_hash, create_at = AMSCore.validate_hash(
                txn_hash=d['hash'], asset=None, from_addr=from_addr, to_addr=None, amount=None,
                from_sequence=from_sequence, op=op
            )

            from_to_set = set()
            for i in op:
                if i['from'] == i['to']:
                    raise TransactionsSelfTransfer(extra=dict(addr=i['from']))
                from_to_set.add(i['from'])
                from_to_set.add(i['to'])
            if from_addr not in from_to_set:
                raise BulkTransactionsFromAddress(extra=dict(from_addr=from_addr))

        return op, from_addr, from_sequence, memo, txn_hash, create_at

    @staticmethod
    async def update_from_op(op_, txn_hash, conn):
        op_from_acc_model = await AMSCore.acc_model(address=op_["from"], conn=conn)
        op_to_acc_model = await AMSCore.acc_model(address=op_["to"], conn=conn)
        await AMSCore.validate_acc(conn=conn, address=op_['from'], model=op_from_acc_model)
        await AMSCore.validate_acc(conn=conn, address=op_['to'], model=op_to_acc_model)
        cost_query = f"""UPDATE {op_from_acc_model.name}
                    SET
                        balances=JSON_REPLACE(
                            balances,
                            CONCAT_WS('.', SUBSTRING_INDEX(JSON_UNQUOTE(JSON_SEARCH(`balances`, 'one', '{op_["asset"]}')), '.', 1), 'balance'),
                            CAST(
                                CAST(
                                    JSON_UNQUOTE( JSON_EXTRACT(`balances`, CONCAT_WS('.', SUBSTRING_INDEX(JSON_UNQUOTE(JSON_SEARCH(`balances`, 'one', '{op_["asset"]}')), '.', 1), 'balance')))
                                    AS {DEM}
                                ) -
                                CAST( '{op_["amount"]}' AS {DEM} )
                                AS CHAR
                            )
                        ),
                        `sequence`=`sequence`+1,
                        `transactions`=IF(
                            JSON_CONTAINS(`transactions`, CAST('"{txn_hash}"' AS JSON), '$') = 1, 
                            `transactions`, 
                            IFNULL(
                                json_array_append(transactions, '$', CAST('"{txn_hash}"' AS JSON)), 
                                CAST('["{txn_hash}"]' AS JSON)
                            )
                        )
                    WHERE address='{op_["from"]}'
                    AND CAST(
                            JSON_UNQUOTE( JSON_EXTRACT(`balances`, CONCAT_WS('.', SUBSTRING_INDEX(JSON_UNQUOTE(JSON_SEARCH(`balances`, 'one', '{op_["asset"]}')), '.', 1), 'balance')))
                            AS {DEM}
                        ) -
                        CAST( '{op_["amount"]}' AS {DEM} ) >= 0"""
        #     AND `sequence`=2;"""

        add_query = f"""UPDATE {op_to_acc_model.name}
                SET
                    balances=JSON_REPLACE(
                        balances,
                        CONCAT_WS('.', SUBSTRING_INDEX(JSON_UNQUOTE(JSON_SEARCH(`balances`, 'one', '{op_["asset"]}')), '.', 1), 'balance'),
                        CAST(
                            CAST(
                                JSON_UNQUOTE( JSON_EXTRACT(`balances`, CONCAT_WS('.', SUBSTRING_INDEX(JSON_UNQUOTE(JSON_SEARCH(`balances`, 'one', '{op_["asset"]}')), '.', 1), 'balance')))
                                AS {DEM}
                            ) +
                            CAST(
                                '{op_["amount"]}'
                                AS {DEM}
                            )
                            AS CHAR
                        )
                    ),
                    `transactions`=IF(
                        JSON_CONTAINS(`transactions`, CAST('"{txn_hash}"' AS JSON), '$') = 1, 
                        `transactions`, 
                        IFNULL(
                            json_array_append(transactions, '$', CAST('"{txn_hash}"' AS JSON)), 
                            CAST('["{txn_hash}"]' AS JSON)
                        )
                    )
                WHERE address='{op_["to"]}'"""

        try:
            cost_row = await conn.execute(cost_query)
            if not cost_row:
                raise TransactionsSendFailed(extra={"from": op_['from'], "e": "cost failed"})
            add_row = await conn.execute(add_query)
            if not add_row:
                raise TransactionsSendFailed(extra=dict(to=op_['to'], e="add failed"))

            await AMSCore.acc_rehash(conn=conn, model=op_from_acc_model, address=op_['from'])
            await AMSCore.acc_rehash(conn=conn, model=op_to_acc_model, address=op_['to'])
        except OperationalError as e:
            if len(e.args) >= 2 and e.args[0] == 3143:
                raise AssetNotTrusted(extra=dict(op=op_, addr='', asset=op_['asset']))
            raise TransactionsSendFailed(extra=dict(e=e))
        op_['amount'] = str(op_['amount'])  # to save in mysql json

    async def bulk_transaction(self,
                               conn: Connection,
                               op: List[dict],
                               transaction_model: Table,
                               txn_hash: str,
                               from_addr: str,
                               from_sequence: int,
                               memo: str,
                               create_at: int,
                               redis: Redis):
        async with conn.transaction():
            for _op in op:
                try:
                    # Lock from_addr
                    async with redis.lock(
                            name=lock_name.format(from_addr=_op["from"]),
                            blocking_timeout=0.2, timeout=100.0):
                        # Do update in op list
                        await self.update_from_op(_op, txn_hash, conn)
                except LockError:
                    raise BulkTransactionsLockFailed(extra=dict(from_addr=_op["from"]))

            # insert transaction
            txn_insert_query = transaction_model.insert()
            try:
                insert_row = await conn.execute(txn_insert_query, values={
                    "hash": txn_hash,
                    "asset": None,
                    "from": from_addr,
                    "to": None,
                    "amount": None,
                    "from_sequence": from_sequence,
                    "is_success": True,
                    "is_bulk": True,
                    "op": op,
                    "memo": memo,
                    "created_at": Arrow.fromtimestamp(create_at).to('utc').datetime
                })
            except IntegrityError as e:
                if len(e.args) >= 2 and e.args[0] == 1062:
                    raise TransactionsSendFailed(extra=dict(sequence=from_sequence, from_addr=from_addr))
                raise TransactionsSendFailed(extra=dict(e=e))

            if not insert_row:
                raise TransactionsSendFailed(extra=dict(txn=txn_hash))
            # End db transaction

    async def bulk_conn(self,
                        txn_hash: str,
                        from_addr: str,
                        from_sequence: int,
                        op: List[dict],
                        memo: str,
                        create_at: int,
                        redis: Redis):
        async with AMSCore.conn() as conn:
            transaction_model = await AMSCore.txn_model(txn_hash=txn_hash, conn=conn)
            acc_model = await AMSCore.acc_model(address=from_addr, conn=conn)
            select_txn = select(acc_model.c.id).where(
                acc_model.c.address == from_addr, acc_model.c.sequence == from_sequence)
            owner_seq_query_row = await conn.fetch_one(select_txn)
            if not owner_seq_query_row:
                raise TransactionsSendFailed(extra=dict(sequence=from_sequence, from_addr=from_addr))
            # Do transaction
            await self.bulk_transaction(
                conn=conn, op=op, transaction_model=transaction_model, txn_hash=txn_hash, from_addr=from_addr,
                from_sequence=from_sequence, memo=memo, create_at=create_at, redis=redis
            )
            # After transaction
            select_txn = transaction_model.select().where(transaction_model.c.hash == txn_hash)
            txn_row = await conn.fetch_one(select_txn)
        return txn_row

    async def post(self, request: Request):
        op, from_addr, from_sequence, memo, txn_hash, create_at = self.valid_request(request)
        txn_row = await self.bulk_conn(txn_hash, from_addr, from_sequence, op, memo, create_at, request.app.ctx.redis)
        return json(TransactionRow.to_json(txn_row), dumps=json_dumps, cls=MyEncoder)


transactions_v1_bp.add_route(BulkTransactionView.as_view(), '/bulk')
transactions_v1_bp.add_route(CreateTxnView.as_view(), '/')



@transactions_v1_bp.get('/<tx_hash:str>')
async def get_transaction_by_hash(_: Request, tx_hash: str):
    async with AMSCore.conn() as conn:
        transaction_model = await AMSCore.txn_model(txn_hash=tx_hash, conn=conn)
        select_txn = transaction_model.select().where(transaction_model.c.hash == tx_hash)
        txn_row = await conn.fetch_one(select_txn)
    if not txn_row:
        raise TransactionNotFound(extra=dict(tx_hash=tx_hash))

    await AMSCore.validate_txn_row(txn_row)

    return json(TransactionRow.to_json(txn_row), dumps=json_dumps, cls=MyEncoder)


@transactions_v1_bp.post('/hash')
async def create_transaction_hash(request: Request):
    txn_hash, asset, from_addr, to_addr, amount, from_sequence, create_at, memo = CreateTxnView.validate_request(request)
    return json(dict(hash=txn_hash, txn_raw={
        "asset": asset, "from": from_addr, "to": to_addr, "amount": str(amount), "from_sequence": from_sequence,
        "memo": memo, "hash": txn_hash
    }))


@transactions_v1_bp.post('/bulk/hash')
async def bulk_create_transaction_hash(request: Request):
    op, from_addr, from_sequence, memo, txn_hash, create_at = BulkTransactionView.valid_request(request)
    return json(dict(hash=txn_hash, txn_raw={
        "from": from_addr,
        "from_sequence": from_sequence,
        "op": op,
        "memo": memo,
        "hash": txn_hash
    }), dumps=json_dumps, cls=MyEncoder)
