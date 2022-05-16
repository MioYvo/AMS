from datetime import timedelta
from decimal import Decimal
from typing import Optional
from json import dumps as json_dumps

from arrow import Arrow
from pymysql import IntegrityError, OperationalError
from sanic import Blueprint, Request, json
from sqlalchemy.engine import Row
from schema import Schema, SchemaError, Use, And, Optional as OptionalSchema
from stellar_sdk import Keypair

from app.model import Transaction, TransactionRow
from config import settings
from core import AMSCore
from core.encoder import MyEncoder
from exceptions import TransactionNotFound, TransactionsBuildFailed, TransactionsExpired, AssetNotTrusted, \
    InsufficientFunds, TransactionsSendFailed, TransactionsSelfTransfer, AddressNotFound

DEM = settings.AMS_DECIMAL

transactions_v1_bp = Blueprint("transactions", version=1, url_prefix='transactions')


@transactions_v1_bp.get('/<tx_hash:str>')
async def get_transaction_by_hash(request: Request, tx_hash: str):
    query = 'SELECT * FROM Transaction WHERE `hash`=:tx_hash'
    async with AMSCore.conn() as conn:
        row: Optional[Row] = await conn.fetch_one(query=query, values=dict(tx_hash=tx_hash))
    if not row:
        raise TransactionNotFound(extra=dict(tx_hash=tx_hash))

    return json(TransactionRow.to_json(row), dumps=json_dumps, cls=MyEncoder)


@transactions_v1_bp.post('/hash')
async def create_transaction_hash(request: Request):
    asset = request.form.get('asset')
    from_addr = request.form.get('from')
    to_addr = request.form.get('to')
    amount = request.form.get('amount')
    from_sequence = request.form.get('from_sequence')
    memo = request.form.get('memo', '')

    if not all([asset, from_addr, to_addr, amount, from_sequence]):
        raise TransactionsBuildFailed(extra={"asset": asset, "from": from_addr, "to": to_addr,
                                             "amount": amount, "from_sequence": from_sequence})
    try:
        amount = Decimal(amount)
    except Exception as e:
        raise TransactionsBuildFailed(extra=dict(amount=amount, e=e))

    txn_hash, txn_raw = AMSCore.build_txn(asset, from_addr, to_addr, amount, from_sequence)

    return json(dict(hash=txn_hash, txn_raw=txn_raw, memo=memo))


def validate_hash(txn_hash: str, asset, from_addr, to_addr, amount, from_sequence, op=None):
    if txn_hash:
        try:
            __txn_hash, create_at = AMSCore.parse_hash(txn_hash)
            _, _txn_hash = AMSCore.build_txn_hash(asset, from_addr, to_addr, amount, from_sequence, create_at, op=op)
        except Exception as e:
            raise TransactionsBuildFailed(extra=dict(error=e))
        else:
            if (Arrow.now() - Arrow.fromtimestamp(create_at)) > timedelta(seconds=settings.TXN_EXPIRED_SECONDS):
                raise TransactionsExpired(extra=dict(txn_datetime=create_at))
            if __txn_hash != _txn_hash:
                raise TransactionsBuildFailed(extra=dict(txn_hash="not valid raw data"))
    else:
        txn_hash, txn_raw = AMSCore.build_txn(asset, from_addr, to_addr, amount, from_sequence, op=op)
        create_at = txn_raw['create_at']

    return txn_hash, create_at


@transactions_v1_bp.post('/')
async def create_transaction(request: Request):
    asset = request.form.get('asset')
    from_addr = request.form.get('from')
    to_addr = request.form.get('to')
    amount = Decimal(request.form.get('amount'))
    from_sequence = request.form.get('from_sequence')
    memo = request.form.get('memo', '')
    txn_hash = request.form.get('hash', '')

    if from_addr == to_addr:
        raise TransactionsSelfTransfer()

    txn_hash, create_at = validate_hash(txn_hash, asset, from_addr, to_addr, amount, from_sequence)

    async with AMSCore.conn() as conn:
        query_asset = f"SELECT JSON_SEARCH(balances, 'one', :asset) as asset FROM Account where `address`=:addr;"
        asset_row: Optional[Row] = await conn.fetch_one(query_asset, values={"asset": asset, "addr": from_addr})
        if not asset_row:
            raise AddressNotFound(extra=dict(address=from_addr))
        if not asset_row.asset:
            raise AssetNotTrusted(extra=dict(asset=asset, addr=from_addr))

        from_asset_pos = asset_row.asset.strip('"').rsplit('.asset')[0]

        from_asset_balance_query = f"""SELECT * FROM Account 
WHERE address='{from_addr}' 
AND cast(balances->>"{from_asset_pos}.balance" AS {DEM}) - CAST('{amount}' AS {DEM} ) >= 0;"""
        from_asset_balance_row: Optional[Row] = await conn.fetch_one(from_asset_balance_query)
        if not from_asset_balance_row:
            raise InsufficientFunds(extra=dict(amount=amount, addr=from_addr))

        to_asset_row: Optional[Row] = await conn.fetch_one(query_asset, values={"asset": asset, "addr": to_addr})
        if not to_asset_row:
            raise AddressNotFound(extra=dict(address=to_addr))
        if not to_asset_row.asset:
            raise AssetNotTrusted(extra=dict(asset=asset, addr=to_addr))

        to_asset_pos = to_asset_row.asset.strip('"').rsplit('.asset')[0]

        async with conn.transaction():
            cost_query = f"""UPDATE Account
SET
    balances=JSON_REPLACE(balances, 
    '{from_asset_pos}.balance', 
    CAST(CAST(balances->>"{from_asset_pos}.balance" AS {DEM}) - CAST('{amount}' AS {DEM}) AS CHAR )),
    `sequence`=`sequence`+1
WHERE address='{from_addr}' 
AND CAST(balances->>"{from_asset_pos}.balance" AS {DEM}) - CAST('{amount}' AS {DEM}) >= 0 
AND `sequence`={from_sequence};"""

            add_query = f"""UPDATE Account
SET
    balances=JSON_REPLACE(
        balances, 
        '{to_asset_pos}.balance', 
        CAST(CAST(balances->>"{to_asset_pos}.balance" AS {DEM}) + CAST('{amount}' AS {DEM}) AS CHAR ))
WHERE address='{to_addr}'"""

            txn_insert_query = Transaction.insert()
            cost_row = await conn.execute(cost_query)
            if not cost_row:
                raise TransactionsSendFailed(extra=dict(sequence=from_sequence))
            add_row = await conn.execute(add_query)
            if not add_row:
                raise TransactionsSendFailed(extra=dict(to=to_addr))
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
                    "created_at": Arrow.fromtimestamp(create_at).to('utc').datetime
                })
            except IntegrityError as e:
                raise TransactionsSendFailed(extra=dict(e=e))

            if not insert_row:
                raise TransactionsSendFailed(extra=dict(txn=txn_hash))
            # assert cost_row and add_row and insert_row
        select_txn = Transaction.select().where(Transaction.c.hash == txn_hash)
        txn_row = await conn.fetch_one(select_txn)
        return json(TransactionRow.to_json(txn_row), dumps=json_dumps, cls=MyEncoder)


bulk_create_transaction_hash_schema = Schema({
    "op": [{
        "from": And(str, Keypair.from_public_key),
        "to": And(str, Keypair.from_public_key),
        "asset": str,
        "amount": And(str, Decimal.__call__),
    }],
    "from": Keypair.from_public_key,
    "from_sequence": And(Use(int), lambda n: n >= 0),
    OptionalSchema("hash", default=''): And(str, lambda n: len(n) == 74, AMSCore.parse_hash),
    OptionalSchema('memo', default=''): And(str, lambda n: len(n) <= 64),
})


@transactions_v1_bp.post('/bulk/hash')
async def bulk_create_transaction_hash(request: Request):
    try:
        d = bulk_create_transaction_hash_schema.validate(request.json)
    except SchemaError as e:
        raise TransactionsBuildFailed(extra=dict(schema=str(e)))
    else:
        op = d['op']
        from_addr = d['from']
        from_sequence = d['from_sequence']
        memo = d['memo']

    txn_hash, txn_raw = AMSCore.build_txn(
        asset='', from_addr=from_addr, to_addr=settings.AMS_BULK_FAKE_TO_ADDR, amount=Decimal('0'),
        from_sequence=from_sequence, op=op
    )
    return json(dict(hash=txn_hash, txn_raw=txn_raw, memo=memo))


@transactions_v1_bp.post('/bulk')
async def bulk_create_transaction(request: Request):
    try:
        d = bulk_create_transaction_hash_schema.validate(request.json)
    except SchemaError as e:
        raise TransactionsBuildFailed(extra=dict(schema=str(e)))
    else:
        op = d['op']
        from_addr = d['from']
        from_sequence = d['from_sequence']
        memo = d['memo']
        txn_hash, create_at = validate_hash(
            txn_hash=d['hash'], asset='', from_addr=from_addr, to_addr=None, amount=Decimal('0'),
            from_sequence=from_sequence, op=op
        )

        # validate all from address
        # op: [{"from": "ABC", "to": "CBA", "asset": "USDT", "amount": "1.23"}, ...]

#         find_asset_pos_sql = """SELECT
# address,CONCAT_WS('.', SUBSTRING_INDEX(JSON_SEARCH(`balances`, 'one', ':asset'), '.', 1), 'balance'), sequence
# FROM Account
# WHERE `address` in (:address)
#         """
#         AMSCore.format_query(find_asset_pos_sql, values=dict(address=all_from.keys()))
#         find_asset_pos_sql.format()
        # validate trusted asset

        async with AMSCore.conn() as conn:
            async with conn.transaction():
                for _op in op:
                    if _op['from'] == _op['to']:
                        raise TransactionsSelfTransfer()

                    cost_query = f"""UPDATE Account
    SET
        balances=JSON_REPLACE(
            balances,
            CONCAT_WS('.', SUBSTRING_INDEX(JSON_UNQUOTE(JSON_SEARCH(`balances`, 'one', '{_op["asset"]}')), '.', 1), 'balance'),
            CAST(
                CAST(
                    JSON_UNQUOTE( JSON_EXTRACT(`balances`, CONCAT_WS('.', SUBSTRING_INDEX(JSON_UNQUOTE(JSON_SEARCH(`balances`, 'one', '{_op["asset"]}')), '.', 1), 'balance')))
                    AS {DEM}
                ) -
                CAST( '{_op["amount"]}' AS {DEM} )
                AS CHAR
            )
        ),
        `sequence`=`sequence`+1
    WHERE address='{_op["from"]}'
    AND CAST(
            JSON_UNQUOTE( JSON_EXTRACT(`balances`, CONCAT_WS('.', SUBSTRING_INDEX(JSON_UNQUOTE(JSON_SEARCH(`balances`, 'one', '{_op["asset"]}')), '.', 1), 'balance')))
            AS {DEM}
        ) -
        CAST( '{_op["amount"]}' AS {DEM} ) >= 0"""
        #     AND `sequence`=2;"""

                    add_query = f"""UPDATE Account
SET
    balances=JSON_REPLACE(
        balances,
        CONCAT_WS('.', SUBSTRING_INDEX(JSON_UNQUOTE(JSON_SEARCH(`balances`, 'one', '{_op["asset"]}')), '.', 1), 'balance'),
        CAST(
            CAST(
                JSON_UNQUOTE( JSON_EXTRACT(`balances`, CONCAT_WS('.', SUBSTRING_INDEX(JSON_UNQUOTE(JSON_SEARCH(`balances`, 'one', '{_op["asset"]}')), '.', 1), 'balance')))
                AS {DEM}
            ) +
            CAST(
                '{_op["amount"]}'
                AS {DEM}
            )
            AS CHAR
        )
    )
WHERE address='{_op["to"]}'"""

                    try:
                        cost_row = await conn.execute(cost_query)
                        if not cost_row:
                            raise TransactionsSendFailed(extra={"from": _op['from']})
                        add_row = await conn.execute(add_query)
                        if not add_row:
                            raise TransactionsSendFailed(extra=dict(to=_op['to']))
                    except OperationalError as e:
                        if len(e.args) >= 2 and e.args[0] == 3143:
                            raise AssetNotTrusted(extra=dict(op=_op, addr='', asset=_op['asset']))
                        raise TransactionsSendFailed(extra=dict(e=e))

                txn_insert_query = Transaction.insert()
                try:
                    insert_row = await conn.execute(txn_insert_query, values={
                        "hash": txn_hash,
                        "asset": '',
                        "from": from_addr,
                        "to": None,
                        "amount": Decimal('0'),
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

            # after db transaction
            select_txn = Transaction.select().where(Transaction.c.hash == txn_hash)
            txn_row = await conn.fetch_one(select_txn)
        return json(TransactionRow.to_json(txn_row), dumps=json_dumps, cls=MyEncoder)


