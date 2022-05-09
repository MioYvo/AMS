from datetime import timedelta
from decimal import Decimal
from typing import Optional
from json import dumps as json_dumps

from arrow import Arrow
from pymysql import IntegrityError
from sanic import Blueprint, Request, json
from sqlalchemy.engine import Row

from app.model import Transaction, TransactionRow
from core import AMSCore
from core.encoder import MyEncoder
from exceptions import TransactionNotFound, TransactionsBuildFailed, TransactionsExpired, AssetNotTrusted, \
    InsufficientFunds, TransactionsSendFailed

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
    except:
        raise TransactionsBuildFailed(extra=dict(amount=amount))

    txn_hash, txn_raw = AMSCore.build_txn(asset, from_addr, to_addr, amount, from_sequence)

    return json(dict(hash=txn_hash, txn_raw=txn_raw, memo=memo))


@transactions_v1_bp.post('/')
async def create_transaction_hash(request: Request):
    asset = request.form.get('asset')
    from_addr = request.form.get('from')
    to_addr = request.form.get('to')
    amount = request.form.get('amount')
    from_sequence = request.form.get('from_sequence')
    memo = request.form.get('memo', '')
    txn_hash = request.form.get('hash', '')

    if txn_hash:
        try:
            __txn_hash, create_at = AMSCore.parse_hash(txn_hash)
            _, _txn_hash = AMSCore.build_txn_hash(asset, from_addr, to_addr, amount, from_sequence, create_at)
        except Exception as e:
            raise TransactionsBuildFailed(extra=dict(error=e))
        else:
            if (Arrow.fromtimestamp(create_at) - Arrow.now()) > timedelta(minutes=5):
                raise TransactionsExpired(extra=dict(txn_datetime=create_at))
            if __txn_hash != _txn_hash:
                raise TransactionsBuildFailed(extra=dict(txn_hash="not valid raw data"))
    else:
        txn_hash, txn_raw = AMSCore.build_txn(asset, from_addr, to_addr, amount, from_sequence)
        create_at = txn_raw['create_at']

    if from_addr == to_addr:
        raise TransactionsBuildFailed(extra=dict(error='`from` cannot equal to `to`'))

    async with AMSCore.conn() as conn:
        query_asset = f"SELECT JSON_SEARCH(balances, 'one', :asset) as asset FROM Account where `address`=:addr;"
        asset_row: Optional[Row] = await conn.fetch_one(query_asset, values={"asset": asset, "addr": from_addr})
        if not asset_row.asset:
            raise AssetNotTrusted(extra=dict(asset=asset, addr=from_addr))

        from_asset_pos = asset_row.asset.strip('"').rsplit('.asset')[0]

        from_asset_balance_query = f"""SELECT * FROM Account 
    WHERE address='{from_addr}' AND balances->>"{from_asset_pos}.balance" - {amount} >= 0;"""
        from_asset_balance_row: Optional[Row] = await conn.fetch_one(from_asset_balance_query)
        if not from_asset_balance_row:
            raise InsufficientFunds(extra=dict(amount=amount, addr=from_addr))

        to_asset_row: Optional[Row] = await conn.fetch_one(query_asset, values={"asset": asset, "addr": to_addr})
        if not to_asset_row.asset:
            raise AssetNotTrusted(extra=dict(asset=asset, addr=to_addr))

        to_asset_pos = to_asset_row.asset.strip('"').rsplit('.asset')[0]

        async with conn.transaction():
            cost_query = f"""UPDATE Account
SET
    balances=JSON_REPLACE(balances, '{from_asset_pos}.balance', balances->>"{from_asset_pos}.balance" - {amount}),
    `sequence`=`sequence`+1
WHERE address='{from_addr}' AND balances->>"{from_asset_pos}.balance" - {amount} >= 0 AND `sequence`={from_sequence};"""

            add_query = f"""UPDATE Account
SET
    balances=JSON_REPLACE(balances, '{to_asset_pos}.balance', balances->>"{to_asset_pos}.balance" + {amount})
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
