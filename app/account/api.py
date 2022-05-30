from typing import Optional, List

from json import dumps as json_dumps
from enum import Enum, unique

import ujson
from sanic import Blueprint, Request, json
from sanic.exceptions import InvalidUsage
from sanic.log import logger
from schema import Schema, And, SchemaError
from sqlalchemy import select
from sqlalchemy.engine import Row
from stellar_sdk import Keypair

from core import ams_crypt, AMSCore
from core.ams_crypt import AMSCrypt
from core.encoder import MyEncoder
from exceptions import AddressNotFound
from app.model import AccountRow, TransactionRow

accounts_v1_bp = Blueprint("accounts", version=1, url_prefix='accounts')


@accounts_v1_bp.get('/<account_address:str>')
async def get_account_by_address(_: Request, account_address: str):
    """Get account info by address.

    openapi:
    ---
    operationId: get_account_by_address
    tags:
      - account
    """
    async with AMSCore.conn() as conn:
        try:
            Keypair.from_public_key(account_address)
        except Exception:
            raise AddressNotFound(extra=dict(address=account_address))
        else:
            acc_model = await AMSCore.acc_model(account_address, conn=conn)
        select_query = select(acc_model).where(acc_model.c.address == account_address)
        row: Optional[Row] = await conn.fetch_one(query=select_query)
    if not row:
        raise AddressNotFound(extra=dict(address=account_address))
    else:
        await AMSCore.validate_acc_row(row)

    return json(AccountRow.to_json(row), dumps=json_dumps, cls=MyEncoder)


@accounts_v1_bp.post('/')
async def create_account(_: Request):
    """

    """
    s_address: Keypair = Keypair.random()
    async with AMSCore.conn() as conn:
        acc_model = await AMSCore.acc_model(s_address.public_key, conn=conn)
        query = acc_model.insert()
        values = {
            "address": s_address.public_key,
            "sequence": 0,
            "secret": ams_crypt.aes_encrypt(
                s_address.secret,
                AMSCrypt.account_secret_aes_key(),
                AMSCrypt.account_secret_aes_iv()).decode(),
            "balances": [],
            "mnemonic": s_address.generate_mnemonic_phrase(),
            "transactions": [],
        }
        _, values['hash'], _ = AMSCore.build_acc_hash(**values)
        await conn.execute(query=query, values=values)
        select_query = select([
            acc_model.c.address, acc_model.c.sequence, acc_model.c.balances, acc_model.c.mnemonic,
            acc_model.c.secret,
            acc_model.c.created_at, acc_model.c.updated_at
        ]).where(acc_model.c.address == values['address'])
        # query = acc_model.select().where(acc_model.c.address == values['address'])
        row: Optional[Row] = await conn.fetch_one(query=select_query)
    if not row:
        raise AddressNotFound(extra=dict(address=values['address']))
    return json(AccountRow.to_json(row, secret=True, decrypt_secret=True, mnemonic=True),
                dumps=json_dumps, status=201, cls=MyEncoder)


@accounts_v1_bp.post('/<account_address:str>/asset')
async def create_account_asset(request: Request, account_address: str):
    """
    信任资产
    """
    asset_str: str = request.form.get('asset')  # TODO valid asset from request form params
    asset_list: List[str] = [a.strip() for a in asset_str.split(',')]
    async with AMSCore.conn() as conn:
        acc_model = await AMSCore.acc_model(account_address, conn=conn)
        select_query = select(acc_model).where(acc_model.c.address == account_address)
        # search_query = f"SELECT * FROM {acc_model.name} WHERE address = '{account_address}'"
        row: Optional[Row] = await conn.fetch_one(query=select_query)

        if not row:
            raise AddressNotFound(extra=dict(address=account_address))
        else:
            await AMSCore.validate_acc_row(row)

        sequence: int = row.sequence

        async with conn.transaction():
            for asset in asset_list:

                query = """UPDATE :account_name
    SET
        balances=JSON_ARRAY_APPEND(balances, '$', CAST('{"asset": ":asset", "balance": "0.0000000"}' AS JSON)),
        sequence=sequence+1
    WHERE address=':account_address' AND sequence=:sequence AND JSON_SEARCH(balances, 'all', ':asset') IS NULL"""
                rst = await conn.execute(AMSCore.format_query(query, values={
                    'account_name': acc_model.name,
                    'asset': asset, 'account_address': account_address, "sequence": sequence
                }))
                if rst:
                    sequence += 1
            # update hash
            await AMSCore.acc_rehash(conn=conn, model=acc_model, address=account_address)
        # fetch rst
        row: Optional[Row] = await conn.fetch_one(
            query=select(acc_model).where(acc_model.c.address == account_address))

    return json(AccountRow.to_json(row), dumps=json_dumps, cls=MyEncoder)


@accounts_v1_bp.get('/<account_address:str>/sequence')
async def account_address_sequence(_: Request, account_address: str):
    """
    """
    async with AMSCore.conn() as conn:
        acc_model = await AMSCore.acc_model(account_address, conn=conn)
        select_query = select(acc_model).where(acc_model.c.address == account_address)
        row: Optional[Row] = await conn.fetch_one(query=select_query)
    if not row:
        raise AddressNotFound(extra=dict(address=account_address))
    else:
        await AMSCore.validate_acc_row(row)

    return json(
        {
            "sequence": row.sequence,
        }, dumps=ujson.dumps
    )


@accounts_v1_bp.get('/<account_address:str>/balances')
async def account_address_sequence(_: Request, account_address: str):
    """
    """
    async with AMSCore.conn() as conn:
        acc_model = await AMSCore.acc_model(account_address, conn=conn)
        search_query = select(acc_model).where(acc_model.c.address == account_address)
        row: Optional[Row] = await conn.fetch_one(query=search_query)
    if not row:
        raise AddressNotFound(extra=dict(address=account_address))
    else:
        await AMSCore.validate_acc_row(row)
    return json(
        {
            "balances": ujson.loads(row.balances),
        }, dumps=ujson.dumps
    )


@unique
class Order(str, Enum):
    ASC = "ASC"
    DESC = "DESC"


address_schema = Schema(And(str, Keypair.from_public_key))


@accounts_v1_bp.get('/<account_address:str>/transactions')
async def account_address_transactions(request: Request, account_address: str):
    # TODO assert txn hash to account json `transactions`
    try:
        address_schema.validate(account_address)
    except SchemaError:
        raise AddressNotFound(extra=dict(address=account_address))

    limit = int(request.args.get('limit', 30))
    cursor = request.args.get('cursor', None)
    try:
        order = getattr(Order, request.args.get('order', 'DESC'))
    except AttributeError:
        # return json([])
        raise InvalidUsage(message=f"Wrong args <order>: {request.args.get('order')}")

    async with AMSCore.conn() as conn:
        acc_model = await AMSCore.acc_model(account_address, conn=conn)
        select_acc = select(acc_model).where(acc_model.c.address == account_address)
        account_txn_row = await conn.fetch_one(select_acc)
        if not account_txn_row:
            raise AddressNotFound(extra=dict(address=account_address))
        else:
            await AMSCore.validate_acc_row(account_txn_row)

        txn_s = account_txn_row.transactions
        if not txn_s:
            return json([])

        if order is Order.DESC:
            txn_s = reversed(txn_s)  # list_reverseiterator

        rows = []
        hit_cursor = False
        for txn in txn_s:
            if cursor:
                if not hit_cursor:
                    if txn == cursor:
                        hit_cursor = True
                        continue  # not include cursor
                    else:
                        continue

            txn_model = await AMSCore.txn_model(txn, conn=conn)
            txn_row = await conn.fetch_one(select(txn_model).where(txn_model.c.hash == txn))
            if txn_row:
                rows.append(txn_row)
            else:
                logger.error(f"{txn_model} {txn} of Account {account_address} NOT FOUND")
            if len(rows) >= limit:
                break

    return json(
        [TransactionRow.to_json(row, replace_id_with_hash=True) for row in rows],
        dumps=json_dumps, cls=MyEncoder
    )
