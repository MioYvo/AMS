from typing import Optional

from json import dumps as json_dumps

import ujson
from sanic import Blueprint, Request, json
from sqlalchemy.engine import Row
from stellar_sdk import Keypair

from core import ams_crypt, AMSCore
from core.ams_crypt import AMSCrypt
from core.encoder import MyEncoder
from exceptions import AddressNotFound
from app.model import Account, AccountRow

account_v1_bp = Blueprint("account", version=1, url_prefix='account')


@account_v1_bp.get('/<account_address:str>')
async def get_account_by_address(request: Request, account_address: str):
    """This is a simple foo handler

    Now we will add some more details

    openapi:
    ---
    operationId: fooDots
    tags:
      - one
      - two
    parameters:
      - name: account_address
        in: query
        description: How many items to return at one time (max 100)
        required: false
        param:
          integer
        schema:
          type: integer
          format: int32
    responses:
      '200':
        description: Just some dots
    """
    query = "SELECT * FROM Account WHERE address = :completed"
    async with AMSCore.conn() as conn:
        row: Optional[Row] = await conn.fetch_one(query=query, values={"completed": account_address})
    if not row:
        raise AddressNotFound(extra=dict(address=account_address))
    return json(AccountRow.to_json(row), dumps=json_dumps, cls=MyEncoder)


@account_v1_bp.post('/<account_address:str>/asset')
async def create_account_asset(request: Request, account_address: str):
    """

    """
    asset = request.form.get('asset')   # TODO valid asset from request form params
    search_query = "SELECT * FROM Account WHERE address = :address"
    async with AMSCore.conn() as conn:
        row: Optional[Row] = await conn.fetch_one(query=search_query, values={"address": account_address})
    if not row:
        raise AddressNotFound(extra=dict(address=account_address))

    async with AMSCore.conn() as conn:
        async with conn.transaction():
            query = """UPDATE Account
SET
    balances=JSON_ARRAY_APPEND(balances, '$', CAST('{"asset": ":asset", "balance": 0}' AS JSON)),
    sequence=sequence+1
WHERE address=':account_address' AND sequence=:sequence AND JSON_SEARCH(balances, 'all', ':asset') IS NULL"""
            row = await conn.execute(AMSCore.format_query(query, values={
                'asset': asset, 'account_address': account_address, "sequence": row.sequence
            }))

            row: Optional[Row] = await conn.fetch_one(query=search_query, values={"address": account_address})

    return json(AccountRow.to_json(row), dumps=json_dumps, cls=MyEncoder)


@account_v1_bp.get('/<account_address:str>/sequence')
async def account_address_sequence(request: Request, account_address: str):
    """
    """
    query = "SELECT * FROM Account WHERE address = :completed"
    async with AMSCore.conn() as conn:
        row: Optional[Row] = await conn.fetch_one(query=query, values={"completed": account_address})
    if not row:
        raise AddressNotFound(extra=dict(address=account_address))
    return json(
        {
            "sequence": row.sequence,
        }, dumps=ujson.dumps
    )


@account_v1_bp.get('/<account_address:str>/balances')
async def account_address_sequence(request: Request, account_address: str):
    """
    """
    query = "SELECT * FROM Account WHERE address = :completed"
    async with AMSCore.conn() as conn:
        row: Optional[Row] = await conn.fetch_one(query=query, values={"completed": account_address})
    if not row:
        raise AddressNotFound(extra=dict(address=account_address))
    return json(
        {
            "balances": ujson.loads(row.balances),
        }, dumps=ujson.dumps
    )


@account_v1_bp.post('/')
async def create_account(request: Request):
    """

    """
    s_address: Keypair = Keypair.random()

    query = Account.insert()
    values = {
        "address": s_address.public_key,
        "sequence": 0,
        "secret": ams_crypt.aes_encrypt(
            s_address.secret,
            AMSCrypt.account_secret_aes_key(),
            AMSCrypt.account_secret_aes_iv()).decode(),
        "balances": []
    }
    async with AMSCore.conn() as conn:
        await conn.execute(query=query, values=values)
        query = Account.select().where(Account.c.address == values['address'])
        row: Optional[Row] = await conn.fetch_one(query=query)
    if not row:
        raise AddressNotFound(extra=dict(address=values['address']))
    return json(AccountRow.to_json(row), dumps=json_dumps, status=201, cls=MyEncoder)
