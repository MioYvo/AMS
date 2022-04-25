from typing import Optional

from json import dumps as json_dumps
from sanic import Blueprint, Request, json
from sqlalchemy.engine import Row
from stellar_sdk import Keypair

from core import ams_crypt, AMSCore
from core.ams_crypt import AMSCrypt
from core.encoder import MyEncoder
from exceptions import AddressNotFound
from app.model import Account, dict_row

account_v1_bp = Blueprint("account", version=1, url_prefix='account')


@account_v1_bp.get('/<account_address:str>')
async def account_address(request: Request, account_address: str):
    query = "SELECT * FROM Account WHERE address = :completed"
    async with AMSCore.conn() as conn:
        row: Optional[Row] = await conn.fetch_one(query=query, values={"completed": account_address})
    if not row:
        raise AddressNotFound(extra=dict(address=account_address))
    return json(dict_row(row), dumps=json_dumps, cls=MyEncoder)


@account_v1_bp.post('/')
async def account_address(request: Request):
    s_address: Keypair = Keypair.random()

    query = Account.insert()
    values = {
        "address": s_address.public_key,
        "sequence": 0,
        "secret": ams_crypt.aes_encrypt(
            s_address.secret,
            AMSCrypt.account_secret_aes_key(),
            AMSCrypt.account_secret_aes_iv()).decode(),
        "balances": {}
    }
    async with AMSCore.conn() as conn:
        await conn.execute(query=query, values=values)
        query = Account.select().where(Account.c.address == values['address'])
        row: Optional[Row] = await conn.fetch_one(query=query)
    if not row:
        raise AddressNotFound(extra=dict(address=account_address))
    return json(dict_row(row), dumps=json_dumps, status=201, cls=MyEncoder)

