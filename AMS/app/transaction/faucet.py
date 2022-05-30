from decimal import Decimal
from typing import Optional

from arrow import Arrow
from databases.core import Connection
from sanic import Request, json, Blueprint
from sanic.views import HTTPMethodView
from schema import Schema, And, Use, SchemaError
from sqlalchemy import Table, select
from sqlalchemy.engine import Row
from stellar_sdk import Keypair
from pymysql import IntegrityError
from json import dumps as json_dumps

from AMS.app.model import Account, TransactionRow
from AMS.config import settings
from AMS.core import AMSCore, MyEncoder
from AMS.exceptions import TransactionsBuildFailed, AddressNotFound, AssetNotTrusted, TransactionsSendFailed

DEM = settings.AMS_DECIMAL
transactions_faucet_v1_bp = Blueprint("faucet", version=1, url_prefix='faucet')


class FaucetCreateTxn(HTTPMethodView):
    from_acc_model_table_name = "Account__1"

    @property
    def schema(self):
        return Schema({
            "to": And(Use(lambda x: x[0]), str, Keypair.from_public_key),
            "asset": And(Use(lambda x: x[0]), str),
            "amount": And(Use(lambda x: x[0]), str, Use(Decimal.__call__), Use(lambda x: x.normalize()), lambda x: x > 0 and x.as_tuple()[2] >= -7),
        })

    def validate_request(self, request, from_sequence):
        try:
            d = self.schema.validate(dict(request.form))
        except SchemaError as e:
            raise TransactionsBuildFailed(extra=dict(schema=str(e)))

        from_addr = settings.AMS_FINANCE_ADDR
        asset: str = d['asset']
        to_addr: str = d['to']
        amount: Decimal = d['amount']
        memo: str = 'faucet'

        txn_hash, create_at = AMSCore.validate_hash(txn_hash='', asset=asset, from_addr=from_addr,
                                                    to_addr=to_addr, amount=amount, from_sequence=from_sequence)

        return txn_hash, asset, from_addr, to_addr, amount, create_at, memo

    @staticmethod
    async def validate_account(to_addr: str, conn: Connection, asset: str):
        to_acc_model = await AMSCore.acc_model(to_addr, conn=conn)

        query_asset = f"SELECT JSON_SEARCH(balances, 'one', ':asset') as asset " \
                      f"FROM :table where `address`=':addr';"

        to_asset_row: Optional[Row] = await conn.fetch_one(
            AMSCore.format_query(query_asset, values={"table": to_acc_model.name, "asset": asset, "addr": to_addr}))
        if not to_asset_row:
            raise AddressNotFound(extra=dict(address=to_addr))
        if not to_asset_row.asset:
            raise AssetNotTrusted(extra=dict(asset=asset, addr=to_addr))
        else:
            to_asset_pos: str = to_asset_row.asset.strip('"').rsplit('.asset')[0]

        await AMSCore.validate_acc(conn=conn, address=to_addr, model=to_acc_model)

        return to_acc_model, to_asset_pos

    @staticmethod
    async def transaction(from_addr: str,
                          from_acc_model: Table,
                          from_sequence: int,
                          conn: Connection,
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
            AND `sequence`={from_sequence};"""
        cost_row = await conn.execute(cost_query)
        if not cost_row:
            raise TransactionsSendFailed(extra=dict(sequence=from_sequence))

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
        add_row = await conn.execute(add_query)
        if not add_row:
            raise TransactionsSendFailed(extra=dict(to=to_addr))

        await AMSCore.acc_rehash(conn=conn, model=to_acc_model, address=to_addr)
        try:
            insert_row = await conn.execute(txn_insert_query, values={
                "hash": txn_hash,
                "asset": asset,
                "from": from_addr,
                "to": to_addr,
                "amount": amount,
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
        async with AMSCore.conn() as conn:
            await AMSCore.check_tables(self.from_acc_model_table_name, conn=conn, model=Account)
            from_acc_model = AMSCore.model_mapping[self.from_acc_model_table_name]
            from_sequence = (await conn.fetch_one(select(from_acc_model.c.sequence).where(
                from_acc_model.c.address == settings.AMS_FINANCE_ADDR))).sequence
            (txn_hash, asset, from_addr,
             to_addr, amount, create_at, memo) = self.validate_request(request, from_sequence)
            to_acc_model, to_asset_pos = await self.validate_account(
                to_addr, conn, asset)
            async with conn.transaction():
                transaction_model = await self.transaction(
                    from_addr=from_addr, from_acc_model=from_acc_model, from_sequence=from_sequence,
                    conn=conn,
                    to_addr=to_addr, to_acc_model=to_acc_model, to_asset_pos=to_asset_pos,
                    amount=amount, txn_hash=txn_hash, asset=asset, memo=memo, create_at=create_at
                )
            txn_row = await conn.fetch_one(select(transaction_model).where(transaction_model.c.hash == txn_hash))
            return json(TransactionRow.to_json(txn_row), dumps=json_dumps, cls=MyEncoder)


transactions_faucet_v1_bp.add_route(FaucetCreateTxn.as_view(), '/')
