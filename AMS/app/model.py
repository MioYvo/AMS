import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict

import sqlalchemy
from arrow import Arrow
from dateutil import tz
from sqlalchemy import text, UniqueConstraint
from sqlalchemy.engine import Row

from AMS.core.ams_crypt import AMSCrypt, aes_decrypt

metadata = sqlalchemy.MetaData()


@dataclass
class AccountBalance:
    balance: Decimal
    asset: str

    @property
    def json(self):
        return dict(balance=self.balance, asset=self.asset)


Account = sqlalchemy.Table(
    "Account",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.BigInteger, primary_key=True),
    sqlalchemy.Column("sequence", sqlalchemy.BigInteger, default=0, server_default=text("0"), nullable=False),
    sqlalchemy.Column("address", sqlalchemy.String(length=56), nullable=False),
    sqlalchemy.Column("secret", sqlalchemy.String(length=100), nullable=False),
    sqlalchemy.Column("balances", sqlalchemy.JSON(), default=[]),
    sqlalchemy.Column('mnemonic', sqlalchemy.String(length=128), nullable=True),
    sqlalchemy.Column('transactions', sqlalchemy.JSON()),
    sqlalchemy.Column('hash', sqlalchemy.String(length=64)),
    sqlalchemy.Column(
        'created_at', sqlalchemy.TIMESTAMP(),
        server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
    ),
    sqlalchemy.Column(
        'updated_at', sqlalchemy.TIMESTAMP(),
        server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
        server_onupdate=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
    ),
    # Index("Account_address_uindex", "address", unique=True)
    UniqueConstraint('address', name='Account_address_uindex')
)

# Account_address_uindex = Index('Account_address_uindex', Account.c.address, unique=True)


Transaction = sqlalchemy.Table(
    "Transaction",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.BigInteger, primary_key=True),
    sqlalchemy.Column("hash", sqlalchemy.String(length=74), nullable=False),
    sqlalchemy.Column("asset", sqlalchemy.String(length=20), nullable=True),
    sqlalchemy.Column("from", sqlalchemy.String(length=56), nullable=False),
    sqlalchemy.Column("to", sqlalchemy.String(length=56), nullable=True, index=True),
    sqlalchemy.Column("is_bulk", sqlalchemy.Boolean, default=False, nullable=False),
    sqlalchemy.Column("op", sqlalchemy.JSON(), default=None, nullable=True),
    sqlalchemy.Column("amount", sqlalchemy.Numeric(precision=23, scale=7), nullable=True),
    sqlalchemy.Column("from_sequence", sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column("is_success", sqlalchemy.Boolean, nullable=False),
    sqlalchemy.Column("memo", sqlalchemy.String(length=64), nullable=True),
    sqlalchemy.Column(
        'created_at', sqlalchemy.TIMESTAMP(),
        server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
    ),
    sqlalchemy.Column(
        'updated_at', sqlalchemy.TIMESTAMP(),
        server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
        server_onupdate=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
    ),
    # Constraint('to', name="Transaction_to_index", ),
    UniqueConstraint('hash', name='Transaction_hash_uindex'),
    UniqueConstraint('from', 'from_sequence', name='Transaction_from_from_sequence_uindex'),
)

# Transaction_from_from_sequence_uindex= Index('Transaction_from_from_sequence_uindex', "Transaction.from",
#                                              Transaction.c.from_sequence, unique=True),
# Transaction_hash_uindex = Index('Transaction_hash_uindex', 'Transaction.hash', unique=True),


def dict_row(row: Row) -> dict:
    d_row = dict(row)
    d_row['created_at'] = Arrow.fromdatetime(d_row['created_at'], tzinfo=tz.tzutc()).to(tz.gettz())
    d_row['updated_at'] = Arrow.fromdatetime(d_row['updated_at'], tzinfo=tz.tzutc()).to(tz.gettz())
    d_row['balances'] = json.loads(d_row['balances'])
    d_row.pop('secret', None)
    return d_row


class AccountRow:
    @classmethod
    def to_json(cls, row: Row, secret=False, decrypt_secret=False, mnemonic=False, transactions=False, hash_=False):
        d_row: Dict[str, Arrow | int | str | datetime] = dict(row)
        d_row['created_at_dt'] = Arrow.fromdatetime(d_row['created_at'], tzinfo=tz.tzutc()).to(tz.gettz())
        d_row['created_at'] = int(d_row['created_at_dt'].timestamp())
        d_row['updated_at_dt'] = Arrow.fromdatetime(d_row['updated_at'], tzinfo=tz.tzutc()).to(tz.gettz())
        d_row['updated_at'] = int(d_row['updated_at_dt'].timestamp())

        d_row.pop('id', None)
        if isinstance(d_row['balances'], str):
            d_row['balances'] = json.loads(d_row['balances'])
        if not secret:
            d_row.pop('secret', None)
        else:
            if decrypt_secret:
                d_row['secret'] = aes_decrypt(
                    d_row['secret'],
                    AMSCrypt.account_secret_aes_key(),
                    AMSCrypt.account_secret_aes_iv()
                ).decode()
            # d_row.pop('secret', None)
        if not transactions:
            d_row.pop('transactions', None)
        if not mnemonic:
            d_row.pop('mnemonic', None)
        if not hash_:
            d_row.pop('hash', None)
        return d_row


class TransactionRow:
    @classmethod
    def to_json(cls, row: Row, replace_id_with_hash=False):
        d_row = dict(row)
        d_row.pop('id', None)
        d_row['op'] = json.loads(d_row['op']) if isinstance(d_row['op'], str) else d_row['op']
        d_row['is_bulk'] = bool(d_row['is_bulk'])
        d_row['created_at_dt'] = Arrow.fromdatetime(d_row['created_at'], tzinfo=tz.tzutc()).to(tz.gettz())
        d_row['created_at'] = int(d_row['created_at_dt'].timestamp())
        d_row['updated_at_dt'] = Arrow.fromdatetime(d_row['updated_at'], tzinfo=tz.tzutc()).to(tz.gettz())
        d_row['updated_at'] = int(d_row['updated_at_dt'].timestamp())
        d_row['is_success'] = bool(d_row['is_success'])
        if replace_id_with_hash:
            d_row['id'] = d_row['hash']
        return d_row
