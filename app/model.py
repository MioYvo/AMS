import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict, Union

import sqlalchemy
from arrow import Arrow
from dateutil import tz
from sqlalchemy import text, ForeignKey, UniqueConstraint
from sqlalchemy.engine import Row

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
    sqlalchemy.Column("asset", sqlalchemy.String(length=20), nullable=False),
    sqlalchemy.Column("from", ForeignKey("Account.address"), nullable=False),
    sqlalchemy.Column("to", ForeignKey("Account.address"), nullable=False),
    sqlalchemy.Column("amount", sqlalchemy.Numeric(precision=23, scale=7), nullable=False),
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
    # Index('Transaction_from_from_sequence_uindex', 'from', 'from_sequence', unique=True),
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
    def to_json(cls, row: Row):
        d_row: Dict[str, Union[datetime, str]] = dict(row)
        d_row['created_at_dt'] = Arrow.fromdatetime(d_row['created_at'], tzinfo=tz.tzutc()).to(tz.gettz())
        d_row['created_at'] = int(d_row['created_at_dt'].timestamp())
        d_row['updated_at_dt'] = Arrow.fromdatetime(d_row['updated_at'], tzinfo=tz.tzutc()).to(tz.gettz())
        d_row['updated_at'] = int(d_row['updated_at_dt'].timestamp())
        if isinstance(d_row['balances'], str):
            d_row['balances'] = json.loads(d_row['balances'])
        d_row.pop('secret', None)
        return d_row


class TransactionRow:
    @classmethod
    def to_json(cls, row: Row):
        d_row = dict(row)
        d_row['created_at_dt'] = Arrow.fromdatetime(d_row['created_at'], tzinfo=tz.tzutc()).to(tz.gettz())
        d_row['created_at'] = int(d_row['created_at_dt'].timestamp())
        d_row['updated_at_dt'] = Arrow.fromdatetime(d_row['updated_at'], tzinfo=tz.tzutc()).to(tz.gettz())
        d_row['updated_at'] = int(d_row['updated_at_dt'].timestamp())
        d_row['is_success'] = bool(d_row['is_success'])
        return d_row
