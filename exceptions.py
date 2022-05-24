from sanic.exceptions import SanicException


class AddressNotFound(SanicException):
    status_code = 40001

    @property
    def message(self):
        return f"Address {self.extra.get('address', '')} not found"


class AssetNotTrusted(SanicException):
    status_code = 40002

    @property
    def message(self):
        return f"Account {self.extra.pop('addr', '')}'s Asset {self.extra.pop('asset', '')} not trusted, {self.extra}"


class TransactionNotFound(SanicException):
    status_code = 40003

    @property
    def message(self):
        return f"Transaction {self.extra['tx_hash']} not found"


class TransactionsOfAccountNotFound(SanicException):
    status_code = 40004

    @property
    def message(self):
        return f"Transactions of Account {self.extra['address']} not found"


class TransactionsBuildFailed(SanicException):
    status_code = 40005

    @property
    def message(self):
        return f"Transaction build failed: {self.extra}"


class TransactionsExpired(SanicException):
    status_code = 40006

    @property
    def message(self):
        return f"Transaction Expired: {self.extra}"


class InsufficientFunds(SanicException):
    status_code = 40007

    @property
    def message(self):
        return f"Insufficient Funds for amount {self.extra['amount']} of {self.extra['addr']}"


class TransactionsSendFailed(SanicException):
    status_code = 40008

    @property
    def message(self):
        return f"Transaction send failed: {self.extra}"


class TransactionsSelfTransfer(SanicException):
    status_code = 40009

    @property
    def message(self):
        return f"Cannot transfer to self {self.extra.get('addr')}"


class BulkTransactionsFromAddress(SanicException):
    status_code = 40009

    @property
    def message(self):
        return f"Op must contains from address. {self.extra.get('from_addr')}"
