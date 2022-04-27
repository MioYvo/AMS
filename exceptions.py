from sanic.exceptions import SanicException


class AddressNotFound(SanicException):
    status_code = 40001

    @property
    def message(self):
        return f"Address {self.extra['address']} not found"


class AssetNotTrusted(SanicException):
    status_code = 40002

    @property
    def message(self):
        return f"Asset {self.extra['asset']} not trusted"
