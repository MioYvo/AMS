from sanic.exceptions import SanicException


class AddressNotFound(SanicException):
    status_code = 404

    @property
    def message(self):
        return f"Address {self.extra['address']} not found"
