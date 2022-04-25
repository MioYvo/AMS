import random

import base58
import ecdsa
from Crypto.Hash import keccak


def keccak256(data: bytes) -> bytes:
    hasher = keccak.new(digest_bits=256)
    hasher.update(data)
    return hasher.digest()


class Address:
    def __init__(self, private_key_bytes: bytes):
        self._raw_key = private_key_bytes
        priv_key = ecdsa.SigningKey.from_string(self._raw_key, curve=ecdsa.SECP256k1)
        self.public_raw_key = priv_key.get_verifying_key().to_string()

    @classmethod
    def random(cls):
        return cls(bytes([random.randint(0, 255) for _ in range(32)]))

    def to_address(self):
        primitive_addr = b"\x17" + keccak256(self.public_raw_key)[-20:]
        addr = base58.b58encode_check(primitive_addr)
        return addr.decode()

    @classmethod
    def generate(cls) -> str:
        address = cls.random().to_address()
        if not address.startswith('A'):
            return cls.generate()
        else:
            return address
