import base64
from functools import lru_cache
from typing import Union

from Crypto.Cipher import PKCS1_v1_5, PKCS1_OAEP
from Crypto.PublicKey import RSA
from Crypto.PublicKey.RSA import RsaKey
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad

from AMS.config import settings

cipher_select = {
    "PKCS1_v1_5": PKCS1_v1_5,
    "PKCS1_OAEP": PKCS1_OAEP
}


class AMSCrypt:
    @classmethod
    @lru_cache(maxsize=1)
    def account_secret_aes_key(cls) -> str:
        with open(settings.ACCOUNT_SECRET_AES_KEY) as f:
            return f.read()

    @classmethod
    @lru_cache(maxsize=1)
    def account_secret_aes_iv(cls) -> str:
        with open(settings.ACCOUNT_SECRET_AES_IV) as f:
            return f.read()


def rsa_encrypt(input_data: Union[str, bytes], public_rsa_key: Union[str, RsaKey], cipher='PKCS1_v1_5') -> bytes:
    if isinstance(input_data, str):
        input_data = input_data.encode()

    if isinstance(public_rsa_key, str):
        public_key = RSA.import_key(public_rsa_key)
    else:
        public_key = public_rsa_key

    cipher = cipher_select[cipher].new(public_key)
    ciphertext: bytes = cipher.encrypt(input_data)
    return base64.b64encode(ciphertext)


def rsa_decrypt(input_data: Union[str, bytes], private_rsa_key: Union[str, RsaKey], cipher='PKCS1_v1_5') -> bytes:
    if isinstance(input_data, str):
        input_data = input_data.encode()
    data = base64.b64decode(input_data)
    if isinstance(private_rsa_key, str):
        private_key = RSA.import_key(private_rsa_key)
    else:
        private_key = private_rsa_key

    _cipher = cipher_select[cipher].new(private_key)
    if cipher == 'PKCS1_v1_5':
        args = (data, True)
    else:
        args = (data,)
    ciphertext = _cipher.decrypt(*args)
    return ciphertext


def aes_encrypt(data: Union[bytes, str], key: Union[bytes, str], iv: Union[bytes, str]) -> bytes:
    cipher = AES.new(
        base64.b64decode(key.encode()) if isinstance(key, str) else base64.b64decode(key),
        AES.MODE_CBC,
        iv=base64.b64decode(iv.encode()) if isinstance(iv, str) else base64.b64decode(iv)
    )
    padded_data = pad(data.encode(), cipher.block_size)
    return base64.b64encode(cipher.encrypt(padded_data))


def aes_decrypt(data: Union[bytes, str], key: Union[bytes, str], iv: Union[bytes, str]) -> bytes:
    cipher = AES.new(
        base64.b64decode(key.encode()) if isinstance(key, str) else base64.b64decode(key),
        AES.MODE_CBC,
        iv=base64.b64decode(iv.encode()) if isinstance(iv, str) else base64.b64decode(iv)
    )
    data = cipher.decrypt(base64.b64decode(data))
    return unpad(data, cipher.block_size)
