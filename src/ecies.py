"""ECIES encryption: X25519 + HKDF-SHA256 + AES-256-GCM.

Ciphertext layout: ephemeral_pub(32) | nonce(12) | AES-GCM ciphertext
Matches SecureWrapper.decrypt() on the ML side.
"""

import os

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat


def ecies_encrypt(plaintext: bytes, model_pub_hex: str) -> bytes:
    model_pub = X25519PublicKey.from_public_bytes(bytes.fromhex(model_pub_hex))
    eph_priv = X25519PrivateKey.generate()
    eph_pub = eph_priv.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    shared_secret = eph_priv.exchange(model_pub)
    aes_key = HKDF(
        algorithm=hashes.SHA256(), length=32, salt=None, info=b"model-decrypt"
    ).derive(shared_secret)
    nonce = os.urandom(12)
    ciphertext = AESGCM(aes_key).encrypt(nonce, plaintext, None)
    return eph_pub + nonce + ciphertext
