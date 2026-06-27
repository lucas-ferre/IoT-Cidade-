"""Criptografia + anti-replay do canal de controle (câmera Python).

Compartilhado entre o servidor de controle do sensor (sensor.py) e o console
(console.py). O mesmo esquema é espelhado no gateway e nos demais sensores:

  Frame cifrado = nonce[12] || AES-128-GCM(ciphertext + tag[16])
  Chave         = AES_SECRET_KEY (UTF-8, padding/truncate p/ 16 bytes)
  Anti-replay   = timestamp carimbado pelo emissor (janela de skew) +
                  cache de command_id já vistos (rejeita reenvio)

Ligado por CONTROL_SECURE=1. Com o flag desligado, nada é cifrado (modo legado).
A import de 'cryptography' é preguiçosa: o módulo carrega mesmo sem a lib quando
o modo seguro está desligado.
"""

import os
import time
import threading
from collections import OrderedDict


def _get_secret(name: str, default: str = "") -> str:
    file_path = os.getenv(name + "_FILE")
    if file_path:
        try:
            with open(file_path, "r", encoding="utf-8") as fh:
                return fh.read().strip()
        except OSError:
            pass
    return os.getenv(name, default)


_raw_key = _get_secret("AES_SECRET_KEY", "SmartCityKey1234").encode("utf-8")
KEY = _raw_key.ljust(16, b"\x00")[:16]

SECURE = os.getenv("CONTROL_SECURE", "0").strip().lower() in ("1", "true", "yes", "on")
MAX_SKEW_SECS = max(1, int(os.getenv("CONTROL_MAX_SKEW_SECS", "30")))

_NONCE_LEN = 12
_TAG_LEN = 16


def _aesgcm():
    # Import preguiçoso — só exige a lib quando o modo seguro é usado.
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    return AESGCM(KEY)


def wrap(plaintext: bytes) -> bytes:
    """Cifra um frame de controle: retorna nonce || ciphertext+tag."""
    nonce = os.urandom(_NONCE_LEN)
    ciphertext = _aesgcm().encrypt(nonce, plaintext, None)
    return nonce + ciphertext


def unwrap(blob: bytes) -> bytes:
    """Decifra um frame de controle (nonce || ciphertext+tag)."""
    if len(blob) < _NONCE_LEN + _TAG_LEN:
        raise ValueError("frame cifrado muito curto")
    return _aesgcm().decrypt(blob[:_NONCE_LEN], blob[_NONCE_LEN:], None)


class ReplayGuard:
    """Rejeita comandos fora da janela de tempo ou com command_id repetido."""

    def __init__(self, max_skew: int = MAX_SKEW_SECS, capacity: int = 512):
        self.max_skew = max_skew
        self.capacity = capacity
        self._seen: "OrderedDict[str, int]" = OrderedDict()
        self._lock = threading.Lock()

    def check(self, command_id: str, ts: int) -> tuple[bool, str]:
        now = int(time.time())
        if ts and abs(now - ts) > self.max_skew:
            return False, f"timestamp fora da janela (±{self.max_skew}s)"
        with self._lock:
            if command_id and command_id in self._seen:
                return False, "command_id repetido (replay)"
            if command_id:
                self._seen[command_id] = now
                while len(self._seen) > self.capacity:
                    self._seen.popitem(last=False)
        return True, ""
