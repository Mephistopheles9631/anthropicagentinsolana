"""Minimal Borsh binary decoder for Solana IDL types.

Only the types actually used in the event schemas we care about are implemented.
No external dependencies — everything is pure Python.
"""
from __future__ import annotations

# Solana base-58 alphabet
_B58_ALPHABET = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58encode(data: bytes) -> str:
    """Pure-Python base58 encoder for Solana public keys."""
    n = int.from_bytes(data, "big")
    result: list[int] = []
    while n > 0:
        n, rem = divmod(n, 58)
        result.append(_B58_ALPHABET[rem])
    for byte in data:
        if byte == 0:
            result.append(_B58_ALPHABET[0])
        else:
            break
    return bytes(reversed(result)).decode("ascii")


class BorshReader:
    """Sequential Borsh binary reader.

    Raises ``ValueError`` if there is not enough data to satisfy a read.
    Callers should catch ``(ValueError, IndexError)`` for malformed inputs.
    """

    __slots__ = ("_data", "_pos")

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0

    @property
    def remaining(self) -> int:
        return len(self._data) - self._pos

    def _consume(self, n: int) -> bytes:
        end = self._pos + n
        if end > len(self._data):
            raise ValueError(
                f"BorshReader: need {n} bytes at pos {self._pos}, "
                f"only {len(self._data) - self._pos} remaining"
            )
        chunk = self._data[self._pos : end]
        self._pos = end
        return chunk

    # ------------------------------------------------------------------
    # Primitive readers
    # ------------------------------------------------------------------

    def read_u8(self) -> int:
        return self._consume(1)[0]

    def read_u16(self) -> int:
        return int.from_bytes(self._consume(2), "little")

    def read_u32(self) -> int:
        return int.from_bytes(self._consume(4), "little")

    def read_u64(self) -> int:
        return int.from_bytes(self._consume(8), "little")

    def read_u128(self) -> int:
        return int.from_bytes(self._consume(16), "little")

    def read_i64(self) -> int:
        return int.from_bytes(self._consume(8), "little", signed=True)

    def read_bool(self) -> bool:
        return bool(self.read_u8())

    def read_pubkey(self) -> str:
        """Read 32 raw bytes and return the base58 Solana address."""
        return _b58encode(self._consume(32))

    def read_string(self) -> str:
        """Borsh string: u32 length prefix + UTF-8 bytes."""
        length = self.read_u32()
        return self._consume(length).decode("utf-8", errors="replace")

    def read_option(self, reader_fn):  # type: ignore[no-untyped-def]
        """Borsh option: u8 tag (0 = None, 1 = Some) + value."""
        tag = self.read_u8()
        if tag == 0:
            return None
        return reader_fn()

    def read_enum_variant(self) -> int:
        """Borsh enum: u8 variant index."""
        return self.read_u8()

    def skip(self, n: int) -> None:
        self._consume(n)
