"""ULID-based public ids.

A ULID is 128 bits, the same as a UUID, so `bat_<ULID>` / `tsk_<ULID>` map 1:1 onto the uuid primary keys:
public ids sort by creation time and are safe to show, while the database keeps native uuid columns.
"""

import os
import time
from uuid import UUID

_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_DECODE = {c: i for i, c in enumerate(_ALPHABET)}


def new_ulid() -> str:
    n = (int(time.time() * 1000) << 80) | int.from_bytes(os.urandom(10), "big")
    return _encode(n)


def _encode(n: int) -> str:
    out = []
    for _ in range(26):
        out.append(_ALPHABET[n & 31])
        n >>= 5
    return "".join(reversed(out))


def ulid_to_uuid(ulid: str) -> UUID:
    n = 0
    for ch in ulid.upper():
        n = (n << 5) | _DECODE[ch]
    return UUID(int=n)


def uuid_to_ulid(u: UUID) -> str:
    return _encode(u.int)


def batch_public_id(u: UUID) -> str:
    return f"bat_{uuid_to_ulid(u)}"


def task_public_id(u: UUID) -> str:
    return f"tsk_{uuid_to_ulid(u)}"


def parse_public_id(value: str, prefix: str) -> UUID:
    """Accept `bat_<ULID>` (or a raw uuid, handy for admin tools). Raises ValueError on garbage."""
    if value.startswith(prefix + "_"):
        body = value[len(prefix) + 1 :]
        if len(body) != 26 or any(c not in _DECODE for c in body.upper()):
            raise ValueError(f"invalid id {value!r}")
        return ulid_to_uuid(body)
    return UUID(value)
