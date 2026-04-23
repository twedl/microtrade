from hashlib import sha256
from pathlib import Path

_CHUNK = 1 << 20


def hash_file(path: Path) -> str:
    h = sha256()
    with open(path, "rb") as f:
        while chunk := f.read(_CHUNK):
            h.update(chunk)
    return h.hexdigest()
