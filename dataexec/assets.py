import hashlib
import random
import string
from pathlib import Path
from typing import Optional

from dataexec.types import Asset, AssetChange, AssetMetadata

_letters = string.ascii_lowercase


def basic_random(lenght=10) -> str:
    return "".join(random.choice(_letters) for i in range(lenght))


def basic_hash(txt: str) -> str:
    _hash = hashlib.md5(txt.encode())
    return _hash.hexdigest()


def build_metadata(
    location: str,
    id_: Optional[str] = None,
    description=None,
    kind="generic",
    author=None,
    derived_from=None,
) -> AssetMetadata:
    msg = description or "first commit"
    if not id_:
        id_ = basic_random()
    init_change = AssetChange(commit=basic_hash(""), msg=msg)
    meta = AssetMetadata(
        id=id_,
        location=location,
        kind=kind,
        author=author,
        changes=[init_change],
        derived_from=derived_from,
    )
    return meta


class AssetText(Asset[str]):
    kind: str = "textfile"
    raw: str

    @classmethod
    def from_location(cls, location, id_=None) -> "AssetText":
        raw = cls.open(location)
        meta = build_metadata(location, id_=id_)
        meta.kind = cls.kind
        obj = cls(raw=raw, meta=meta)
        return obj

    @staticmethod
    def open(location: str) -> str:
        with open(location, "r", encoding="utf-8") as f:
            txt = f.read()
        return txt

    def write(self) -> bool:
        if self.asset_exist():
            with open(self.location, "w", encoding="utf-8") as f:
                f.write(self.raw)

            return True
        return False

    def get_hash(self) -> str:
        return basic_hash(self.raw)

    def asset_exist(self) -> bool:
        return Path(self.meta.location).exists()
