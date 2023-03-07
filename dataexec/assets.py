from pathlib import Path
import shutil
from typing import Optional
from dataexec.types import AssetChange, AssetMetadata, Asset
from dataexec.utils import basic_hash, basic_random


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


class TextAsset(Asset[str]):
    """
    Dummy implementation to open texts files
    """

    kind: str = "textfile"

    @classmethod
    def from_location(cls, location, id_=None) -> "TextAsset":
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

    def copy(self, location: str, new_id=None) -> str:
        id_ = new_id or basic_random()
        shutil.copy(self.location, location)
        return new_id

    def write(self) -> bool:
        with open(self.location, "w", encoding="utf-8") as f:
            f.write(self.raw)

        return True

    def get_hash(self) -> str:
        return basic_hash(self.raw)

    def it_exist(self) -> bool:
        return Path(self.meta.location).exists()


def copy_asset(asset: Asset, new_location) -> Asset:
    id_ = asset.copy(new_location)
    new_asset = asset.from_location(new_location, id_)
    return new_asset
