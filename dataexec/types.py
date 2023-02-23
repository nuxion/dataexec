from datetime import datetime
from typing import Any, Dict, Generic, List, Optional, TypeVar

from pydantic import BaseModel, Field

AssetT = TypeVar("AssetT")


class AssetChange(BaseModel):
    commit: str
    msg: str
    created_at: datetime = Field(default_facatory=datetime.utcnow)


class AssetMetadata(BaseModel):
    id: str
    location: str
    kind: str = "generic"
    description: Optional[str] = None
    author: Optional[str] = None
    derived_from: Optional[str] = None
    build_by_task: Optional[str] = None
    extra: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_facatory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class Asset(Generic[AssetT]):
    kind: str

    def __init__(
        self,
        *,
        raw: AssetT,
        meta: AssetMetadata,
    ):
        self.meta = meta
        self._raw = raw

    @property
    def id(self) -> str:
        return self.meta.id

    @property
    def location(self) -> str:
        return self.meta.location

    @classmethod
    def from_meta(cls, meta: AssetMetadata) -> "Asset":
        raw = cls.open(meta.location)
        obj = cls(raw=raw, meta=meta)
        return obj

    @property
    def raw(self) -> AssetT:
        return self._raw

    @staticmethod
    def open(location: str) -> AssetT:
        raise NotImplementedError()

    def write(self) -> bool:
        raise NotImplementedError()

    def asset_exist(self) -> bool:
        raise NotImplementedError()

    def get_hash(self) -> str:
        raise NotImplementedError()

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if isinstance(other, Asset):
            return self.id == self.id
        return NotImplemented

    def __str__(self):
        return f"<Asset [kind={self.kind}] in {self.location}>"

    def __repr__(self):
        return f"<Asset [kind={self.kind}] in {self.location}>"


class Output(BaseModel):
    status: str
    from_task: Optional[str] = None
    assets: List[Asset] = Field(default_factory=list)
    elapsed: str = ""


class Input(BaseModel):
    # params: Dict[str, Any] = Field(default_factory=dict)
    from_task: Optional[str] = None
    assets: List[Asset] = Field(default_factory=list)


class TaskMeta(BaseModel):
    taskid: str
    name: str
    params: Dict[str, Any] = Field(default_factory=dict)


class TaskDelayed(BaseModel):
    execid: str
    created_at: datetime = Field(default_facatory=datetime.utcnow)


class OutputRef(BaseModel):
    from_task: str
    status: str
    assets: List[AssetMetadata] = Field(default_factory=list)


class InputRef(BaseModel):
    params: Dict[str, Any]
    assets: List[AssetMetadata] = Field(default_factory=list)
    from_task: Optional[str] = None
