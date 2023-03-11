from datetime import datetime
from enum import Enum
from typing import Any, Dict, Generic, List, Optional, TypeVar, NewType, Union

from pydantic import BaseModel, Field

AssetT = TypeVar("AssetT")


class AssetChange(BaseModel):
    commit: str
    msg: str
    created_at: datetime = Field(default_factory=datetime.utcnow)


class AssetMetadata(BaseModel):
    id: str
    location: str
    kind: str = "generic"
    description: Optional[str] = None
    author: Optional[str] = None
    derived_from: Optional[str] = None
    build_by_task: Optional[str] = None
    extra: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
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

    @classmethod
    def from_location(
        cls, location, id_=None, author=None, derived_from=None
    ) -> "Asset":
        raise NotImplementedError()

    def copy(self, location, new_id=None) -> str:
        raise NotImplementedError()

    @property
    def raw(self) -> AssetT:
        return self._raw

    @staticmethod
    def open(location: str) -> AssetT:
        raise NotImplementedError()

    def write(self) -> bool:
        raise NotImplementedError()

    def it_exist(self) -> bool:
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


StepReturn = NewType("StepReturn", Union[Asset, List[Asset]])


class Output(BaseModel):
    status: str
    current_step_id: str
    current_step_name: str
    elapsed: int
    from_step: Optional[str] = None
    assets: List[Asset] = Field(default_factory=list)
    error: Optional[Exception] = None

    class Config:
        arbitrary_types_allowed = True
        use_enum_values = True


class StepParams(BaseModel):
    params: Dict[str, Any] = Field(default_factory=dict)
    asset: Optional[Asset] = None

    class Config:
        arbitrary_types_allowed = True


class Input(BaseModel):
    # params: Dict[str, Any] = Field(default_factory=dict)
    params: Dict[str, Any] = Field(default_factory=dict)
    assets: List[Asset] = Field(default_factory=list)

    class Config:
        arbitrary_types_allowed = True


class OutputRef(BaseModel):
    from_task: str
    task: str
    status: str
    assets: List[AssetMetadata] = Field(default_factory=list)


class InputRef(BaseModel):
    params: Dict[str, Any]
    assets: List[AssetMetadata] = Field(default_factory=list)
    from_task: Optional[str] = None


class ExecStatus(str, Enum):
    created = "CREATED"
    waiting = "WAITING"
    running = "RUNNING"
    cancelled = "CANCELLED"
    failed = "FAILED"
    done = "DONE"


class ExecLog(BaseModel):
    step_name: str
    step_execid: str
    wf_exec_id: Optional[str] = None
    status: str = ExecStatus.created
    error: Optional[Exception] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        use_enum_values = True
        arbitrary_types_allowed = True


class WorkflowExecLog(BaseModel):
    wf_exec_id: Optional[str] = None
    status: str = ExecStatus.created
    error: Optional[Exception] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        use_enum_values = True
        arbitrary_types_allowed = True
