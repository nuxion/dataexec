import time
from abc import ABC, abstractmethod
from datetime import datetime
from importlib import import_module
from typing import Any, Callable, Dict, List, Optional, Union

from dataexec import types, defaults
from dataexec.types import Asset, AssetChange, AssetMetadata


def _init_asset_class(fullclass_path, meta: AssetMetadata) -> Asset:
    """get a class or object from a module. The fullclass_path should be passed as:
    package.my_module.MyClass
    """
    module, class_ = fullclass_path.rsplit(".", maxsplit=1)
    mod = import_module(module)
    cls: Asset = getattr(mod, class_)
    obj = cls.from_meta(meta)
    return obj


class AssetRegistrySpec(ABC):
    kind_mapper: Dict[str, str]

    @abstractmethod
    def get(self, id: str) -> Asset:
        pass

    @abstractmethod
    def create(self, asset: Asset, msg: str, write: bool = True):
        pass

    @abstractmethod
    def commit(self, asset: Asset, message: str, write: bool = True):
        pass

    @abstractmethod
    def delete(self, id: str) -> bool:
        pass

    @abstractmethod
    def list(self) -> List[AssetMetadata]:
        pass

    @abstractmethod
    def list_changes(self, asset_id: str) -> List[AssetChange]:
        pass


class RegistrySpec(ABC):
    def __init__(
        self,
        kind_mapper: Dict[str, str] = defaults.KIND_MAPPER,
        params: Dict[str, Any] = {},
    ):
        self.kind_mapper = kind_mapper
        self._params = params

    @abstractmethod
    def get_asset(self, id_: str) -> Asset:
        pass

    @abstractmethod
    def create_asset(self, asset: Asset, msg: str, write: bool = True):
        pass

    @abstractmethod
    def commit_asset(self, asset: Asset, msg: str, write: bool = True):
        pass

    @abstractmethod
    def delete_asset(self, id: str) -> bool:
        pass

    @abstractmethod
    def list_assets(self) -> List[AssetMetadata]:
        pass

    @abstractmethod
    def list_changes(self, asset_id: str) -> List[AssetChange]:
        pass

    @abstractmethod
    def create_task(self, task_id: str):
        pass

    @abstractmethod
    def register_task(self, task_id: str):
        pass


class RegistryInMemory(RegistrySpec):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.assets: Dict[str, AssetMetadata] = {}
        self.changes: Dict[str, List[AssetChange]]

    def get_asset(self, id_: str) -> Asset:
        meta = self.assets[id_]
        asset = _init_asset_class(self.kind_mapper[meta.kind], meta)

        return asset

    def create_asset(self, asset: Asset, msg: str, write: bool = True):
        if write:
            asset.write()
        change = AssetChange(commit=asset.get_hash(), msg=msg)
        self.assets[asset.id] = asset.meta
        self.changes[asset.id] = [change]

    def commit_asset(self, asset: Asset, msg: str, write: bool = True):
        if write:
            asset.write()
        change = AssetChange(commit=asset.get_hash(), msg=msg)
        self.assets.update({asset.id: asset.meta})
        self.changes[asset.id].append(change)

    def delete_asset(self, id: str) -> bool:
        del self.assets[id]
        return True

    def list_assets(self) -> List[AssetMetadata]:
        return list(self.assets.values())

    def list_changes(self, asset_id: str) -> List[AssetChange]:
        return self.changes[asset_id]

    def create_task(self, task_id: str):
        raise NotImplementedError()

    def register_task(self, task_id: str):
        raise NotImplementedError()


class TaskFutureSpec:
    def __init__(self, execid: str, created_at=datetime.utcnow()):
        self.execid = execid
        self.created_at = created_at

    def is_done(self) -> bool:
        raise NotImplementedError()

    def get_result(self) -> types.Output:
        raise NotImplementedError()

    def status(self) -> str:
        raise NotImplementedError()
