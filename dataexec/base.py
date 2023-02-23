import time
from abc import ABC, abstractmethod
from datetime import datetime
from importlib import import_module
from typing import Any, Callable, Dict, List, Optional, Union

from dataexec import types
from dataexec.types import Asset, AssetChange, AssetMetadata


def _init_class(fullclass_path, meta: AssetMetadata) -> Asset:
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
    asset_mapper: Dict[str, str]

    @abstractmethod
    def get_asset(self, id: str) -> Asset:
        pass

    @abstractmethod
    def create_asset(self, asset: Asset, msg: str, write: bool = True):
        pass

    @abstractmethod
    def commit_asset(self, asset: Asset, message: str, write: bool = True):
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




class AssetRegistryInMemory(AssetRegistrySpec):
    kind_mapper = {"textfile": "dataexec.assets.AssetText"}

    def __init__(self):
        self.data: Dict[str, AssetMetadata] = {}
        self.changes: Dict[str, List[AssetChange]]

    def get(self, id: str) -> Asset:
        meta = self.data.get(id)
        asset = _init_class(self.kind_mapper[meta.kind], meta)

        return asset

    def create(self, asset: Asset, msg: str, write: bool = True):
        if write:
            asset.write()
        change = AssetChange(commit=asset.get_hash(), msg=msg)
        self.data[asset.id] = asset.meta
        self.changes[asset.id] = [change]

    def commit(self, asset: Asset, msg: str, write: bool = True):
        if write:
            asset.write()
        change = AssetChange(commit=asset.get_hash(), msg=msg)
        self.data.update({asset.id: asset.meta})
        self.changes[asset.id].append(change)

    def delete(self, id: str) -> bool:
        del self.data[id]
        return True

    def list(self) -> List[AssetMetadata]:
        return list(self.data.values())

    def list_changes(self, asset_id: str) -> List[AssetChange]:
        return self.changes[asset_id]


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


class TaskRunnerSpec:
    def __init__(self, *, meta: types.TaskMeta, params: Dict[str, Any] = {}):
        self.meta = meta
        self.params = params

    def run(self, inputs: types.Input) -> TaskFutureSpec:
        raise NotImplementedError()

    def register(self, result: types.OutputRef) -> bool:
        raise NotImplementedError()


class Sequence:
    def __init__(self, tasks: List[TaskRunnerSpec]):
        self._tasks = tasks

    def start(self, inputs: types.Input):
        for task in self._tasks:
            fut = task.run(inputs)
            while not fut.is_done():
                time.sleep(5)
            result = fut.get_result()
            inputs = types.Input(from_task=task.meta.taskid, assets=result.assets)



class Task:
    name: str
    func: Callable
    notify: bool = True
    repeat: int = 2
            

seq = Sequence(
    [
        TaskRunnerSpec(meta=types.TaskMeta(taskid="asd", name="pepe")),
        TaskRunnerSpec(meta=types.TaskMeta(taskid="asd", name="pepe")),
    ]
)
seq.start(types.Input())


@seq.task(name="test", notifications=True, repeat=3, cache="5d")
def example(params={}, assests=[]) -> Union[List[Asset], None]:
    pass
