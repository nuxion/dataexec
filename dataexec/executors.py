import asyncio
import contextlib
import time
from abc import ABC, abstractmethod, ABCMeta
from multiprocessing import get_context
from concurrent.futures import ProcessPoolExecutor, CancelledError, TimeoutError
from typing import Any, Awaitable, Callable, Dict, Generic, TypeVar, Optional, Coroutine
from concurrent.futures._base import Future

from pydantic import BaseModel

from dataexec import errors, types, utils

ExecT = TypeVar("ExecT", bound=BaseModel)
ExecResult = TypeVar("ExecResult")
Exec2T = TypeVar("Exec2T")


def _process_wrapper(fn, ctx, *args, **kwargs):
    with ProcessPoolExecutor(mp_context=ctx) as pool:
        future = pool.submit(fn, *args, **kwargs)
        yield future


class MPConfig(BaseModel):
    pool_size: int = 2
    method: str = "fork"
    timeoput: int = 60


class IGenericFuture(Generic[ExecResult], ABC):
    @abstractmethod
    def cancel(self):
        pass

    @abstractmethod
    def canceled(self) -> bool:
        pass

    @abstractmethod
    def running(self) -> bool:
        pass

    @abstractmethod
    def done(self) -> bool:
        pass

    @abstractmethod
    def result(self, timeout=None) -> ExecResult:
        pass


class ITask(metaclass=ABCMeta):
    @abstractmethod
    def get_status(self) -> str:
        pass

    @abstractmethod
    def cancel(self):
        raise NotImplementedError()

    @abstractmethod
    def cancelled(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def running(self) -> bool:
        raise NotImplementedError()

    def done(self) -> bool:
        raise NotImplementedError()

    def result(self, timeout=None) -> Any:
        raise NotImplementedError()


class TaskBase(Generic[Exec2T]):
    def __init__(
        self,
        taskid: str,
        awaitable: Exec2T,
        result: Any = None,
    ):
        self._result = result
        self.id = taskid
        self.obj = awaitable
        self._status: types.ExecStatus = types.ExecStatus.done

    def get_status(self) -> str:
        return self._status.value

    def cancel(self) -> bool:
        raise NotImplementedError()

    def cancelled(self) -> bool:
        return self._status == types.ExecStatus.cancelled

    def running(self) -> bool:
        raise NotImplementedError()

    def done(self) -> bool:
        raise NotImplementedError()

    def result(self, timeout=None) -> Any:
        raise NotImplementedError()


class AIOTaskBase(Generic[Exec2T]):
    def __init__(
        self,
        taskid: str,
        awaitable: Exec2T,
        result: Any = None,
    ):
        self._result = result
        self.id = taskid
        self.obj = awaitable
        self._status: types.ExecStatus = types.ExecStatus.done

    async def get_status(self) -> str:
        return self._status.value

    async def cancel(self) -> bool:
        raise NotImplementedError()

    def cancelled(self) -> bool:
        return self._status == types.ExecStatus.cancelled

    def running(self) -> bool:
        raise NotImplementedError()

    def done(self) -> bool:
        raise NotImplementedError()

    async def result(self, timeout=None) -> Any:
        raise NotImplementedError()


class IExecutor(metaclass=ABCMeta):
    @abstractmethod
    def submit(self, fn: Callable, *args, **kwargs) -> TaskBase:
        raise NotImplementedError()

    # @abstractmethod
    # def map(self, fn: Callable, *iterables) -> List[IFuture]:
    #     raise NotImplementedError()

    # @abstractmethod
    # def startmap(self, funcs: List[Callable],  *iterables) -> List[IFuture]:
    #     raise NotImplementedError()

    # @abstractmethod
    # def shutdown(wait=True, *, cancel_futures=False):
    #     raise NotImplementedError()


class AIOExecutor(ABC):
    @abstractmethod
    async def submit(self, fn: Callable, *args, **kwargs) -> AIOTaskBase:
        raise NotImplementedError()


class LocalTask(TaskBase[Coroutine]):
    def cancel(self) -> bool:
        return False

    def cancelled(self) -> bool:
        return False

    def running(self) -> bool:
        return bool(self._result)

    def done(self) -> bool:
        return bool(self._result)

    def result(self, timeout=None) -> Any:
        if not self._result:
            self._result = next(self.obj)
            try:
                next(self.obj)
            except StopIteration:
                pass
        return self._result


class FutureTask(TaskBase[Future]):
    def __init__(self, taskid: str, awaitable: Future, coro: Coroutine):
        super().__init__(taskid, awaitable)
        self.coro = coro

    def cancel(self) -> bool:
        cancelled = self.obj.cancel()
        if not cancelled:
            try:
                self.obj.result(0.1)
            except TimeoutError:
                cancelled = True
        if cancelled and not self.obj.running():
            self._status = types.ExecStatus.cancelled
            return cancelled
        return False

    def running(self) -> bool:
        return self.obj.is_alive()

    def done(self) -> bool:
        return True

    def _next(self):
        try:
            next(self.coro)
        except StopIteration:
            pass
        except CancelledError:
            pass
        except TimeoutError:
            pass

    def result(self, timeout=None) -> Any:
        if not self._result:
            try:
                self._result = self.obj.result(timeout)
                self._status = types.ExecStatus.done
                self._next()
            except TimeoutError as e:
                self._status = types.ExecStatus.failed
                raise errors.TaskTimeoutError(self.id) from e
        return self._result


class AIOTask(AIOTaskBase[Awaitable]):
    async def cancel(self):
        try:
            self.obj.cancel()
        except asyncio.CancelledError as e:
            raise errors.CancelledError(self.obj) from e

    def cancelled(self) -> bool:
        return self.obj.cancelled()

    def running(self) -> bool:
        return not self.obj.done()

    def done(self) -> bool:
        return self.obj.done()

    async def result(self, timeout=None) -> Any:
        try:
            if timeout and not self._result:
                self._result = await asyncio.wait_for(self.obj, timeout=timeout)
            elif not self._result:
                self._result = await self.obj

        except asyncio.TimeoutError as e:
            raise errors.TaskTimeoutError(self.id) from e

        return self._result


class LocalDev(IExecutor):
    """The defaul executor"""

    @staticmethod
    def _call_wrapper(fn: Callable, *args, **kwargs):
        yield fn(*args, **kwargs)

    def submit(self, fn: Callable, *args, **kwargs) -> LocalTask:
        """
        the result of the function is wrapped on a coroutine to mimic
        the same api of other tasks, and force the call
        to LocalTask.result()
        """

        taskid = utils.secure_random_str()
        coro = self._call_wrapper(fn, *args, **kwargs)
        return LocalTask(taskid, coro)


class LocalProcess(IExecutor):
    def __init__(self, method="fork"):
        self._ctx = get_context(method)

    def submit(self, fn: Callable, *args, **kwargs) -> FutureTask:
        coro = _process_wrapper(fn, self._ctx, *args, **kwargs)
        future = next(coro)
        taskid = utils.secure_random_str()
        return FutureTask(taskid, future, coro)


class AsyncLocal(AIOExecutor):
    async def submit(self, fn: Callable, *args, **kwargs) -> AIOTask:
        awaitable = asyncio.create_task(fn(*args, **kwargs))
        taskid = utils.secure_random_str()
        return AIOTask(taskid, awaitable)
