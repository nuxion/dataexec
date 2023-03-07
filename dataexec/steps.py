import contextlib
import secrets
import time
from collections import OrderedDict
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import Callable, List, Dict, Any

from pydantic import BaseModel, Field

from dataexec import types, utils, errors
from dataexec.types import ExecLog, ExecStatus


class Step:
    def __init__(
        self,
        name,
        func: Callable,
        params: Dict[str, Any] = None,
        is_async=False,
        from_task=None,
        raise_on_error=True,
    ):
        self.name = name
        self._func = func
        # self._args = args
        # self._kwargs = kwargs
        self._params = params
        self.execid = None
        self._elapsed = 0.0
        self.is_async = is_async
        self._from_task = from_task
        self._raise = raise_on_error

    def _call_exception(self, e: Exception) -> types.Output:
        if self._raise:
            raise errors.StepExecutionError(self.name) from e
        return self._generate_output([], status=ExecStatus.failed, e=e)

    @contextlib.asynccontextmanager
    async def execute(self, *args, **kwargs) -> types.Output:
        try:
            _started = time.time()
            result = await self._func(*args, **kwargs)
            self._elapsed = int(_started - time.time())
            if not isinstance(result, list):
                output = self._generate_output([result])
            else:
                output = self._generate_output(result)

            yield output
        except Exception as e:
            self._elapsed = int(_started - time.time())
            output = self._call_exception(e)
            yield output
        finally:
            pass

    def _generate_output(
        self, result, status=ExecStatus.completed, e=None
    ) -> types.Output:
        _res = result if result else []
        output = types.Output(
            status=status,
            current_task=self.name,
            elapsed=self._elapsed,
            from_task=self._from_task,
            assets=_res,
            error=e,
        )
        return output

    def __call__(self, *args, **kwargs) -> types.Output:
        try:
            _started = time.time()
            self.execid = utils.secure_random_str()
            if not kwargs and self._params:
                result = self._func(*args, **self._params)
            else:
                result = self._func(*args, **kwargs)
            self._elapsed = int(_started - time.time())
            if not isinstance(result, list):
                output = self._generate_output([result])
            else:
                output = self._generate_output(result)
        except Exception as e:
            self._elapsed = int(_started - time.time())
            output = self._call_exception(e)
        return output
