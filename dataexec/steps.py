import contextlib
import time
from typing import Any, Callable, Dict, List, Optional

from dataexec import errors, types, utils


class Step:
    def __init__(
        self,
        name,
        func: Callable[..., types.StepReturn],
        params: Dict[str, Any] = {},
        step_id=None,
        is_async=False,
        from_step=None,
        raise_on_error=True,
    ):
        self.name = name
        self.func = func
        # self._args = args
        # self._kwargs = kwargs
        self.id = step_id or utils.secure_random_str()
        self.params = params
        self.execid: str = ""
        self._elapsed = 0
        self.is_async = is_async
        self._from_step = from_step
        self._raise = raise_on_error
        self._call_count = 0
        self.output: types.Output = self._generate_output(
            [], status=types.ExecStatus.created
        )

    @property
    def previous(self) -> Optional["Step"]:
        return self._from_step

    def set_previous(self, step_id: str):
        self._from_step = step_id

    def _call_exception(self, e: Exception) -> types.Output:
        if self._raise:
            raise errors.StepExecutionError(self.name) from e
        return self._generate_output([], status=types.ExecStatus.failed, e=e)

    @contextlib.asynccontextmanager
    async def execute(self, *args, **kwargs):
        try:
            _started = time.time()
            self._call_count += 1
            self.execid = utils.secure_random_str()
            if not kwargs and self.params:
                result = await self.func(*args, **self.params)
            else:
                result = await self.func(*args, **kwargs)
            self._elapsed = int(_started - time.time())
            if not isinstance(result, list):
                output = self._generate_output([result])
            else:
                output = self._generate_output(result)

            if not output.assets:
                raise errors.StepWithoutResult(self.id, self.name)
            yield output
        except Exception as e:
            self._elapsed = int(_started - time.time())
            output = self._call_exception(e)
            yield output
        finally:
            self.output = output

    def _generate_output(
        self, result: List[types.Asset], status=types.ExecStatus.completed, e=None
    ) -> types.Output:
        _res = result if result else []
        output = types.Output(
            status=status,
            current_step_id=self.id,
            current_step_name=self.name,
            elapsed=self._elapsed,
            from_step=self._from_step,
            assets=_res,
            error=e,
        )
        return output

    def __call__(self, *args, **kwargs) -> types.Output:
        try:
            _started = time.time()
            self._call_count += 1
            self.execid = utils.secure_random_str()
            if not kwargs and self.params:
                result = self.func(*args, **self.params)
            else:
                result = self.func(*args, **kwargs)
            self._elapsed = int(_started - time.time())
            if not isinstance(result, list):
                output = self._generate_output([result])
            else:
                output = self._generate_output(result)

            if not output.assets:
                raise errors.StepWithoutResult(self.id, self.name)
        except Exception as e:
            self._elapsed = int(_started - time.time())
            output = self._call_exception(e)

        self.output = output

        return output
