import time
from typing import Any, Callable, Dict, Optional

from dataexec import errors, types, utils


class Step:
    def __init__(
        self,
        func: Callable[..., types.StepReturn],
        alias=None,
        params: Dict[str, Any] = {},
        step_id=None,
        is_async=False,
        from_step=None,
        raise_on_error=True,
    ):
        self.alias = alias or func.__name__
        self.func = func
        self.id = step_id or utils.secure_random_str()
        self.params = params
        self.execid: str = ""
        self._elapsed = 0
        self.is_async = is_async
        self._from_step = from_step
        self._raise = raise_on_error
        self._call_count = 0
        self._output: types.Output = self._generate_output(
            [], status=types.ExecStatus.created
        )

    @property
    def previous(self) -> Optional["Step"]:
        return self._from_step

    def result(self) -> types.Output:
        return self._output

    def set_previous(self, step_id: str):
        self._from_step = step_id

    def _call_exception(self, e: Exception) -> types.Output:
        if self._raise:
            raise errors.StepExecutionError(self.alias) from e
        return self._generate_output([], status=types.ExecStatus.failed, e=e)

    def _generate_output(
        self, result: Any, status=types.ExecStatus.done, e=None
    ) -> types.Output:
        _assets = []
        if result:
            if isinstance(result, list):
                for r in result:
                    if isinstance(r, types.Asset):
                        _assets.append(r)
            else:
                if isinstance(result, types.Asset):
                    _assets.append(result)

        self._output = types.Output(
            status=status,
            current_step_id=self.id,
            current_step_name=self.alias,
            elapsed=self._elapsed,
            from_step=self._from_step,
            assets=_assets,
            error=e,
        )
        return self._output

    async def run_async(self, *args, **kwargs):
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
                raise errors.StepWithoutResult(self.id, self.alias)
        except Exception as e:
            self._elapsed = int(_started - time.time())
            output = self._call_exception(e)

        self._output = output

        return result

    def __call__(self, *args, **kwargs):
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

            # if not output.assets:
            #    raise errors.StepWithoutResult(self.id, self.alias)
        except Exception as e:
            self._elapsed = int(_started - time.time())
            output = self._call_exception(e)

        self._output = output

        return result
