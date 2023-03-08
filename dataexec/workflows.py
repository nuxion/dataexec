from collections import OrderedDict
from typing import Callable, List, Any, Dict, Optional
from functools import wraps

from dataexec import types
from dataexec.steps import Step
from dataexec import errors
from itertools import islice
from tqdm.auto import trange


class Sequence:
    def __init__(self, registry={}, steps: List[Step] = [], disable_tqdm=False):
        self.registry = registry
        self.steps = OrderedDict()
        if steps:
            for s in steps:
                self.steps[s.name] = s
        self.errors = []
        self.assets = []
        self.exec_log: List[types.ExecLog] = []
        self._disable_tqdm = disable_tqdm

    def _get_step(self, name: str) -> Step:
        return self.steps[name]

    def _run_step(
        self, name: str, result: Optional[types.Output] = None, prev_step=None, **kwargs
    ) -> types.Output:
        step = self._get_step(name)
        step.set_previous(prev_step)
        if not prev_step:
            result = step(**kwargs)
        elif result:
            to_inject = self._inject_params(step, result)
            result = step(**to_inject)
        else:
            raise errors.StepWithoutResult(step.id, step.name)

        log = types.ExecLog(step_name=name, execid=step.execid)
        log.status = result.status
        log.error = result.error
        self.exec_log.append(log)
        return result

    def step(self, name: str, cache=None, from_task=None, raise_on_error=True):
        def decorator(f):
            @wraps(f)
            def decorated_function(*args, **kwargs):
                if name not in self.steps:
                    self.add_step(
                        name,
                        f,
                        is_async=False,
                        from_task=from_task,
                        raise_on_error=raise_on_error,
                    )
                result = self.steps[name](*args, **kwargs)
                return result

            return decorated_function

        return decorator

    def add_step(self, name: str, func: Callable, is_async, from_task, raise_on_error):
        step = Step(name, func, is_async, from_task, raise_on_error)
        self.steps[name] = step

    def _inject_params(self, next_step: Step, result: types.Output) -> Dict[str, Any]:
        to_inject = {}
        for n, type_ in next_step.func.__annotations__.items():
            for asset in result.assets:
                if isinstance(asset, type_):
                    to_inject[n] = asset
        if next_step.params:
            to_inject.update(next_step.params)
        return to_inject

    def _iter(self, result, prev_step, **kwargs):
        stop = len(self.steps) - 1
        for name in islice(self.steps, 0, stop):
            # print(f"Running task {name}")
            yield self._run_step(name, result, prev_step=prev_step, **kwargs)

    def __call__(self, **kwargs) -> types.Output:
        # steps = list(self.steps)
        # first = self._get_step(steps[0])
        # print(f"Running task {first.name}")
        # prev_step = None
        # result = self._run_step(first.name, prev_step=prev_step, **kwargs)
        # stop = len(self.steps) - 1
        prev_step = None
        _result = None
        # _result = None
        # results = [r for r in self._iter(_result, prev_step, **kwargs)]
        steps = list(self.steps)
        for i in trange(len(steps), desc="Sequence Step iteration"):
            name = steps[i]
            _result = self._run_step(name, _result, prev_step=prev_step, **kwargs)
            prev_step = _result.current_step_id

        _last = next(reversed(self.steps))
        return self.steps[_last].output
