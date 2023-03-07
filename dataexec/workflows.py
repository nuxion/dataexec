from collections import OrderedDict
from typing import Callable, List, Any, Dict
from functools import wraps

from dataexec import types
from dataexec.steps import Step
from dataexec import errors


class Workflow:
    def __init__(self, registry={}, steps: List[Step] = None):
        self.registry = registry
        self.steps: OrderedDict[str, Step] = OrderedDict()
        if steps:
            for s in steps:
                self.steps[s.name] = s
        self.errors = []
        self.assets = []
        self.exec_log: List[types.ExecLog] = []

    def _run_step(
        self, name: str, result: types.Output = None, **kwargs
    ) -> types.Output:
        step = self.steps[name]
        if result:
            to_inject = self._inject_params(step, result)
            result = step(**to_inject)
        else:
            result = step(**kwargs)
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
        for n, type_ in next_step._func.__annotations__.items():
            for asset in result.assets:
                if isinstance(asset, type_):
                    to_inject[n] = asset
        if next_step._params:
            to_inject.update(next_step._params)
        return to_inject

    def run(self, **kwargs) -> types.Asset:
        steps = list(self.steps)
        print(f"Running task {steps[0]}")
        result = self._run_step(steps[0], **kwargs)
        for name in steps[1:]:
            print(f"Running task {name}")
            result = self._run_step(name, result)
        return result
