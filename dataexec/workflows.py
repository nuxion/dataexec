from abc import ABC, abstractmethod
from collections import OrderedDict
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, cast

from tqdm.auto import trange

from dataexec import errors, types, utils
from dataexec.steps import Step
from dataexec.executors import IExecutor, LocalDev, TaskBase


class IWorkflow(ABC):
    @abstractmethod
    def run(self, *args, **kwargs) -> types.Output:
        raise NotImplementedError()


class LocalExecutor:
    def run(self, wf: IWorkflow, *args, **kwargs) -> types.Output:
        return wf.run()


class WorkflowBase(IWorkflow):
    def __init__(
        self,
        registry={},
        steps: List[Step] = [],
        disable_tqdm=False,
        wf_id=None,
        wf_alias="sequence",
        executor: IExecutor = LocalDev(),
    ):
        self.registry = registry
        self.wf_id = wf_id or utils.basic_random()
        self.wf_alias = wf_alias
        self.steps = OrderedDict()
        if steps:
            for s in steps:
                self.steps[s.alias] = s
        # self.errors = []
        # self.assets = []
        self.exec_log: List[types.ExecLog] = []
        self._current_wf_id = utils.secure_random_str()
        self.wf_executions: List[str] = []
        self._disable_tqdm = disable_tqdm
        self.executor = executor

    def _get_step(self, name: str) -> Step:
        return self.steps[name]

    def _last_step(self) -> str:
        return next(reversed(self.steps))

    def _run_step(
        self,
        name: str,
        result: Optional[types.Output],
        prev_step,
        *args,
        **kwargs,
    ) -> types.Output:
        step = self._get_step(name)
        step.set_previous(prev_step)
        if not prev_step:
            future = self.executor.submit(step, *args, **kwargs)
        elif result:
            to_inject = self._inject_params(step, result)
            future = self.executor.submit(step, **to_inject)
        future.result()
        result = step.result()

        log = types.ExecLog(step_name=name, step_execid=step.execid)
        log.status = result.status
        log.error = result.error
        log.wf_exec_id = self._current_wf_id
        self.exec_log.append(log)
        return result

    def step(self, name: str, cache=None, repeat=None, raise_on_error=True):
        def decorator(f):
            @wraps(f)
            def decorated_function(*args, **kwargs):
                if name not in self.steps:
                    self.add_step(
                        name,
                        f,
                        is_async=False,
                        from_task=None,
                        raise_on_error=raise_on_error,
                    )

                result = self._run_step(name, None, None, *args, **kwargs)
                return result

            return decorated_function

        return decorator

    def add_step(
        self, name: str, func: Callable, is_async, from_task, raise_on_error
    ) -> Step:
        step = Step(func, name, is_async, from_task, raise_on_error)
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


class Sequence(WorkflowBase):
    def run(self, *args, **kwargs) -> types.Output:
        prev_step = None
        _result = None
        steps = list(self.steps)
        self._current_wf_id = utils.secure_random_str()
        self.wf_executions.append(self._current_wf_id)
        for i in trange(len(steps), desc=f"{self.wf_alias}'s iteration"):
            name = steps[i]
            _result = self._run_step(name, _result, prev_step, *args, **kwargs)
            prev_step = _result.current_step_id

        return self.steps[self._last_step()].result()


# class Parallel(WorkflowBase):
#    def run(self, *args, **kwargs) -> List[types.Output]:
#        pass
