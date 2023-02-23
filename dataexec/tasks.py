from functools import wraps
from typing import Callable, List
from pydantic import BaseModel

class ExecLog:
    execid: str
    name: str
    status: int
    msg: str


class Workflow:
    def __init__(self, registry={}):
        self.registry = registry
        self.tasks = []
        self.errors = []
        self.assets = []
        self.exec_log: List[ExecLog] = []

    def _exec_task(name: str):
        pass

    def task(self, name: str):
        def decorator(f):
            @wraps(f)
            def decorated_function(*args, **kwargs):
                self.add_task(name, f)
                result = f(*args, **kwargs)
                return result

            return decorated_function

        return decorator

    def add_task(self, name: str, task: Callable):
        self.tasks.append((name, task))

    def run(self):
        for n, task in self.tasks:
            print(f"Running task {n}")
            try:
                task()
            except Exception as e:
                self.errors.append({n: str(e)})
