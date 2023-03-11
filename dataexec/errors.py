class StepExecutionError(Exception):
    def __init__(self, name):
        super().__init__(f"Step {name} execution failed")


class StepWithoutResult(Exception):
    def __init__(self, id, name):
        super().__init__(f"Step id:{id}|name:{name} doesn't return <Asset>")


class CancelledError(Exception):
    def __init__(self, name):
        super().__init__(f"Task {name} cancelled")


class TaskTimeoutError(Exception):
    def __init__(self, name):
        super().__init__(f"Task {name} time outed")
