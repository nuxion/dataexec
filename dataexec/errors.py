class StepExecutionError(Exception):
    def __init__(self, name):
        super().__init__(f"Step {name} execution failed")
