from dcdag.task import Task


class _NothingTask(Task[None]):
    def complete(self) -> bool:
        return True

    def run(self):
        pass


class UnspecifiedNamespaceTask(_NothingTask):
    pass


class OverrideNamespaceTask(_NothingTask):
    __namespace__ = "override_namespace"


class ClearNamespaceTask(_NothingTask):
    __namespace__ = ""


class CustomFamilyTask(_NothingTask, family="custom_family"):
    pass


class CustomFamilyTask2(Task[None], family="custom_family_2"):
    def complete(self) -> bool:
        return True

    def run(self):
        pass
