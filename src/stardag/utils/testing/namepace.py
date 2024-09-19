from stardag.task import Task


class _DoNothing(Task[None]):
    def complete(self) -> bool:
        return True

    def run(self):
        pass


class UnspecifiedNamespace(_DoNothing):
    pass


class OverrideNamespaceByDUnder(_DoNothing):
    __namespace__ = "override_namespace"


class ClearNamespaceByDunder(_DoNothing):
    __namespace__ = ""


class OverrideNamespaceByDUnderChild(OverrideNamespaceByDUnder):
    pass


class OverrideNamespaceByArg(_DoNothing, namespace_override="override_namespace"):
    pass


class ClearNamespaceByArg(_DoNothing, namespace_override=""):
    pass


class OverrideNamespaceByArgChild(OverrideNamespaceByArg):
    pass


class CustomFamilyByArgFromIntermediate(_DoNothing, family_override="custom_family"):
    """Uses family_override with intermediate task implementation inheritance."""

    pass


class CustomFamilyByArgFromTask(Task[None], family_override="custom_family_2"):
    """Uses family_override with base task."""

    def complete(self) -> bool:
        return True

    def run(self):
        pass


class CustomFamilyByDUnder(_DoNothing):
    """Children would have to override either namespace or family (almost never makes
    sense to use this)"""

    __family__ = "custom_family_3"


class CustomFamilyByArgFromIntermediateChild(CustomFamilyByArgFromIntermediate):
    """Should not inherit family_override."""

    pass


class CustomFamilyByArgFromTaskChild(CustomFamilyByArgFromTask):
    """Should not inherit family_override."""

    pass


try:

    class CustomFamilyByDUnderChild(CustomFamilyByDUnder):  # type: ignore
        """Must override __family__."""

        pass

except ValueError:

    class CustomFamilyByDUnderChild(
        CustomFamilyByDUnder, family_override="custom_family_3_child"
    ):
        """Must override __family__."""

        pass
