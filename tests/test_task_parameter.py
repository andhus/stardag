from dcdag.auto_task import AutoFSTTask
from dcdag.task_parameter import TaskLoads, TaskParam, TaskSet


class ChildTask(AutoFSTTask[str]):
    a: str

    def run(self) -> None:
        return None


class ParentTask(AutoFSTTask[str]):
    child: TaskParam[ChildTask]

    def run(self) -> None:
        return None


def test_task_param():
    parent = ParentTask(child=ChildTask(a="A"))
    assert parent.model_dump() == {
        "version": None,
        "child": {
            "__family__": "ChildTask",
            "__namespace__": "",
            "version": None,
            "a": "A",
        },
    }
    assert parent._id_hash_jsonable() == {
        "namespace": "",
        "family": "ParentTask",
        "parameters": {
            "version": None,
            "child": parent.child.task_id,
        },
    }


class ParentTask2(AutoFSTTask[str]):
    children: TaskSet[ChildTask]

    def run(self) -> None:
        return None


def test_set_of_task_params():
    parent = ParentTask2(children=frozenset([ChildTask(a="A"), ChildTask(a="B")]))
    parent_dict = parent.model_dump(mode="json")
    assert parent_dict.keys() == {"version", "children"}
    assert parent_dict["version"] is None
    assert sorted(parent_dict["children"], key=lambda x: x["a"]) == [
        {
            "__family__": "ChildTask",
            "__namespace__": "",
            "version": None,
            "a": "A",
        },
        {
            "__family__": "ChildTask",
            "__namespace__": "",
            "version": None,
            "a": "B",
        },
    ]
    assert ParentTask2.model_validate_json(parent.model_dump_json()) == parent
    assert parent._id_hash_jsonable() == {
        "namespace": "",
        "family": "ParentTask2",
        "parameters": {
            "version": None,
            "children": sorted([child.task_id for child in parent.children]),
        },
    }


class ParentTask3(AutoFSTTask[str]):
    child: TaskLoads[str]

    def run(self) -> None:
        return None


def test_task_loads():
    parent = ParentTask3(child=ChildTask(a="A"))
    assert parent.model_dump() == {
        "version": None,
        "child": {
            "__family__": "ChildTask",
            "__namespace__": "",
            "version": None,
            "a": "A",
        },
    }
    assert parent._id_hash_jsonable() == {
        "namespace": "",
        "family": "ParentTask3",
        "parameters": {
            "version": None,
            "child": parent.child.task_id,
        },
    }
