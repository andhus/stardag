# `stardag` User Guide

For Python API level documentation, see source code. (-> TODO :))

See also `./examples` folder.

## Core Concepts

- Abstraction over the filesystem: The target location of any asset is deterministically determined by its input parameters, _before_ it has been executed.
- Each Asset has a self-contained representation of its entire upstream dependency tree -> great for reducing complexity and composability.
- Declarative: Concurrency and execution can be planned separately. has its limitations, but no framework gives it a ambitious go...
- `make`/`luigi` style bottom up execution
- Typesafe/hints, leverage pythons ecosystem around types...

## The Three Levels of the Task-API

The following three ways of specifying a `root_task`, its _dependencies_, _persistent targets_ and _serialization_ are 100% equivalent:

### The Decorator (`@task`) API

```python
from stardag.decorator import Depends, task

@task(family="Range")
def get_range(limit: int) -> list[int]:
    return list(range(limit))

@task(family="Sum")
def get_sum(integers: Depends[list[int]]) -> int:
    return sum(integers)

root_task = get_sum(integers=get_range(limit=10))
```

### Extending the `AutoFSTTask` (Auto **F**ile **S**ystem **T**arget Task)

```python
from stardag.auto_task import AutoFSTTask

class Range(AutoFSTTask[list[int]]):
    limit: int

    def run(self):
        self.output().save(list(range(self.limit)))


class Sum(AutoFSTTask[int]):
    integers: TaskLoads[list[int]]

    def requires(self):
        return self.integers

    def run(self):
        self.output().save(sum(self.integers.output().load()))


root_task = Sum(integers=Range(limit=10))
```

### Extending the base `Task`

```python
from stardag.resources import get_target
from stardag.target import LoadableSaveableFileSystemTarget
from stardag.target.serialize import JSONSerializer, Serializable
from stardag.task import Task

def default_relpath(task: Task) -> str:
    return "/".join(
        [
            task.get_namespace().replace(".", "/"),
            task.get_family(),
            task.task_id[:2],
            task.task_id[2:4],
            f"{task.task_id}.json",
        ]
    )


class Range(Task[LoadableSaveableFileSystemTarget[list[int]]]):
    limit: int

    def output(self) -> LoadableSaveableFileSystemTarget[list[int]]:
        return Serializable(
            wrapped=get_target(default_relpath(self), task=self),
            serializer=JSONSerializer(list[int]),
        )

    def run(self):
        self.output().save(list(range(self.limit)))


class Sum(Task[LoadableSaveableFileSystemTarget[int]]):
    integers: TaskLoads[list[int]]

    def requires(self):
        return self.integers

    def output(self) -> LoadableSaveableFileSystemTarget[int]:
        return Serializable(
            wrapped=get_target(default_relpath(self), task=self),
            serializer=JSONSerializer(int),
        )

    def run(self):
        return self.output().save(sum(self.integers.output().load()))

root_task =  Sum(integers=Range(limit=10))
```

In short:

- The decorator API can be used when defining a task for which all upstream dependencies are _injected_ as "task parameters". Sane defaults and type annotations are leverage to infer target location and serialization.
- The `AutoFSTTask` should be used when upstream dependencies (output of `.requires()`) needs to be _computed_ based on task input parameters. Most things, like the target path, are still easily tweakable by overriding properties/methods of the `AutoFSTTask`.
- The base `Task` should be used when we want full flexibility and/or use non-filesystem target (like a row in a DB for example).
