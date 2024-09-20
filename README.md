# `*DAG` - Declarative & Composable DAGs

`stardag`'s primary objective is to provide a clean Python API for representing persistently stored assets - the code that produce them and their dependencies - as a declarative Directed Acyclic Graph (DAG). As such, it is a natural descendant of [Luigi](https://github.com/spotify/luigi). It emphasizes ease of use and _composability_.

Stardag is built on top of, any integrates well with, Pydantic and utilizes expressive type annotations to reduce boilerplate and clarify io-contracts of tasks.

## Getting started

### Installation

```sh
pip install stardag
```

### Hello World

```python
from stardag.decorator import Depends, task

@task
def get_range(limit: int) -> list[int]:
    return list(range(limit))

@task
def get_sum(integers: Depends[list[int]]) -> int:
    return sum(integers)

# Declarative/"lazy" specification of DAG, no computation yet.
task = get_sum(integers=get_range(limit=10))

print(repr(task))
# get_sum(version='0', integers=get_range(version='0', limit=10))
print(repr(task.requires()))
# {'integers': get_range(version='0', limit=10)}
print(task.output().path)
# /path/to/stardag-target-roots/default/get_sum/v0/8f/ea/8fea194424a5b2acaa03b0b53fb228b60b2a5ac6.json'
print(task.complete())
# False

# Materialize task targets
from stardag.build.sequential import build

build(task)
# ...

print(task.complete())
# True

print(task.output().load())
# 45

# `task` is a Pydantic `BaseModel`
from pydantic import BaseModel
assert isinstance(task, BaseModel)
print(task.model_dump_json(indent=2))
# {
#   "version": "0",
#   "integers": {
#     "version": "0",
#     "limit": 10,
#     "__family__": "get_range",
#     "__namespace__": ""
#   }
# }


# Call original functions, no targets persisted
res = get_sum.call(get_range.call(10))
print(res)
# 45
```

## Why yet another Python DAG-framework?

Luigi is extremely powerful in its simplicity, but outdated. Most modern frameworks makes a point out of being fully flexible and dynamic "just annotate any python function as a task and go..." kind of. `stardag` takes a different approach; the power of a framework is when it is helping the user to code - and even think - in a way that helps you reduce complexity and provides a structured way of doing things.

That said, the declarative DAG abstraction is _not_ suitable for all data processing/workflows, that's why it is `stardag`s ambition to be _interoperable_ with other modern data workflow frameworks (Prefect, Dagster, Modal, ...) that lacks a clean enough declarative DAG abstraction, both as an sdk and at the execution layer.

## Why not just use Luigi then?

A lot has happened in the ecosystem since Luigi was created and it is not really under active development. In my opinion, where luigi falls short as an SDK is in its _lack of composability_ of Tasks; promoting tightly coupled DAGs and "parameter explosion" a classical case of ["Composition over inheritance"](#composability-ftw)

- As an SDK -> _Composability_! (Solved by allowing tasks as inputs(/parameters).
- Orchestration and execution: Made intentionally no attempt at orchestration... Scheduler ran slow etc.
- Minimalistic (for good and bad) a lot of boilerplate to get it "production ready".

Adds:

- Opinionated best practices out of the box (minimal boilerplate)
- yet fully tweakable
- modern complete type hints
- execution framework agnostic
- (planned: asyncio tasks)
- (planned: in memory caching)

## What's missing in modern data workflow frameworks?

The clean declarative DAG abstraction, for when it is the best option. This is often the case in Machine Learning development or generally when we:

- work with large files
- care deeply about reproducibility and explicit asset dependencies
- want [Make](https://www.gnu.org/software/make/)-style bottom up" execution

### Comparison

#### Dagster

Has assets and their lineage as first class citizens, but they are not generally parametrizable (requires convolved factory pattern) also tightly coupled to the "platform".

(TODO explain with examples).

#### Prefect

Only has the notion of task (run) dependencies _after the fact_, when tasks have ran. No bottom up execution - in this case you are advised manage the persistent target outside of the primary framework. This is exactly what you need something like luigi or stardag for (non-trivial to scale). The lack of bottom up execution also makes the built-in caching mechanism - hashing of actual input _data_ instead of the declarative specification of dito - extremely inefficient in common cases.

(TODO: add concrete example)

## Composability FTW

See [Composition over inheritance](https://en.wikipedia.org/wiki/Composition_over_inheritance)

### Lack of Composability in Luigi

Summary:

- No built-in parameter (hashing and serialization is not trivial) for data classes/pydantic models, promotes a _flat_ parameter set (because of emphasis of CLI triggering?)
- No built-in parameter for a Task _instance_ (which is the obvious way to achive composability) and not trivial to implement (again: serialization and hashing).
- `requires`/`inherits` promotes inheritance, where composability is a clearly superior abstraction in most cases.
- Lack of modern type hinting, which is helpful to support composability.
