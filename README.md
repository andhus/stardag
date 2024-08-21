# `DC-DAG` Declarative Composable DAG

Ascendent of luigi, emphasis on representing Assets and their dependencies as a declarative and composable Directed Acyclic Graph.

Why? Luigi was extremely powerful in its simplicity. Most modern frameworks makes a point out of being fully flexible and dynamic "just annotate any python function as a task and go..." kind of. `DC-DAG` takes a very different approach; the power of a framework is when it is helping (/forcing) the user to code - and even think - in a way that helps you reduce complexity and do things in one way. `Makse

Assets at the core

Why Named Asset and Not Task: A class is typically named after what _an instance_ of it is. A task it what maps a set of inputs. Prefect correctly uses the term `task`: an instance returns assets when called on inputs.

Where luigi came short:

- As an SDK -> Composabillity! Solved by allowing assets(/tasks) as inputs(/parameters).
- Orchestration and execution: Made no attempt at infra... Scheduler ran slow.

Comparison

Dagster: has assets but the are not generally parametrizable
Prefect: No real concept of assets. Task execution is tracked not assets. Each task is not self contained represetation of its dependencies.

Main idea

- Abstraction over the filesystem: The target location of any asset is deterministically determined by its input parameters, _before_ it has been executed.
- Each Assset has a self-contained representation of its entire upstream dependency tree -> great for reducing complexity and composability.
- Declarative: Concurrency and execution can be planned separately. has its limitiations, but no framework gives it a ambitious go...
- `make`/`luigi` style
- Typesafe/hints, leverage pythons ecosystem around types...

## TODO

### Basics

- [ ] Rename back to `Task` and `task_id`
- [ ] Move `.load()` to Target (!) -> Generic only in target
- [ ] Use annotation instead of json_schema-hack to pass extra info about parameters
- [ ] basic unit testing
- [ ] Express dynamic deps explicitly (Generic: Task[TargetT, RunT], StaticTask,
      DynamicTask) or just class variable `has_dynamic_dependencies: bool` (possible to
      overload type hints on this? Yes: <https://stackoverflow.com/questions/70136046/can-you-type-hint-overload-a-return-type-for-a-method-based-on-an-argument-passe>
      but probably overkill)

### Features

- [ ] FileSystemTargets (luigi style or alternative?)
- [ ] Serialization, based on type annotation of task target?
-

### Execution

- [ ] Luigi runner
- [ ] Prefect runner
