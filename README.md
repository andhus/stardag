# `DC-DAG` Declarative Composable DAG

Ascendent of luigi, emphasis on representing Assets and their dependencies as a declarative and composable Directed Acyclic Graph.

Why? Luigi was extremely powerful in its simplicity. Most modern frameworks makes a point out of being fully flexible and dynamic "just annotate any python function as a task and go..." kind of. `DC-DAG` takes a very different approach; the power of a framework is when it is helping (/forcing) the user to code - and even think - in a way that helps you reduce complexity and do things in one way. `Makse

Assets at the core

Why Named Asset and Not Task: A class is typically named after what _an instance_ of it is. A task it what maps a set of inputs. Prefect correctly uses the term `task`: an instance returns assets when called on inputs.

Where luigi came short:

- As an SDK -> Composabillity! Solved by allowing assets(/tasks) as inputs(/parameters).
- Orchestration and execution: Made no attempt at infra... Scheduler ran slow.

Adds:

- Opinionated Best practices build in (minimal boilerplate)
- yet fully tweakable
- modern complete type hints
- execution framework agnostic
- async tasks
- in memory caching

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

- [x] Rename back to `Task` and `task_id`
- [x] Move `.load()` to Target (!) -> Generic only in target, See Notes!
- [x] Add `context` to output, run, requires
- [ ] Use annotation instead of json_schema-hack to pass extra info about parameters
- [ ] basic unit testing
- [ ] Basic heuristic for run-time type checking of Generic type in TaskParams
- [ ] Express dynamic deps explicitly (Generic: Task[TargetT, RunT], StaticTask, -> NO just type annotate as union in base class.
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

## Notes

### Types and "API"

Put all loading under Target, all targets load something, None

So base target has load and exits

TGT[LoadT]

FSTGT also has open(r/w)

FSTGT[LoadT, StreamT]

FSTGT base class takes serialiser as arg, should comply with but does not affect generic types. It could additionally specify the file format with just some Annotated[StreamT, …]

TaskParam should if possible support (only!) target protocols, consuming task should only care about LoadT and/or StreamT

Rename back Asset to Task, the target is the asset.

The reason to move load to target is

- cleaner types
- Asset vs task…
- All targets can be created via target manager

Target = TargetManager.get_fs_tgt(task=self, path, root_key)

Can manage roots but also IN MEMORY caches based on the task instances. Can be configured per run…

FDF = Annotated[pd.DataFrame, Feather]

Autoinfer target from generic, should be possible:)

DFFeather = Annotated[bytes, DFFeatherSerializer]

Class MyTask(
Task[FST[pd.DataFrame, DFFeather]
):
…

Class Dowstream(Task[…]):
task: TaskParam[TGT[pd.DataFrame]]

FST[PydanticModel, JSONStr]

## Functional API

```python

@task
def df_transform(
      param_a: int,
      df: Depends[pd.DataFrame],
) -> pd.DataFrame:
      return df * param_a

root_task = df_transform.init(param_a=2, df=get_df.init(url=""))

@compose
def dag(
    param_a,
    url,
):
    return df_transform(param_a=param_a, df=get_df(url))

```
