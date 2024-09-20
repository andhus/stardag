# NOTEs and TODOs

## TODO

### Basics

- [x] Rename back to `Task` and `task_id`
- [x] Move `.load()` to Target (!) -> Generic only in target, See Notes!
- [ ] ~~Add `context` to output, run, requires~~
- [x] Use annotation instead of json_schema-hack to pass extra info about parameters
- [ ] basic unit testing
  - [ ] Add fixture for auto clearing the registry
  - [ ] Add testing util for registering tasks defined outside of test function
- [ ] Basic heuristic for run-time type checking of Generic type in TaskParams
- [ ] Express dynamic deps explicitly (Generic: Task[TargetT, RunT], StaticTask, -> NO just type annotate as union in base class.
      DynamicTask) or just class variable `has_dynamic_dependencies: bool` (possible to
      overload type hints on this? Yes: <https://stackoverflow.com/questions/70136046/can-you-type-hint-overload-a-return-type-for-a-method-based-on-an-argument-passe>
      but probably overkill)

### Features

- [ ] FileSystemTargets
  - [ ] Atomic Writes (copy luigi approach?)
  - [ ] S3
  - [ ] GS
- [x] Serialization -> AutoTask
  - [x] Module structure
    - [x] Rename to just `AutoTask`?
  - [ ] ~~Extend Interface of Serializer to have `.init(annotation)` after initialization -> This way you can set additional tuning parameters up front (without partials), and compose serializers (see below: `GZip(JSON())`) and property~~
  - [x] `default_ext: str`
  - [x] Make serializer initialization happen on task declaration for early errors! Use `__pydantic_init_subclass__`
  - [x] Allow specifying explicit serializer: `AutoTask[Feather[pd.DataFrame]]` = `AutoTask[Annotated[pd.DataFrame, PandasFeatherSerializer()]]`
  - [ ] Defaults for:
    - [x] anything JSONAble (pydantic)
    - [x] pd.DataFrame
    - [ ] pd.Series
    - [ ] numpy array
    - [x] Fallback on Pickle
  - [ ] (`GZip[JSON[dict[str, str]]]` = `GZipJSON[dict[str, str]]` = `Annotated[dict[str, str], GZip(JSON())]` ?)
  - [ ] Set `.ext` based on serializer. I.e. add `_relpath_ext` as a property, which by default reads from self.serializer.default_ext
- [ ] function decorator API
  - [x] PoC
  - [x] basic unit tests
- [ ] `ml_pipeline` example
  - [ ] Make output from class_api and decorator_api equivalent
  - [ ] Add units test (compare state of InMemoryTarget)
  - [x] Blocked by some serialization fixes

### Execution

- [x] Basic sequential
- [ ] Luigi runner?
- [ ] Prefect runner
- [ ] Modal runner (+ "deployment")

### Release

- [ ] repo and package name (`stardag`, `*dag`, :star: dag)
- [ ] PyPI name
- [ ] Github Workflows
- [ ] unit test (tox??) with poetry
- [ ] Relase package with poetry spec.?
- [ ] Cleanup README, basic Docs and overview of core features
