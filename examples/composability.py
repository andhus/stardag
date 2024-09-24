from stardag.build.sequential import build
from stardag.decorator import task


@task
def add(a: float, b: float) -> float:
    return a + b


@task
def multiply(a: float, b: float) -> float:
    return a * b


@task
def subtract(a: float, b: float) -> float:
    return a - b


if __name__ == "__main__":
    expression = add(
        a=add(a=1, b=2),
        b=subtract(
            a=multiply(a=3, b=4),
            b=5,
        ),
    )

    print(expression.model_dump_json(indent=2))
    # {
    #   "version": "0",
    #   "a": {
    #     "version": "0",
    #     "a": 1.0,
    #     "b": 2.0,
    #     "__family__": "add",
    #     "__namespace__": ""
    #   },
    #   "b": {
    #     "version": "0",
    #     "a": {
    #       "version": "0",
    #       "a": 3.0,
    #       "b": 4.0,
    #       "__family__": "multiply",
    #       "__namespace__": ""
    #     },
    #     "b": 5.0,
    #     "__family__": "subtract",
    #     "__namespace__": ""
    #   }
    # }
    build(expression)
    result = expression.output().load()
    print(result)
    # 10.0
