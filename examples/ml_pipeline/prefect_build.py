"""Builds the ML Pipeline using Prefect and a local (ThreadPool)TaskRunner.

Prerequisites:
- Prefect installed: `poetry install --extras prefect`
- Use a prefect server, one of:
    * Use local prefect server:
       ```shell
       prefect server start
       ```

       then, in separate terminal:

       ```shell
       export PREFECT_API_URL="http://127.0.0.1:4200/api"
       ```

    * Use Prefect Cloud:
       Sign up at https://www.prefect.io/ then run:

       ```shell
       prefect cloud login
       ```
"""

import asyncio

from prefect import flow

from stardag.integration.prefect.build import build as prefect_build
from stardag.integration.prefect.build import create_markdown
from stardag.task import Task

from .class_api import get_metrics_dag


async def custom_callback(task):
    """Upload artifacts to Prefect Cloud for tasks that implement the special method."""
    if hasattr(task, "prefect_on_complete_artifacts"):
        for artifact in task.prefect_on_complete_artifacts():
            await artifact.create()


@flow
async def build_dag(task: Task):
    """A flow that builds any stardag Task.

    NOTE that since task is a Pydantic model, if is serialized correctly as JSON by
    prefect. This means that if this flow is deployed to Prefect Cloud, the json
    representation of any task can be submitted to the flow via the UI.
    """
    await prefect_build(
        task,
        before_run_callback=create_markdown,
        after_run_callback=custom_callback,
    )


if __name__ == "__main__":
    metrics = get_metrics_dag()
    asyncio.run(build_dag(metrics))  # type: ignore
    print(metrics.output().load())
