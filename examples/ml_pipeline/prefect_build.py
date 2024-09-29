import asyncio

from prefect import flow

from stardag.integration.prefect.build import build as prefect_build
from stardag.integration.prefect.build import create_markdown

from .class_api import get_metrics_dag


async def custom_callback(task):
    if hasattr(task, "prefect_on_complete_artifacts"):
        for artifact in task.prefect_on_complete_artifacts():
            await artifact.create()


@flow
async def ml_pipeline():
    metrics_dag = get_metrics_dag()
    await prefect_build(
        metrics_dag,
        before_run_callback=create_markdown,
        after_run_callback=custom_callback,
    )
    return metrics_dag


if __name__ == "__main__":
    metrics = asyncio.run(ml_pipeline())  # type: ignore
    print(metrics.output().load())
