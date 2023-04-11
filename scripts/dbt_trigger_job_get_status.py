"""Module containing tasks and flows for interacting with dbt Cloud job runs"""
import asyncio
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

from httpx import HTTPStatusError
from prefect import flow, get_run_logger, task
from typing_extensions import Literal
from prefect.task_runners import ConcurrentTaskRunner,SequentialTaskRunner

from prefect_dbt.cloud.credentials import DbtCloudCredentials
from prefect_dbt.cloud.exceptions import (
    DbtCloudGetRunArtifactFailed,
    DbtCloudGetRunFailed,
    DbtCloudJobRunTimedOut,
    DbtCloudListRunArtifactsFailed,
)
from prefect_dbt.cloud.utils import extract_user_message

"""Module containing tasks and flows for interacting with dbt Cloud jobs"""
import asyncio
import shlex
import time
from json import JSONDecodeError
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

from httpx import HTTPStatusError
from prefect import flow, get_run_logger, task
from prefect.blocks.abstract import JobBlock, JobRun
from prefect.context import FlowRunContext
from prefect.utilities.asyncutils import sync_compatible
from pydantic import Field
from typing_extensions import Literal

from prefect_dbt.cloud.credentials import DbtCloudCredentials
from prefect_dbt.cloud.exceptions import (
    DbtCloudGetJobFailed,
    DbtCloudGetRunArtifactFailed,
    DbtCloudGetRunFailed,
    DbtCloudJobRunCancelled,
    DbtCloudJobRunFailed,
    DbtCloudJobRunIncomplete,
    DbtCloudJobRunTimedOut,
    DbtCloudJobRunTriggerFailed,
    DbtCloudListRunArtifactsFailed,
)
from prefect_dbt.cloud.models import TriggerJobRunOptions
from prefect_dbt.cloud.runs import (
    DbtCloudJobRunStatus
    #get_dbt_cloud_run_artifact
    #get_dbt_cloud_run_info
    #list_dbt_cloud_run_artifacts,
    #wait_for_dbt_cloud_job_run,
)
from prefect_dbt.cloud.utils import extract_user_message

EXE_COMMANDS = ("build", "run", "test", "seed", "snapshot")


class DbtCloudJobRunStatus(Enum):
    """dbt Cloud Job statuses."""

    QUEUED = 1
    STARTING = 2
    RUNNING = 3
    SUCCESS = 10
    FAILED = 20
    CANCELLED = 30

    @classmethod
    def is_terminal_status_code(cls, status_code: Any) -> bool:
        """
        Returns True if a status code is terminal for a job run.
        Returns False otherwise.
        """
        return status_code in [cls.SUCCESS.value, cls.FAILED.value, cls.CANCELLED.value]
    
    ####################
credentials = DbtCloudCredentials.load("dbtcloudvelocity")
#@task(
#   name="Wait for dbt Cloud job run",
#   description="Waits for a dbt Cloud job run to finish running.",
#)

async def get_dbt_cloud_run_info(
    dbt_cloud_credentials: DbtCloudCredentials,
    run_id: int,
    include_related: Optional[
        List[Literal["trigger", "job", "debug_logs", "run_steps"]]
    ] = None,
) -> Dict:
    """
    A task to retrieve information about a dbt Cloud job run.

    Args:
        dbt_cloud_credentials: Credentials for authenticating with dbt Cloud.
        run_id: The ID of the job to trigger.
        include_related: List of related fields to pull with the run.
            Valid values are "trigger", "job", "debug_logs", and "run_steps".
            If "debug_logs" is not provided in a request, then the included debug
            logs will be truncated to the last 1,000 lines of the debug log output file.

    Returns:
        The run data returned by the dbt Cloud administrative API.

    Example:
        Get status of a dbt Cloud job run:
        ```python
        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import get_run

        @flow
        def get_run_flow():
            credentials = DbtCloudCredentials(api_key="my_api_key", account_id=123456789)

            return get_run(
                dbt_cloud_credentials=credentials,
                run_id=42
            )

        get_run_flow()
        ```
    """  # noqa
    try:
        async with dbt_cloud_credentials.get_administrative_client() as client:
            response = await client.get_run(
                run_id=run_id, include_related=include_related
            )
    except HTTPStatusError as ex:
        raise DbtCloudGetRunFailed(extract_user_message(ex)) from ex
    return response.json()["data"]

async def get_dbt_cloud_run_artifact(
    dbt_cloud_credentials: DbtCloudCredentials,
    run_id: int,
    path: str,
    step: Optional[int] = None,
) -> Union[Dict, str]:
    """
    A task to get an artifact generated for a completed run. The requested artifact
    is saved to a file in the current working directory.

    Args:
        dbt_cloud_credentials: Credentials for authenticating with dbt Cloud.
        run_id: The ID of the run to list run artifacts for.
        path: The relative path to the run artifact (e.g. manifest.json, catalog.json,
            run_results.json)
        step: The index of the step in the run to query for artifacts. The
            first step in the run has the index 1. If the step parameter is
            omitted, then this method will return the artifacts compiled
            for the last step in the run.

    Returns:
        The contents of the requested manifest. Returns a `Dict` if the
            requested artifact is a JSON file and a `str` otherwise.

    Examples:
        Get an artifact of a dbt Cloud job run:
        ```python
        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.runs import get_dbt_cloud_run_artifact

        @flow
        def get_artifact_flow():
            credentials = DbtCloudCredentials(api_key="my_api_key", account_id=123456789)

            return get_dbt_cloud_run_artifact(
                dbt_cloud_credentials=credentials,
                run_id=42,
                path="manifest.json"
            )

        get_artifact_flow()
        ```

        Get an artifact of a dbt Cloud job run and write it to a file:
        ```python
        import json

        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import get_dbt_cloud_run_artifact

        @flow
        def get_artifact_flow():
            credentials = DbtCloudCredentials(api_key="my_api_key", account_id=123456789)

            get_run_artifact_result = get_dbt_cloud_run_artifact(
                dbt_cloud_credentials=credentials,
                run_id=42,
                path="manifest.json"
            )

            with open("manifest.json", "w") as file:
                json.dump(get_run_artifact_result, file)

        get_artifact_flow()
        ```
    """  # noqa

    try:
        async with dbt_cloud_credentials.get_administrative_client() as client:
            response = await client.get_run_artifact(
                run_id=run_id, path=path, step=step
            )
    except HTTPStatusError as ex:
        raise DbtCloudGetRunArtifactFailed(extract_user_message(ex)) from ex

    if path.endswith(".json"):
        artifact_contents = response.json()
    else:
        artifact_contents = response.text

    return artifact_contents


async def wait_for_dbt_cloud_job_run(
    run_id: int,
    dbt_cloud_credentials: DbtCloudCredentials,
    max_wait_seconds: int = 900,
    poll_frequency_seconds: int = 10,
) -> Tuple[DbtCloudJobRunStatus, Dict]:
    """
    Waits for the given dbt Cloud job run to finish running.

    Args:
        run_id: The ID of the run to wait for.
        dbt_cloud_credentials: Credentials for authenticating with dbt Cloud.
        max_wait_seconds: Maximum number of seconds to wait for job to complete
        poll_frequency_seconds: Number of seconds to wait in between checks for
            run completion.

    Raises:
        DbtCloudJobRunTimedOut: When the elapsed wait time exceeds `max_wait_seconds`.

    Returns:
        run_status: An enum representing the final dbt Cloud job run status
        run_data: A dictionary containing information about the run after completion.


    Example:


    """
    logger = get_run_logger()
    seconds_waited_for_run_completion = 0
    wait_for = []
    while seconds_waited_for_run_completion <= max_wait_seconds:
        run_data_future = await get_dbt_cloud_run_info(
            dbt_cloud_credentials=dbt_cloud_credentials,
            run_id=run_id,
            include_related= [
                            List[Literal["run_steps"]]
                             ]
        )
        #run_data = await run_data_future.result()
        #run_status_code = run_data.get("status")
        run_data = run_data_future
        #run_data = run_data_future
        run_status_code = (run_data_future.get("status"))

        if DbtCloudJobRunStatus.is_terminal_status_code(run_status_code):
            return DbtCloudJobRunStatus(run_status_code), run_data

        wait_for = [run_data_future]
        try:
            latest_step = max(run_data_future["run_steps"], key=lambda _: _["index"])
            #print (latest_step)
            step_name = latest_step["name"]
        except:
            step_name = []
            pass
        logger.info(
            "dbt Cloud job run with ID %i has status %s. Running at step %s. Next status check in %i seconds.",
            run_id,
            DbtCloudJobRunStatus(run_status_code).name,
            step_name,
            poll_frequency_seconds,
        )
        await asyncio.sleep(poll_frequency_seconds)
        seconds_waited_for_run_completion += poll_frequency_seconds

    raise DbtCloudJobRunTimedOut(
        f"Max wait time of {max_wait_seconds} seconds exceeded while waiting "
        "for job run with ID {run_id}"
    )

#@task(
#    name="Trigger dbt Cloud job run",
#    description="Triggers a dbt Cloud job run for the job "
#    "with the given job_id and optional overrides.",
#    retries=3,
#    retry_delay_seconds=10,
#)
async def trigger_dbt_cloud_job_run(
    dbt_cloud_credentials: DbtCloudCredentials,
    job_id: int,
    options: Optional[TriggerJobRunOptions] = None,
) -> Dict:
    """
    A task to trigger a dbt Cloud job run.

    Args:
        dbt_cloud_credentials: Credentials for authenticating with dbt Cloud.
        job_id: The ID of the job to trigger.
        options: An optional TriggerJobRunOptions instance to specify overrides
            for the triggered job run.

    Returns:
        The run data returned from the dbt Cloud administrative API.

    Examples:
        Trigger a dbt Cloud job run:
        ```python
        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run


        @flow
        def trigger_dbt_cloud_job_run_flow():
            credentials = DbtCloudCredentials(api_key="my_api_key", account_id=123456789)

            trigger_dbt_cloud_job_run(dbt_cloud_credentials=credentials, job_id=1)


        trigger_dbt_cloud_job_run_flow()
        ```

        Trigger a dbt Cloud job run with overrides:
        ```python
        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run
        from prefect_dbt.cloud.models import TriggerJobRunOptions


        @flow
        def trigger_dbt_cloud_job_run_flow():
            credentials = DbtCloudCredentials(api_key="my_api_key", account_id=123456789)

            trigger_dbt_cloud_job_run(
                dbt_cloud_credentials=credentials,
                job_id=1,
                options=TriggerJobRunOptions(
                    git_branch="staging",
                    schema_override="dbt_cloud_pr_123",
                    dbt_version_override="0.18.0",
                    target_name_override="staging",
                    timeout_seconds_override=3000,
                    generate_docs_override=True,
                    threads_override=8,
                    steps_override=[
                        "dbt seed",
                        "dbt run --fail-fast",
                        "dbt test --fail fast",
                    ],
                ),
            )


        trigger_dbt_cloud_job_run()
        ```
    """  # noqa
    logger = get_run_logger()

    logger.info(f"Triggering run for job with ID {job_id}")

    try:
        async with dbt_cloud_credentials.get_administrative_client() as client:
            response = await client.trigger_job_run(job_id=job_id, options=options)
    except HTTPStatusError as ex:
        raise DbtCloudJobRunTriggerFailed(extract_user_message(ex)) from ex

    run_data = response.json()["data"]

    if "project_id" in run_data and "id" in run_data:
        logger.info(
            f"Run successfully triggered for job with ID {job_id}. "
            "You can view the status of this run at "
            f"https://{dbt_cloud_credentials.domain}/#/accounts/"
            f"{dbt_cloud_credentials.account_id}/projects/{run_data['project_id']}/"
            f"runs/{run_data['id']}/"
        )

    return run_data

async def list_dbt_cloud_run_artifacts(
    dbt_cloud_credentials: DbtCloudCredentials, run_id: int, step: Optional[int] = None
) -> List[str]:
    """
    A task to list the artifact files generated for a completed run.

    Args:
        dbt_cloud_credentials: Credentials for authenticating with dbt Cloud.
        run_id: The ID of the run to list run artifacts for.
        step: The index of the step in the run to query for artifacts. The
            first step in the run has the index 1. If the step parameter is
            omitted, then this method will return the artifacts compiled
            for the last step in the run.

    Returns:
        A list of paths to artifact files that can be used to retrieve the generated artifacts.

    Example:
        List artifacts of a dbt Cloud job run:
        ```python
        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import list_dbt_cloud_run_artifacts

        @flow
        def list_artifacts_flow():
            credentials = DbtCloudCredentials(api_key="my_api_key", account_id=123456789)

            return list_dbt_cloud_run_artifacts(
                dbt_cloud_credentials=credentials,
                run_id=42
            )

        list_artifacts_flow()
        ```
    """  # noqa
    try:
        async with dbt_cloud_credentials.get_administrative_client() as client:
            response = await client.list_run_artifacts(run_id=run_id, step=step)
    except HTTPStatusError as ex:
        raise DbtCloudListRunArtifactsFailed(extract_user_message(ex)) from ex
    return response.json()["data"]

'''
@task(
    name="Trigger dbt Cloud job run and wait for completion",
    description="Triggers a dbt Cloud job run and waits for the"
    "triggered run to complete.",
)
'''
async def trigger_dbt_cloud_job_run_and_wait_for_completion(
    dbt_cloud_credentials: DbtCloudCredentials,
    job_id: int,
    trigger_job_run_options: Optional[TriggerJobRunOptions] = None,
    max_wait_seconds: int = 900,
    poll_frequency_seconds: int = 10,
    retry_filtered_models_attempts: int = 3,
) -> Dict:
    """
    Flow that triggers a job run and waits for the triggered run to complete.

    Args:
        dbt_cloud_credentials: Credentials for authenticating with dbt Cloud.
        job_id: The ID of the job to trigger.
        trigger_job_run_options: An optional TriggerJobRunOptions instance to
            specify overrides for the triggered job run.
        max_wait_seconds: Maximum number of seconds to wait for job to complete
        poll_frequency_seconds: Number of seconds to wait in between checks for
            run completion.
        retry_filtered_models_attempts: Number of times to retry models selected by `retry_status_filters`.

    Raises:
        DbtCloudJobRunCancelled: The triggered dbt Cloud job run was cancelled.
        DbtCloudJobRunFailed: The triggered dbt Cloud job run failed.
        RuntimeError: The triggered dbt Cloud job run ended in an unexpected state.

    Returns:
        The run data returned by the dbt Cloud administrative API.

    Examples:
        Trigger a dbt Cloud job and wait for completion as a stand alone flow:
        ```python
        import asyncio
        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run_and_wait_for_completion

        asyncio.run(
            trigger_dbt_cloud_job_run_and_wait_for_completion(
                dbt_cloud_credentials=DbtCloudCredentials(
                    api_key="my_api_key",
                    account_id=123456789
                ),
                job_id=1
            )
        )
        ```

        Trigger a dbt Cloud job and wait for completion as a sub-flow:
        ```python
        from prefect import flow
        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run_and_wait_for_completion

        @flow
        def my_flow():
            ...
            run_result = trigger_dbt_cloud_job_run_and_wait_for_completion(
                dbt_cloud_credentials=DbtCloudCredentials(
                    api_key="my_api_key",
                    account_id=123456789
                ),
                job_id=1
            )
            ...

        my_flow()
        ```

        Trigger a dbt Cloud job with overrides:
        ```python
        import asyncio
        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run_and_wait_for_completion
        from prefect_dbt.cloud.models import TriggerJobRunOptions

        asyncio.run(
            trigger_dbt_cloud_job_run_and_wait_for_completion(
                dbt_cloud_credentials=DbtCloudCredentials(
                    api_key="my_api_key",
                    account_id=123456789
                ),
                job_id=1,
                trigger_job_run_options=TriggerJobRunOptions(
                    git_branch="staging",
                    schema_override="dbt_cloud_pr_123",
                    dbt_version_override="0.18.0",
                    target_name_override="staging",
                    timeout_seconds_override=3000,
                    generate_docs_override=True,
                    threads_override=8,
                    steps_override=[
                        "dbt seed",
                        "dbt run --fail-fast",
                        "dbt test --fail fast",
                    ],
                ),
            )
        )
        ```
    """  # noqa
    logger = get_run_logger()

    triggered_run_data_future = await trigger_dbt_cloud_job_run(
        dbt_cloud_credentials=dbt_cloud_credentials,
        job_id=job_id,
        options=trigger_job_run_options,
    )
    #run_data = await triggered_run_data_future.result()
    #run_id = run_data.get("id")
    run_id = (triggered_run_data_future.get("id"))
    #run_id = (await triggered_run_data_future.run_data()).get("id")
    if run_id is None:
        raise RuntimeError("Unable to determine run ID for triggered job.")

    #print ("Job Triggered")

    final_run_status, run_data = await wait_for_dbt_cloud_job_run(
        run_id=run_id,
        dbt_cloud_credentials=dbt_cloud_credentials,
        max_wait_seconds=max_wait_seconds,
        poll_frequency_seconds=poll_frequency_seconds,
    )

    #print ("Wait for job run to complete")
    if final_run_status == DbtCloudJobRunStatus.SUCCESS:
        logger.info(
            "dbt Cloud job run with ID %s completed successfully!",
            run_id,
        )
        return run_data
    elif final_run_status == DbtCloudJobRunStatus.CANCELLED:
        raise DbtCloudJobRunCancelled(
            f"Triggered job run with ID {run_id} was cancelled."
        )
    elif final_run_status == DbtCloudJobRunStatus.FAILED:
        while retry_filtered_models_attempts > 0:
            logger.info(
                f"Retrying job run with ID: {run_id} "
                f"{retry_filtered_models_attempts} more times"
            )
        else:
            raise DbtCloudJobRunFailed(f"Triggered job run with ID: {run_id} failed.")
    else:
        raise RuntimeError(
            f"Triggered job run with ID: {run_id} ended with unexpected"
            f"status {final_run_status.value}."
        )
'''
@task(name="00-dev-adhoc-run")
def dbt_run(jobid:int):
    asyncio.run(trigger_dbt_cloud_job_run_and_wait_for_completion(dbt_cloud_credentials=credentials, poll_frequency_seconds=30,job_id=jobid))
    #await asyncio.sleep(1)
#dbt_run(jobid=77300)
'''