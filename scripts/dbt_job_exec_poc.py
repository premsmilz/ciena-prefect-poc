"""Module containing tasks and flows for interacting with dbt Cloud job runs"""
import asyncio
from datetime import datetime
import sys
from prefect import flow, get_run_logger, task
from prefect.task_runners import ConcurrentTaskRunner,SequentialTaskRunner
from prefect_dbt.cloud.jobs import TriggerJobRunOptions
from prefect_dbt.cloud.credentials import DbtCloudCredentials
from dbt_trigger_job_get_status import trigger_dbt_cloud_job_run_and_wait_for_completion

credentials = DbtCloudCredentials.load("dbtcloudvelocity")
retries: int = 0
retry_delay_seconds: int = 30

@task(name="00-dev-adhoc-run",retries=retries, retry_delay_seconds=retry_delay_seconds)
def dbt_run(jobid:int):
    asyncio.run(trigger_dbt_cloud_job_run_and_wait_for_completion(dbt_cloud_credentials=credentials,poll_frequency_seconds=30,job_id=jobid))
    #await asyncio.sleep(1)
#dbt_run(jobid=77300)

@task(name="D11-Prod-RevStream-Schedule-Loss-Contract-AP Invoice-PO Distribution Details",retries=retries, retry_delay_seconds=retry_delay_seconds)
def dbt_run_1(jobid:int):
    asyncio.run(trigger_dbt_cloud_job_run_and_wait_for_completion (
        dbt_cloud_credentials=credentials,
        poll_frequency_seconds=30,
        job_id=jobid,
        trigger_job_run_options= TriggerJobRunOptions(steps_override=[
                        "dbt build --select transform.transform_anaplan_shared_dataset.tables.tasd_anaplan_column_security_rules_transform+1"
                                                                     ] 
                                )
    )
    )
    #await asyncio.sleep(1)
#dbt_run(jobid=263116)
now = datetime.now()
currentdatetime = now.strftime("%Y%m%d%H%M%S")
print (currentdatetime)

@flow(name="dbt_job_exec_poc",flow_run_name=f"dbt_job_exec_poc-run-at-{currentdatetime}",task_runner=SequentialTaskRunner())
def dbt_job_exec_poc(p_job_id_1:int, p_job_id_2:int):
    dbt_run.submit(jobid=p_job_id_1)
    dbt_run_1.submit(jobid=p_job_id_2)

#    parallel_subflows=[dbt_run(jobid=77300),dbt_run_1(jobid=263116)]
#    await asyncio.gather(*parallel_subflows)
if __name__== "__main__":
    #main_flow(int(sys.argv[1]),int(sys.argv[2]))
    #dbt_job_exec_poc(p_job_id_1=77300,p_job_id_2=263116)
    dbt_job_exec_poc()