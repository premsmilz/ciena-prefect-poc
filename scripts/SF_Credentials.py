import asyncio
from prefect import flow,get_run_logger,task
from prefect_snowflake.credentials import SnowflakeCredentials
from prefect_dbt.cloud import DbtCloudCredentials, DbtCloudJob
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run,trigger_dbt_cloud_job_run_and_wait_for_completion,TriggerJobRunOptions
from prefect.task_runners import SequentialTaskRunner
import datetime

credentials = DbtCloudCredentials.load("dbtcloudvelocity")
def trigger_dbt_cloud_job_run_flow(jobid:int):
    credentials = DbtCloudCredentials.load("dbtcloudvelocity")
    #trigger_dbt_cloud_job_run(dbt_cloud_credentials=credentials, job_id=77300)
    trigger_dbt_cloud_job_run_and_wait_for_completion (
        dbt_cloud_credentials=credentials,
        poll_frequency_seconds=30,
        trigger_job_run_options= TriggerJobRunOptions(steps_override=[
                        "dbt build --select transform.transform_anaplan_shared_dataset.tables.tasd_anaplan_column_security_rules_transform+1"
                                                                     ] 
                                ),
        job_id=jobid
    )

@flow(flow_run_name="{name}-on-{date:%A}")
def my_flow(name: str, date: datetime.datetime):
    pass
# creates a flow run called 'marvin-on-Thursday'
#my_flow(name="marvin", date=datetime.datetime.utcnow())


@flow(name="00-dev-adhoc-run")
def dbt_run(jobid:int):
    trigger_dbt_cloud_job_run_flow(jobid)
    #await asyncio.sleep(1)
#dbt_run(jobid=77300)

@flow(name="D11-Prod-RevStream-Schedule-Loss-Contract-AP Invoice-PO Distribution Details")
def dbt_run_1(jobid:int):
    trigger_dbt_cloud_job_run_and_wait_for_completion (
        dbt_cloud_credentials=credentials,
        poll_frequency_seconds=30,
        job_id=jobid
    )
    #await asyncio.sleep(1)
#dbt_run(jobid=263116)

@flow(name="test-dbt-jobs")
def main_flow():
    dbt_run(jobid=77300)
    dbt_run_1(jobid=263116)
#    parallel_subflows=[dbt_run(jobid=77300),dbt_run_1(jobid=263116)]
#    await asyncio.gather(*parallel_subflows)
main_flow()
#if __name__ == "__main__":
    #asyncio.run(main_flow())
#    asyncio.run(trigger_dbt_cloud_job_run_flow())


#@flow(name="sf-credentials-test")
#def my_flow():
 #   my_block = SnowflakeCredentials.load("cred-sf-flybid")
  #  my_block1 = DbtCloudCredentials.load("dbtcloudvelocity")
   # sf_connection = my_block.get_client()
    #dbt_connection = my_block1.get_administrative_client()
    #print (sf_connection)
    #print (dbt_connection)
#my_flow()

#@flow
#def my_flow():
#    my_block = SnowflakeCredentials.load("cred-sf-flybid")
#    return my_block
#my_flow()








