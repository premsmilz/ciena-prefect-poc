from prefect import flow
from prefect_snowflake.credentials import SnowflakeCredentials
from prefect_dbt.cloud import DbtCloudCredentials

#snowflake_target_configs = SnowflakeTargetConfigs.load("dbtcloudvelocity")
my_block1 = DbtCloudCredentials.load("dbtcloudvelocity")


@flow(name="sf-credentials-test")
def my_flow():
    my_block = SnowflakeCredentials.load("cred-sf-flybid")
    my_block1 = DbtCloudCredentials.load("dbtcloudvelocity")
    sf_connection = my_block.get_client()
    dbt_connection = my_block1.get_administrative_client()
    print (sf_connection)
    print (dbt_connection)
my_flow()