import time

import boto3
from dagster import (
    AssetExecutionContext,
    ConfigurableResource,
    Definitions,
    MaterializeResult,
    Output,
    asset,
)


class EMRClient(ConfigurableResource):
    region_name: str

    def get_client(self):
        return boto3.client('emr', region_name=self.region_name)
    
@asset
def new_emr_cluster(context: AssetExecutionContext, emr_client: EMRClient):
    client = emr_client.get_client()

    # Define the minimal viable job flow (cluster) configuration
    cluster_config = {
        'Name': 'MinimalEMRCluster',
        'ReleaseLabel': 'emr-6.4.0',
        'LogUri': 's3://emr-testing-202406/my-emr-logs/',  # Specify your S3 bucket for logs
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': 'Master nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1
                }
            ],
            'Ec2KeyName': 'yuhan-test',  # Specify your EC2 key pair
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
        },
        'Applications': [{'Name': 'Hadoop'}, {'Name': 'Spark'}],
        'JobFlowRole': 'EMR_EC2_DefaultRole',  # Specify the IAM role for EC2
        'ServiceRole': 'EMR_DefaultRole',  # Specify the IAM role for EMR
        'VisibleToAllUsers': True
    }

    response = client.run_job_flow(**cluster_config)
    cluster_id = response['JobFlowId']
    context.log.info(f'Cluster created with ID: {cluster_id}') # `print` also works but this shows up in the same log as other Dagster logs

    # return Output here as we want to pass down the cluster_id value to the next asset
    return Output(
        value=cluster_id,
        metadata={
            "cluster_id": cluster_id, # also log the same info so it also shows up in the asset catalog
        }
    )

@asset
def emr_step_1(context: AssetExecutionContext, new_emr_cluster: str, emr_client: EMRClient):
    cluster_id = new_emr_cluster
    client = emr_client.get_client()

    # Define the step configuration
    step_config = {
        'Name': 'My Spark Job',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--class', 'org.apache.spark.examples.SparkPi',  # Example class, replace with your class
                's3://emr-testing-202406/my_spark_script.py',  # Replace with your script location
                '--output_uri', 's3://emr-testing-202406/pi-calc.json' # custom arguments to your script
            ]
        }
    }

    response = client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[step_config]
    )
    step_id = response['StepIds'][0]
    context.log.info(f'Step created with ID: {step_id}')
    _emr_waiter(context, client, cluster_id, step_id)


    # nothing to return, so we can just log the metadata by returning MaterializeResult
    return MaterializeResult( 
        metadata={
            "step_id": step_id
        }
    )

defs = Definitions(
    assets=[new_emr_cluster, emr_step_1],
    resources={
        "emr_client": EMRClient(region_name="us-west-2")
    }
)



#######################
### Helper function ###
#######################
# Workaround for the lack of a built-in better logging in EMR waiter in boto3
# https://github.com/boto/botocore/issues/2923
def _emr_waiter(context, client, cluster_id, step_id):
    # Range is number of minutes to wait
    rangeValue = 480
    for attempt in range(rangeValue):

        step_status = client.describe_step(
            ClusterId=cluster_id,
            StepId=step_id
        )

        if step_status["Step"]["Status"]["State"] == "COMPLETED":
            context.log.info(step_id + " - EMR step has finished")
            # Finished
            break

        if step_status["Step"]["Status"]["State"] == "PENDING":
            context.log.info(step_id + " - EMR step is pending")
            # Sleep for one minute
            time.sleep(60)

        if step_status["Step"]["Status"]["State"] == "RUNNING":
            context.log.info(step_id + " - EMR step is running")
            # Sleep for one minute
            time.sleep(60)

        if step_status["Step"]["Status"]["State"] == "CANCEL_PENDING":
            context.log.info(step_id + " - EMR step Failed")
            # Failed
            raise Exception(step_id + ' - Task failed with CANCEL_PENDING')

        if step_status["Step"]["Status"]["State"] == "CANCELLED":
            context.log.info(step_id + " - EMR step Failed")
            # Failed
            raise Exception(step_id + ' - Task failed with CANCELLED')

        if step_status["Step"]["Status"]["State"] == "FAILED":
            context.log.info(step_id + " - EMR step Failed")
            # Failed
            raise Exception(step_id + ' - Task failed with FAILED')

        if step_status["Step"]["Status"]["State"] == "INTERRUPTED":
            context.log.info(step_id + " - EMR step Failed")
            # Failed
            raise Exception(step_id + ' - Task failed with INTERRUPTED')

        if attempt == (rangeValue - 1):
            context.log.info(step_id + " - Task timed out")
            # Failed
            raise Exception(step_id + ' - Task timed out')