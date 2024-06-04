import time

import boto3

# Create a new EMR client
emr_client = boto3.client('emr', region_name='us-west-2')

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

# Create the cluster
response = emr_client.run_job_flow(**cluster_config)
cluster_id = response['JobFlowId']
print(f'Cluster created with ID: {cluster_id}')

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

# Add the step to the cluster
response = emr_client.add_job_flow_steps(
    JobFlowId=cluster_id,
    Steps=[step_config]
)

# Get the step ID
step_id = response['StepIds'][0]
print(f'Step created with ID: {step_id}')

# Wait for the step to complete
# waiter = emr_client.get_waiter("step_complete")
# waiter.wait(
#     ClusterId=cluster_id,
#     StepId=step_id,
#     WaiterConfig={
#         "Delay": 30,
#         "MaxAttempts": 10
#     }
# )
# If this get complicated, consider customizing the waiter.
#   See https://stackoverflow.com/questions/51487546/aws-python-sdk-boto3-emr-client-get-waiterstep-complete-failing
# Example:
def emr_waiter(client, cluster_id, step_id):
    # Range is number of minutes to wait
    rangeValue = 480
    for attempt in range(rangeValue):

        step_status = client.describe_step(
            ClusterId=cluster_id,
            StepId=step_id
        )

        if step_status["Step"]["Status"]["State"] == "COMPLETED":
            print(step_id + " - EMR step has finished")
            # Finished
            break

        if step_status["Step"]["Status"]["State"] == "PENDING":
            print(step_id + " - EMR step is pending")
            # Sleep for one minute
            time.sleep(60)

        if step_status["Step"]["Status"]["State"] == "RUNNING":
            print(step_id + " - EMR step is running")
            # Sleep for one minute
            time.sleep(60)

        if step_status["Step"]["Status"]["State"] == "CANCEL_PENDING":
            print(step_id + " - EMR step Failed")
            # Failed
            raise Exception(step_id + ' - Task failed with CANCEL_PENDING')

        if step_status["Step"]["Status"]["State"] == "CANCELLED":
            print(step_id + " - EMR step Failed")
            # Failed
            raise Exception(step_id + ' - Task failed with CANCELLED')

        if step_status["Step"]["Status"]["State"] == "FAILED":
            print(step_id + " - EMR step Failed")
            # Failed
            raise Exception(step_id + ' - Task failed with FAILED')

        if step_status["Step"]["Status"]["State"] == "INTERRUPTED":
            print(step_id + " - EMR step Failed")
            # Failed
            raise Exception(step_id + ' - Task failed with INTERRUPTED')

        if attempt == (rangeValue - 1):
            print(step_id + " - Task timed out")
            # Failed
            raise Exception(step_id + ' - Task timed out')
        
emr_waiter(emr_client, cluster_id, step_id)
        
# # Terminate the cluster
# response = emr_client.terminate_job_flows(
#     JobFlowIds=[cluster_id]
# )
# print(f'Cluster {cluster_id} is being terminated.')