import json
import boto3
import os
import time


def lambda_handler(event, context):
    # glue_client=boto3.client('glue');

    # glue_client.start_crawler(Name='colaberrygluecrawlertriggerbased')

    # print('Started Glue Crawler colaberrygluecrawlertriggerbased :: ')

    # print("Glue Crawler has completed its task and triggered Glue Job To Perform Data Ingestion Now")

    # # Wait for the Glue Crawler to finish
    # while glue_client.get_crawler(Name='colaberrygluecrawlertriggerbased')['Crawler']['State'] != 'READY':
    #     print("Glue Crawler has still not finished its task")

    # print('Glue Crawler is idle now')

    # create AWS Glue client
    glue = boto3.client('glue')

    # create AWS EMR client
    emr = boto3.client('emr')

    print("event :: ", event)

    backend_code = "s3://colaberrycodefilelocationforemr/colaberrypysparkweatheranalysiscodeforemrpart.py"

    spark_submit = [
        'spark_submit',
        '--master', 'yarn',
        '--deploy-mode', 'cluster',
        backend_code
    ]

    print("Spark Submit : ", spark_submit)

    s3_input_path = os.environ['s3_input_path']

    # start AWS Glue job for data ingestion
    glue_response = glue.start_job_run(
        JobName='colaberrygluejobcleandataingestionweatherandcropdata',
        Arguments={"--s3_input_path": s3_input_path}
    )

    print('Started Glue Job colaberrygluejobcleandataingestionweatherandcropdata :: ')

    time.sleep(10)  # delay for 10 seconds

    # get job run ID for AWS Glue job
    job_run_id = glue_response['JobRunId']

    print('Glue Job is complete now')

    # start AWS EMR cluster for data analysis
    emr_response = emr.run_job_flow(
        Name='colaberrycodechallengeemrclusterforweatherdataanalysis',
        LogUri="s3://colaberryemrlogs/elasticmapreduce/",
        ReleaseLabel='emr-6.10.0',
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': "Slave",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 2,
                }
            ],
            'Ec2KeyName': 'ColaberryKeyPairEMRCluster1',
            'Ec2SubnetId': 'subnet-0475c0dc6c731d672',
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
        },
        Applications=[
            {
                'Name': 'Spark'
            },
            {
                'Name': 'Hive'
            }
        ],
        Steps=[
            {
                'Name': 'weather-analysis',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': spark_submit
                }
            }],
        BootstrapActions=[],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
    )

    # get cluster ID for AWS EMR cluster
    cluster_id = emr_response['JobFlowId']

    print("EMR has finished its task")

    # return job run ID and cluster ID
    print("Job Run ID : ", job_run_id)
    print("Cluster ID : ", cluster_id)

    return {
        'status': 'Success of EMR Job and Analyzed Weather Data has been stored in Redshift table'
    }