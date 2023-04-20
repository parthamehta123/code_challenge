import boto3
import os
import time
import json


def lambda_handler(event, context):
    emr_response = None
    glue_response = None
    print("event :: ", event)
    glue = boto3.client('glue')
    emr = boto3.client('emr', region_name='us-east-2', aws_access_key_id='AKIA3WEVIQ3BMQ3GU7VY',
                       aws_secret_access_key='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')
    backend_code = "s3://colaberrycodefilelocationforemr/colaberrypysparkweatheranalysiscodeforemrpart.py"

    # Extract the S3 bucket and key for the uploaded files
    s3_event = event['Records']
    file_list = []
    for record in s3_event:
        s3_bucket = record['s3']['bucket']['name']
        s3_key = record['s3']['object']['key']
        file_list.append((s3_bucket, s3_key))
    print("File List: ", file_list)

    spark_submit = ['spark-submit', '--master', 'yarn', '--deploy-mode', 'cluster', backend_code,
                    '--jars s3://colaberrycodefilelocationforemr/mysql-connector-java-8.0.27.jar' \
        , '--driver-class-path s3://colaberrycodefilelocationforemr/mysql-connector-java-8.0.27.jar' \
        , '--conf spark.executor.extraClassPath s3://colaberrycodefilelocationforemr/mysql-connector-java-8.0.27.jar' \
        , 'spark.driver.extraClassPath s3://colaberrycodefilelocationforemr/mysql-connector-java-8.0.27.jar' \
        , 'spark.executor.extraClassPath s3://colaberrycodefilelocationforemr/mysql-connector-java-8.0.27.jar'
                    ]

    # Keep track of Glue job names and EMR cluster names that have already been started
    started_jobs = []
    started_clusters = []

    for file in file_list:

        # Check if the file was uploaded to the wx_data folder
        if file[1].startswith('wx_data/'):
            print('wx_data file uploaded, triggering wx_data Glue job')
            job_name = 'colaberrygluejobcleandataingestionweatherdata'
            s3_input_path = os.environ['s3_wx_data_input_path']
            db_name_rds_instance = os.environ['db_name_rds_instance']
            host_rds_instance = os.environ['host_rds_instance']
            jdbc_url = os.environ['jdbc_url']
            password_rds_instance = os.environ['password_rds_instance']
            user_rds_instance = os.environ['user_rds_instance']
            s3_key = file[1]
            spark_submit += [s3_bucket, s3_key]

        # Check if the file was uploaded to the yld_data folder
        elif file[1].startswith('yld_data/'):
            print('yld_data file uploaded, triggering yld_data Glue job')
            job_name = 'colaberrygluejobcleandataingestioncropdata'
            s3_input_path = os.environ['s3_yld_data_input_path']
            db_name_rds_instance = os.environ['db_name_rds_instance']
            host_rds_instance = os.environ['host_rds_instance']
            jdbc_url = os.environ['jdbc_url']
            password_rds_instance = os.environ['password_rds_instance']
            user_rds_instance = os.environ['user_rds_instance']
            s3_key = file[1]
            spark_submit += [s3_bucket, s3_key]

        else:
            print('File uploaded to unknown folder, skipping')
            continue

        # start AWS Glue job for data ingestion
        glue_response = glue.start_job_run(
            JobName=job_name,
            Arguments={"--s3_input_path": s3_input_path, "--s3_file_name": file[1],
                       "--db_name_rds_instance": db_name_rds_instance,
                       "--host_rds_instance": host_rds_instance, "--jdbc_url": jdbc_url,
                       "--password_rds_instance": password_rds_instance,
                       "--user_rds_instance": user_rds_instance
                       }
        )

        # get job run ID for AWS Glue job
        job_run_id = glue_response['JobRunId']
        print(f'Started Glue Job {job_name} for file {file[1]}')

    time.sleep(10)  # delay for 10 seconds

    print(f'{job_name} Glue Job is complete now')

    # Trigger EMR cluster
    for file in file_list:
        # Check if the file was uploaded to the wx_data folder
        if file[1].startswith('wx_data/'):
            print('wx_data file uploaded, triggering EMR Cluster')

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
                    }
                ],
                Steps=[
                    {
                        'Name': 'weather_analysis',
                        'ActionOnFailure': 'CONTINUE',
                        'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': spark_submit
                        }
                    }],
                BootstrapActions=[{
                    'Name': 'colaberrybootstrapaction',
                    'ScriptBootstrapAction': {
                        'Path': 's3://colaberrycodefilelocationforemr/copymysqljar.sh'
                    }
                }],
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

        else:
            print('File uploaded to unknown folder, skipping')

    # Return the response outside the for loop
    return {
        'status': 'Success of EMR Job and Analyzed Weather Data has been stored in Redshift table'
    }
