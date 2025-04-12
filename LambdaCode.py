import boto3

def lambda_handler(event, context):
    # Hardcoded bucket name - Replace with your actual bucket name
    bucket_name = 'your-bucket-name'  # Replace 'your-bucket-name' with your S3 bucket name

    # Check if the event is an S3 event
    if 'Records' in event and len(event['Records']) > 0:
        s3_event = event['Records'][0]
        # Extract bucket name and object key from the S3 event
        object_key = s3_event['s3']['object']['key']

        # Check if the file is uploaded to the specified folder
        if object_key.startswith('dir/'):  # Adjust 'dir/' to the desired folder prefix
            # Print a message indicating a file was uploaded
            print(f"File '{object_key}' uploaded to bucket '{bucket_name}' in folder 'trigger'")

            # Create EMR client
            emr_client = boto3.client('emr')

            # Check if a cluster with the same name already exists
            existing_clusters = emr_client.list_clusters(
                ClusterStates=['STARTING']
            )['Clusters']

            existing_cluster_names = [cluster['Name'] for cluster in existing_clusters]

            cluster_name = 'YourClusterName'  # Replace with your desired cluster name

            if cluster_name in existing_cluster_names:
                print(f"A cluster with the name '{cluster_name}' already exists. Skipping cluster creation.")
                return {
                    'statusCode': 200,
                    'body': 'Cluster with the same name already exists'
                }

            # Trigger EMR cluster creation
            response = emr_client.run_job_flow(
                Name=cluster_name,
                ReleaseLabel='emr-6.4.0',  # Specify your EMR release version here
                LogUri='s3://your-log-bucket/elasticmapreduce/',  # Replace with your log URI bucket
                ServiceRole='arn:aws:iam::your-account-id:role/your-service-role',  # Replace with your service role ARN
                Instances={
                    'InstanceGroups': [
                        {
                            'InstanceCount': 1,
                            'InstanceRole': 'MASTER',
                            'InstanceType': 'm5.xlarge',  # Adjust based on your instance type
                            'Name': 'MasterNode',
                            'EbsConfiguration': {
                                'EbsBlockDeviceConfigs': [
                                    {
                                        'VolumeSpecification': {
                                            'VolumeType': 'gp2',
                                            'SizeInGB': 32  # Adjust volume size as needed
                                        },
                                        'VolumesPerInstance': 2
                                    }
                                ]
                            }
                        }
                    ],
                    'KeepJobFlowAliveWhenNoSteps': False,  # Terminate cluster after last step
                    'Ec2KeyName': 'your-ec2-key-name'  # Replace with your EC2 key pair name
                },
                Applications=[
                    {'Name': 'Spark'},
                    {'Name': 'Zeppelin'}
                ],
                VisibleToAllUsers=True,
                JobFlowRole='YourJobFlowRole',  # Replace with your Job Flow Role
                Tags=[{'Key': 'for-use-with-amazon-emr-managed-policies', 'Value': 'true'}],
                Steps=[
                    {
                        'Name': 'Snow Job',
                        'ActionOnFailure': 'CONTINUE',
                        'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': [
                                'spark-submit',
                                '--deploy-mode', 'client',
                                '--packages', 'net.snowflake:spark-snowflake_2.12:2.11.2-spark_3.1',
                                '--class', 'pack.snow',
                                's3://your-bucket-name/spark.jar'  # Replace with your JAR location in S3
                            ]
                        }
                    },
                    {
                        'Name': 'API Job',
                        'ActionOnFailure': 'CONTINUE',
                        'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': [
                                'spark-submit',
                                '--deploy-mode', 'client',
                                '--class', 'pack.api',
                                's3://your-bucket-name/spark.jar'  # Replace with your JAR location in S3
                            ]
                        }
                    },
                    {
                        'Name': 'S3 Job',
                        'ActionOnFailure': 'CONTINUE',
                        'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': [
                                'spark-submit',
                                '--deploy-mode', 'client',
                                '--class', 'pack.s3',
                                's3://your-bucket-name/spark.jar'  # Replace with your JAR location in S3
                            ]
                        }
                    },
                    {
                        'Name': 'Master Job',
                        'ActionOnFailure': 'CONTINUE',
                        'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': [
                                'spark-submit',
                                '--deploy-mode', 'client',
                                '--class', 'pack.master',
                                's3://your-bucket-name/spark.jar'  # Replace with your JAR location in S3
                            ]
                        }
                    }
                ],
                ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION'
            )

            print("EMR cluster started successfully")

            # You can also return a response if needed
            return {
                'statusCode': 200,
                'body': 'EMR cluster started successfully'
            }
        else:
            # If the file is not uploaded to the specified folder, do nothing
            print(f"File '{object_key}' uploaded to bucket '{bucket_name}' but not in the 'trigger' folder")
            return {
                'statusCode': 200,
                'body': 'File uploaded but not in the trigger folder'
            }
    else:
        # If the event is not an S3 event, return an error response
        return {
            'statusCode': 400,
            'body': 'Event is not an S3 event'
        }
