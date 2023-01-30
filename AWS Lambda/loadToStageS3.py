import boto3


def lambda_handler(event, context):
    glue_jobs =	{
        "tripdata.json": "tripdatajsonjob"
        }
        
    file_name = event['Records'][0]['s3']['object']['key']
    bucketName=event['Records'][0]['s3']['bucket']['name']

    glue=boto3.client('glue');
    
    if file_name == "tripdata.json":
        response = glue.start_job_run(JobName = glue_jobs['load_Stage'])
        print("Run {}".format(glue_jobs['load_Stage'))
    