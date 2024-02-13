import boto3

ENV = "dev"
ETL_GLUE_JOB = "my-glue-job"
REGION = "eu-west-1"

session = boto3.session.Session(profile_name=ENV)
glue = session.client('glue', REGION)

def trigger_glue(file_path):
    response = glue.start_job_run(JobName=ETL_GLUE_JOB,
                                  Arguments={'--file_path': file_path,
                                             '--env': ENV,
                                             '--region': REGION}
                                 )
    return response

response = trigger_glue("/parse/this/path")
print(response)
