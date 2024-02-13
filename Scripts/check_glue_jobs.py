import boto3
import datetime

JOB_NAME = 'mi-glue-job-run-queries-dev'
REGION = 'eu-west-1'
TIME_FORMAT = '%y-%m-%d %H:%M'
GLUE_URL = "https://{region}.console.aws.amazon.com/glue/home?region={region}#jobRun:jobName={job_name};jobRunId={run_id}"
S3_URL = "https://s3.console.aws.amazon.com/s3/buckets/datalake/{table_name}"
CW_URL = "https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}#logEventViewer:group=/aws-glue/jobs/error;stream={run_id}"

def _format_time(timestamp):
    return datetime.datetime.strftime(timestamp+datetime.timedelta(hours=1), TIME_FORMAT)

def _get_jobs(glue_client, job_name, amount):
    return glue_client.get_job_runs(JobName=JOB_NAME, MaxResults=amount)['JobRuns']

def main(amount):
    glue_client = boto3.client('glue', region)
    for job in _get_jobs(glue_client, JOB_NAME, amount):
        table_name = job['Arguments']['--table_name']
        if job['JobRunState'] == 'SUCCEEDED':
            print(_format_time(job['StartedOn']), table_name, job['JobRunState'], S3_URL.format(table_name=table_name))
        else:
            print(_format_time(job['StartedOn']), table_name, job['JobRunState'], CW_URL.format(region=REGION, run_id=job['Id']))

if __name__ == "__main__":
    main(20)
