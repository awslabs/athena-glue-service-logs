from athena_glue_service_logs.job import JobRunner

job_run = JobRunner(service_name='s3_access')
job_run.convert_and_partition()
