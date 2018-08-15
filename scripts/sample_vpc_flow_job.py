from athena_glue_service_logs.job import JobRunner

job_run = JobRunner(service_name='vpc_flow')
job_run.convert_and_partition()
