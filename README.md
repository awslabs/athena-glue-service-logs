# AthenaGlueServiceLogs

Glue jobs and library to manage conversion of AWS Service Logs into Athena-friendly formats.

_Note that this library is under active development. If you run into issues, please file an issue or reach out to [@dacort](https://twitter.com/dacort)._

## Overview

AWS Service Logs come in all different formats. Ideally they could all be queried in place by Athena and, while some can, for cost and performance reasons it can be better to convert the logs into partitioned Parquet files.

The general approach is that for any given type of service log, we have Glue Jobs that can do the following:
1. Create source tables in the Data Catalog
2. Create destination tables in the Data Catalog
3. Know how to convert the source data to partitioned, Parquet files
4. Maintain new partitions for both tables

The following AWS Service Logs are currently supported:
  - Application Load Balancer (ALB)
  - ELB (Classic)
  - CloudTrail
  - CloudFront
  - S3 Access
  - VPC Flow

## Glue Jobs

The implementation aims to be as simple as possible. Utilizing AWS Glue's ability to include Python libraries from S3, an example job for converting S3 Access logs is as simple as this:

```python
from athena_glue_service_logs.job import JobRunner
 
job_run = JobRunner(service_name='s3_access')
job_run.convert_and_partition()
```

That said, if there are additional changes you wish to make to the data, you're likely better off writing your own Glue script.

## Service Names

When creating a job, you must specify a service name - this service name indicates which script to copy over for the Glue job. Here's a mapping of Service Log types to their names.

| Service Log | Service Name Keyword | 
| ----- | ----- |
| ALB | alb | 
| ELB | elb |
| CloudFront | cloudfront |
| CloudTrail | cloudtrail | 
| S3 Access | s3_access |
| VPC Flow | vpc_flow |

## Creating a job

**Pre-requisite**: The `AWSGlueServiceRoleDefault` IAM role must already exist in the account. ([more details](https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html))

Additionally, the Glue script must be deployed in a bucket in the same region as your Glue job.
For this reason, publicly available versions of the scripts won't be available just yet but perhaps someday.

💥 **Warning** These jobs can incur a significant amount of cost depending on how much data you have in your source location!

>As an example, a Glue Job to convert 10GB of data takes about 18 minutes to run with 10 DPU - that would only be about $1.32 with [current Glue pricing](https://aws.amazon.com/glue/pricing/), but many people have a lot of log data. :)

Each log type requires an initial Glue script to bootstrap the job and S3 locations will be different depending on the type of service log. These details are specified below in the [Service Log Specifics](#service-log-specifics) section.

For now, you'll have to run the created jobs manually to trigger the conversion, but you can create a schedule for it in the Glue console. The Glue job will create the raw and converted tables and containing databases, if necessary, and run the conversion proccess over data found in the source location.

### Option 1: Deploy scripts and create job with `make`

> _Note: Requires the [jq](https://stedolan.github.io/jq/) utility_

There is an example config file in [scripts/example_glue_jobs.json](scripts/example_glue_jobs.json). Copy that to `scripts/glue_jobs.json` and modify the service definitions for your desired service logs. Note that the values in the `defaults` key can be overridden for each service.

To deploy the scripts to your own bucket, override the `RELEASE_BUCKET` environment variable and run the following make commands. The scripts will be packaged up, uploaded to your desired bucket and a Glue job will be created for the specified `SERVICE_NAME`.

```shell
RELEASE_BUCKET=<YOUR_BUCKET_NAME> make private_release
RELEASE_BUCKET=<YOUR_BUCKET_NAME> make create_job service=<SERVICE_NAME>
```

`SERVICE_NAME` is the key in the config file. Don't change those keys as they are also references to the actual Glue scripts.

Depending on what you put for the `JOB_NAME_BASE` variable in the config file, this will create a Glue Job using the latest development branch of this code with a name like `JOB_NAME_BASE_LogMaster_ScriptVersion`. 

### Option 2: AWS CLI commands

You must deploy the Python module and sample jobs to an S3 bucket - you can use `make private_release` as noted above to do so, or `make package` and copy both `dist/athena_glue_converter_<SCRIPT_VERSION>.zip` and `scripts/*` to an S3 bucket.

Glue Jobs for each service log type can be created using an AWS CLI command. Below is an example command for doing so with S3 Access Logs. Note that `--extra-py-files` and `ScriptLocation` must point to wherever you copied them to.

```shell
aws glue create-job --name S3AccessLogConvertor \
--description "Convert and partition S3 Access logs" \
--role AWSGlueServiceRoleDefault \
--command Name=glueetl,ScriptLocation=s3://aws-emr-bda-public/agsl/glue_scripts/sample_s3_access_job.py \
--default-arguments '{
    "--extra-py-files":"s3://aws-emr-bda-public/agsl/glue_scripts/athena_glue_converter_latest.zip",
    "--job-bookmark-option":"job-bookmark-enable",
    "--raw_database_name":"aws_service_logs",
    "--raw_table_name":"s3_access_raw",
    "--converted_database_name":"aws_service_logs",
    "--converted_table_name":"s3_access_optimized",
    "--TempDir":"s3://<bucket>/tmp",
    "--s3_converted_target":"s3://<bucket>/converted/s3_access",
    "--s3_source_location":"s3://<bucket>/s3_access/"
}'
```

## Service Log Specifics

### ALB Logs

For ALB logs, configure your source location to be the prefix before the logs split out into regions. For example, this usually looks like:
`s3://<BUCKET>/<PREFIX>/AWSLogs/<AWS_ACCOUNT_ID>/elasticloadbalancing/`

### ELB Logs

The same holds true for ELB logs as ALB logs. Make sure your source location is configured as above.

### CloudTrail Logs

Similar to ALB logs, CloudTrail appends it's own prefix. You must provide the prefix to where there regions are located. Similar to:
`s3://<BUCKET>/<PREFIX>/AWSLogs/<AWS_ACCOUNT_ID>/CloudTrail/`

The other thing to note about CloudTrail logs is that there are a few fields that can contain relatively arbitrary JSON. Due to the complexity of detecting a useable schema from these fields, we have decided to leave them as raw JSON strings.

They can be queried using [JSON extract](https://docs.aws.amazon.com/athena/latest/ug/extracting-data-from-JSON.html) functionality in Athena. In addition, these fields are prefixed with `json_` to indicate this characteristic:
- json_useridentity
- json_requestparameters
- json_responseelements
- json_additionaleventdata

Finally, because of how Glue categorizes the CloudTrail data, the "raw" table will not be queryable in Athena.

### CloudFront Logs

CloudFront logs are delivered to a single prefix in S3. As such, queries over the "raw" table can timeout of there is a large amount of data in that prefix. 

The source location should point to that prefix:
`s3://<BUCKET>/<PREFIX>/`

### S3 Access Logs

S3 Access logs are similar to CloudFront in that they are delivered in a single prefix. Make sure your source location is configured as above.

### VPC Flow Logs

VPC Flow logs also deliver into the AWSLogs prefix: 
`s3://<BUCKET>/<PREFIX>/AWSLogs/<AWS_ACCOUNT_ID>/vpcflowlogs/`

## FAQ

### Does this use Glue Crawlers?
No - there are a couple reasons for this currently.
1. We cannot explicitly define table names with crawlers. For UX simplicity, we wanted to be able to specify table names.
2. We already know the schema and the partition layout of the source data.

### What other log formats are you planning to support?
That depends! Which logs would you like to see? Other log formats we're thinking about include:
- VPC Flow Logs
- AWS Cost and Usage Reports
- Database Audit Logs

Create an issue if you have requests for others!

## ToDos

- [ ] Graceful schema upgrades
- [ ] Allow for custom backfill timeframe
- [ ] Data type conversions when writing to Parquet