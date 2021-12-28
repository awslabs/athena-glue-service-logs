import argparse
from athena_glue_service_logs.s3_access import S3AccessRawCatalog
import sys
import os
import json
import boto3
import botocore
from botocore.client import BaseClient
from awsglue.blueprint.workflow import *
from awsglue.blueprint.job import *
from awsglue.blueprint.crawler import *

def create_databases(client):
    try:
        client.create_database(
            DatabaseInput={
                'Name': "aws_service_logs"
            }
        )
        print("New database is created.")
    except client.exceptions.AlreadyExistsException:
        print("Existing database is used.")
    try:
        client.create_database(
            DatabaseInput={
                'Name': "aws_service_logs_raw"
            }
        )
        print("New database is created.")
    except client.exceptions.AlreadyExistsException:
        print("Existing database is used.")
    
    return [client.get_database(Name="aws_service_logs"), client.get_database(Name="aws_service_logs_raw")]

def create_classifier(client):
    try:
        return client.create_classifier(
            GrokClassifier={
                "Classification": "s3access",
                "Name": "S3 Access Logs",
                "GrokPattern": '%{NOTSPACE:bucket_owner} %{NOTSPACE:bucket} \\[%{INSIDE_BRACKETS:time}\\] %{NOTSPACE:remote_ip} %{NOTSPACE:requester} %{NOTSPACE:request_id} %{NOTSPACE:operation} %{NOTSPACE:key} "%{INSIDE_QS:request_uri}" %{NOTSPACE:http_status} %{NOTSPACE:error_code} %{NOTSPACE:bytes_sent} %{NOTSPACE:object_size} %{NOTSPACE:total_time} %{NOTSPACE:turnaround_time} "?%{INSIDE_QS:referrer}"? "%{INSIDE_QS:user_agent}" %{NOTSPACE:version_id}(%{2019_OPTION_1}|%{2019_OPTION_2}|%{2019_OPTION_3}|%{2019_OPTION_4})?',
                "CustomPatterns": 'INSIDE_QS ([^"]*)\nINSIDE_BRACKETS ([^\\]]*)\nSIGNATURE_VERSION (SigV\\d+|-)\nTLS_VERSION (TLS.+|-)\nS3_FIELDS_2019_001 (%{NOTSPACE:host_id} %{SIGNATURE_VERSION:signature_version} %{NOTSPACE:cipher_suite} %{NOTSPACE:auth_type} %{NOTSPACE:header})\nS3_FIELDS_2019_002 (%{S3_FIELDS_2019_001} %{TLS_VERSION:tls_version})\n2019_OPTION_1 (\\s\\S+\\s%{S3_FIELDS_2019_002})\n2019_OPTION_2 (\\s%{S3_FIELDS_2019_002})\n2019_OPTION_3 (\\s\\S+\\s%{S3_FIELDS_2019_001})\n2019_OPTION_4 (\\s%{NOTSPACE:UNWANTED})',
            }
        )
    except client.exceptions.AlreadyExistsException:
        print("S3 Access Classifier already exists")


def generate_layout(user_params, system_params):
    # There are a few things that happen when we generate a layout
    # 1. If it doesn't exist, we create a Classifer (if needed)
    # 2. We create source and destination tables
    # 3. We create a source Crawler (if necessary) with that Classifier
    # 4. We create a job for the specific conversion type
    # 5. We create a destination Crawler for the converted location
    session = boto3.Session(region_name=system_params["region"])
    glue = session.client("glue")

    # Testing this with a hard-coded version for now
    [source_db, target_db] = create_databases(glue)

    # Create source/dest database tables
    classifier = create_classifier(glue)

    # Now we use the existing functionality to create the tables
    raw = S3AccessRawCatalog(system_params.get('region'), "aws_service_logs_raw", "s3", user_params.get('InputDataLocation'))
    optimized = S3AccessRawCatalog(system_params.get('region'), "aws_service_logs", "s3", user_params.get('OutputDataLocation'))
    raw.create_table()
    optimized.create_table()

    # Create a source crawler

    command = {
        "Name": "glueetl",
        "ScriptLocation": user_params.get('ScriptLocation'),
        "PythonVersion": "3",
    }
    arguments = {
        "--TempDir": user_params.get('TempLocation'),
        "--job-bookmark-option": "job-bookmark-enable",
        "--job-language": "python",
    }

    print("Creating ETL job")
    etl_job = Job(
        Name="{}_etl_job".format(user_params["WorkflowName"]),
        Command=command,
        Role=user_params.get('IAMRole'),
        DefaultArguments=arguments,
        WorkerType="G.1X",
        NumberOfWorkers=int(user_params['DPU']),
        GlueVersion="2.0",
    )


    # Create a target crawler
    target_crawler = Crawler(
        Name="s3_optimized_crawler",
        Role=user_params.get('IAMRole'),
        Targets={
            "CatalogTargets": [
                {
                    "DatabaseName": "aws_service_logs",
                    "Tables": ["s3"]
                }
            ]
        },
        Grouping={
            "TableGroupingPolicy": "CombineCompatibleSchemas"
        },
        SchemaChangePolicy={"DeleteBehavior": "LOG"},
        DependsOn={etl_job: "SUCCEEDED"}
    )

    sample_workflow = Workflow(
        Name=user_params["WorkflowName"],
        Entities=Entities(Jobs=[etl_job], Crawlers=[target_crawler]),
    )
    return sample_workflow
