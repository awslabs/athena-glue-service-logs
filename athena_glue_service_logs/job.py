# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at

# http://www.apache.org/licenses/LICENSE-2.0

# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""The primary class responsible for orchestrating conversion of AWS Service Logs

It performs several necessary steps depending on the state of the job:
    1. Creates a "raw native" log table in the Glue Data Catalog
       This is used primarily to enable Glue bookmarking to keep track of which data has been processed.
    2. Creates a "converted optimized" log table in the Glue Data Catalog
    3. Runs the converter Glue Job that converts and partitions the raw data
    4. Adds recent partitions to both the raw and optimized tables, if necessary

The `convert_and_partition` method wraps this functionality into one method that can be easily called from a Glue job.

The thought is that a console can call the CreateJob Glue API with this script as it's job command.
Then the console will StartJobRun with the resulting Job name and several parameters:
    --s3_source_location - The S3 location of the original AWS Service Logs
    --raw_database_name - The name of the database where the raw logs should be stored
    --raw_table_name - The name of the table for the raw service logs
    --s3_converted_target - The S3 location where converted logs will be written
    --converted_database_name - The name of the database where the converted logs are stored
    --converted_table_name - The name of the table for the converted and optimized AWS service logs
"""
import sys
import json
import urllib.request, urllib.error, urllib.parse
import logging

from awsglue.utils import getResolvedOptions
from awsglue.job import Job

from athena_glue_service_logs.alb import ALBRawCatalog, ALBConvertedCatalog
from athena_glue_service_logs.elb_classic import ELBRawCatalog, ELBConvertedCatalog
from athena_glue_service_logs.cloudtrail import CloudTrailRawCatalog, CloudTrailConvertedCatalog
from athena_glue_service_logs.cloudfront import CloudFrontRawCatalog, CloudFrontConvertedCatalog
from athena_glue_service_logs.s3_access import S3AccessRawCatalog, S3AccessConvertedCatalog
from athena_glue_service_logs.vpc_flow import VPCFlowRawCatalog, VPCFlowConvertedCatalog
from athena_glue_service_logs.partitioners.null_partitioner import NullPartitioner
from athena_glue_service_logs.converter import DataConverter

# For now, enabe logging directly inside the module
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class JobRunner:
    """This class manages the orchestration of new jobs, including parsing arguments and control flow"""
    SERVICE_DEFINITIONS = {
        'alb': [ALBRawCatalog, ALBConvertedCatalog],
        'elb': [ELBRawCatalog, ELBConvertedCatalog],
        'cloudtrail': [CloudTrailRawCatalog, CloudTrailConvertedCatalog],
        'cloudfront': [CloudFrontRawCatalog, CloudFrontConvertedCatalog],
        's3_access': [S3AccessRawCatalog, S3AccessConvertedCatalog],
        'vpc_flow': [VPCFlowRawCatalog, VPCFlowConvertedCatalog],
    }

    def __init__(self, service_name):
        args = getResolvedOptions(sys.argv, ['JOB_NAME'] + self._job_arguments())

        # Validate the service name
        if not self.is_valid_service(service_name):
            raise Exception("'%s' is not yet a supported service." % service_name)

        self.glue_context = self._init_glue_context()
        self.job = Job(self.glue_context)
        region = self.get_instance_region()

        # Create data catalog references
        raw_klas = self.SERVICE_DEFINITIONS[service_name][0]
        converted_klas = self.SERVICE_DEFINITIONS[service_name][1]

        self.raw_catalog = raw_klas(
            region,
            args['raw_database_name'],
            args['raw_table_name'],
            args['s3_source_location']
        )
        self.optimized_catalog = converted_klas(
            region,
            args['converted_database_name'],
            args['converted_table_name'],
            args['s3_converted_target']
        )

        # Assume that if the raw table does not exist, this is our first run
        self.initial_run = not self.raw_catalog.does_table_exist()

        # Create a converter object and initialize the glue job!
        self.converter = DataConverter(self.glue_context, self.raw_catalog, self.optimized_catalog)
        self.job.init(args['JOB_NAME'], args)

    @staticmethod
    def is_valid_service(service_name):
        """Determines whether the given service_name is a supported service or not"""
        return service_name in JobRunner.SERVICE_DEFINITIONS

    def get_instance_region(self):
        """Retrieve the current AWS Region from the Instance Metadata"""
        contents = urllib.request.urlopen("http://169.254.169.254/latest/dynamic/instance-identity/document").read()
        return json.loads(contents).get('region')

    def create_tables_if_needed(self):
        """If this is the initial run of the Job, create both the raw and optmized tables in the Data Catalog"""
        if self.initial_run is True:
            # TODO: Fail if the table already exists, or for converted tables if the S3 path already exists
            LOGGER.info("Initial run, scanning S3 for partitions.")
            self.raw_catalog.initialize_table_from_s3()
            # Note that if the source table is partitionless, this is a null-op.
            self.optimized_catalog.initialize_with_partitions(self.raw_catalog.partitioner.build_partitions_from_s3())

    def add_new_raw_partitions(self):
        """For the raw catalog, check and see if any new partitions exist for UTC today.

        Continue this check for every day previous until we reach a day where a partition exists."""
        if self.initial_run is not True:
            LOGGER.info("Recurring run, only looking for recent partitions on raw catalog.")
            self.raw_catalog.add_recent_partitions()

    def add_new_optimized_partitions(self):
        """For the optimized catalog, check and see if any new partitions exist for UTC today.
        Continue this check for every day previous until we reach a day where a partition exists.

        If this is the initial run, add whatever partitions we can find.
        """
        if self.initial_run and isinstance(self.raw_catalog.partitioner, NullPartitioner):
            LOGGER.info("Initial run with source NullPartitioner, adding all partitions from S3.")
            self.optimized_catalog.get_and_create_partitions()
        else:
            self.optimized_catalog.add_recent_partitions()

    def trigger_conversion(self):
        """Trigger the DataConverter"""
        self.converter.run()

    def finish(self):
        """Take any actions necessary to finish the job"""
        self.job.commit()

    def convert_and_partition(self):
        """A wrapper for the most common operations of these jobs. This allows for a simple one-line
        interface to the consumer, but allows them to use more-specific methods if need be.
        """
        self.create_tables_if_needed()
        self.add_new_raw_partitions()
        self.trigger_conversion()
        self.add_new_optimized_partitions()
        self.finish()

    @staticmethod
    def _job_arguments():
        return [
            'raw_database_name',
            'raw_table_name',
            'converted_database_name',
            'converted_table_name',
            's3_source_location',
            's3_converted_target'
        ]

    @staticmethod
    def _init_glue_context():
        # Imports are done here so we can isolate the configuration of this job
        from awsglue.context import GlueContext
        from pyspark.context import SparkContext
        spark_context = SparkContext.getOrCreate()
        spark_context._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")  # noqa pylint: disable=protected-access
        spark_context._jsc.hadoopConfiguration().set("parquet.enable.summary-metadata", "false")  # noqa pylint: disable=protected-access
        return GlueContext(spark_context)


def main():
    """Primary entry point"""
    job_run = JobRunner(service_name='alb')
    job_run.create_tables_if_needed()
    job_run.add_new_raw_partitions()
    job_run.trigger_conversion()
    job_run.add_new_optimized_partitions()
    job_run.finish()


if __name__ == '__main__':
    main()
