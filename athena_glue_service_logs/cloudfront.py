# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at

# http://www.apache.org/licenses/LICENSE-2.0

# or in the "license" file accompanying this file. This file is distributed 
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either 
# express or implied. See the License for the specific language governing 
# permissions and limitations under the License.

"""Catalog Manager implementations for CloudFront service logs

Documentation for the format of CloudTrail logs can be found here:
https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html
"""
import logging

from athena_glue_service_logs.catalog_manager import BaseCatalogManager
from athena_glue_service_logs.partitioners.date_partitioner import DatePartitioner
from athena_glue_service_logs.partitioners.null_partitioner import NullPartitioner


# For now, enabe logging directly inside the module
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class CloudFrontRawCatalog(BaseCatalogManager):
    """An implementation of BaseCatalogManager for CloudFront logs

    CloudFront logs are stored in a single prefix. They are TSV files with 2 header comments.
    """

    def timestamp_field(self):
        return "time"

    def get_partitioner(self):
        return NullPartitioner(s3_location=self.s3_location, hive_compatible=False)

    def _build_storage_descriptor(self, partition_values=None):
        if partition_values is None:
            partition_values = []
        return {
            "Columns": [
                {"Name": "date", "Type": "string"},
                {"Name": "time", "Type": "string"},
                {"Name": "location", "Type": "string"},
                {"Name": "bytes", "Type": "bigint"},
                {"Name": "requestip", "Type": "string"},
                {"Name": "method", "Type": "string"},
                {"Name": "host", "Type": "string"},
                {"Name": "uri", "Type": "string"},
                {"Name": "status", "Type": "int"},
                {"Name": "referrer", "Type": "string"},
                {"Name": "useragent", "Type": "string"},
                {"Name": "querystring", "Type": "string"},
                {"Name": "cookie", "Type": "string"},
                {"Name": "resulttype", "Type": "string"},
                {"Name": "requestid", "Type": "string"},
                {"Name": "hostheader", "Type": "string"},
                {"Name": "requestprotocol", "Type": "string"},
                {"Name": "requestbytes", "Type": "bigint"},
                {"Name": "timetaken", "Type": "double"},
                {"Name": "xforwardedfor", "Type": "string"},
                {"Name": "sslprotocol", "Type": "string"},
                {"Name": "sslcipher", "Type": "string"},
                {"Name": "responseresulttype", "Type": "string"},
                {"Name": "httpversion", "Type": "string"}
            ],
            "Location": self.partitioner.build_partitioned_path(partition_values),
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                "Parameters": {
                    "separatorChar": "\t"
                }
            },
            "BucketColumns": [],  # Required or SHOW CREATE TABLE fails
            "Parameters": {}  # Required or create_dynamic_frame.from_catalog fails for partitions
        }

    def _table_parameters(self):
        return {
            "skip.header.line.count": "2",
        }


class CloudFrontConvertedCatalog(BaseCatalogManager):
    """An implementation of BaseCatalogManager for CloudFront converted tables"""

    def timestamp_field(self):
        return "time"

    def get_partitioner(self):
        return DatePartitioner(s3_location=self.s3_location, hive_compatible=True)

    def _build_storage_descriptor(self, partition_values=None):
        if partition_values is None:
            partition_values = []
        return {
            "Columns": [
                {"Name": "time", "Type": "timestamp"},
                {"Name": "location", "Type": "string"},
                {"Name": "bytes", "Type": "bigint"},
                {"Name": "requestip", "Type": "string"},
                {"Name": "method", "Type": "string"},
                {"Name": "host", "Type": "string"},
                {"Name": "uri", "Type": "string"},
                {"Name": "status", "Type": "int"},
                {"Name": "referrer", "Type": "string"},
                {"Name": "useragent", "Type": "string"},
                {"Name": "querystring", "Type": "string"},
                {"Name": "cookie", "Type": "string"},
                {"Name": "resulttype", "Type": "string"},
                {"Name": "requestid", "Type": "string"},
                {"Name": "hostheader", "Type": "string"},
                {"Name": "requestprotocol", "Type": "string"},
                {"Name": "requestbytes", "Type": "bigint"},
                {"Name": "timetaken", "Type": "double"},
                {"Name": "xforwardedfor", "Type": "string"},
                {"Name": "sslprotocol", "Type": "string"},
                {"Name": "sslcipher", "Type": "string"},
                {"Name": "responseresulttype", "Type": "string"},
                {"Name": "httpversion", "Type": "string"}
            ],
            "Location": self.partitioner.build_partitioned_path(partition_values),
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {}
            },
            "BucketColumns": [],  # Required or SHOW CREATE TABLE fails
            "Parameters": {}  # Required or create_dynamic_frame.from_catalog fails for partitions
        }

    def conversion_actions(self, dynamic_frame):
        from awsglue.transforms import Map

        def combine_datetime(record):
            """Combine two date and time fields into one time field"""
            record['time'] = "%s %s" % (record['date'], record['time'])
            del record['date']
            return record

        mapped_dyf = Map.apply(frame=dynamic_frame, f=combine_datetime)
        return mapped_dyf
