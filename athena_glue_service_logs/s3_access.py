# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at

# http://www.apache.org/licenses/LICENSE-2.0

# or in the "license" file accompanying this file. This file is distributed 
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either 
# express or implied. See the License for the specific language governing 
# permissions and limitations under the License.

"""Implementation for converting and partitioning S3 Access Logs"""
from athena_glue_service_logs.catalog_manager import BaseCatalogManager
from athena_glue_service_logs.partitioners.date_partitioner import DatePartitioner
from athena_glue_service_logs.partitioners.null_partitioner import NullPartitioner


class S3AccessRawCatalog(BaseCatalogManager):
    """An implementatin of BaseCatalogManager for S3 Access raw logs"""

    def get_partitioner(self):
        return NullPartitioner(s3_location=self.s3_location, hive_compatible=False)

    def timestamp_field(self):
        return "time"

    def _build_storage_descriptor(self, partition_values=None):
        if partition_values is None:
            partition_values = []
        return {
            "Columns": [
                {"Name": "bucket_owner", "Type": "string"},
                {"Name": "bucket", "Type": "string"},
                {"Name": "time", "Type": "string"},
                {"Name": "remote_ip", "Type": "string"},
                {"Name": "requester", "Type": "string"},
                {"Name": "request_id", "Type": "string"},
                {"Name": "operation", "Type": "string"},
                {"Name": "key", "Type": "string"},
                {"Name": "request_uri", "Type": "string"},
                {"Name": "http_status", "Type": "string"},
                {"Name": "error_code", "Type": "string"},
                {"Name": "bytes_sent", "Type": "string"},
                {"Name": "object_size", "Type": "string"},
                {"Name": "total_time", "Type": "string"},
                {"Name": "turnaround_time", "Type": "string"},
                {"Name": "referrer", "Type": "string"},
                {"Name": "user_agent", "Type": "string"},
                {"Name": "version_id", "Type": "string"},
                {"Name": "host_id", "Type": "string"},
                {"Name": "signature_version", "Type": "string"},
                {"Name": "cipher_suite", "Type": "string"},
                {"Name": "authentication_type", "Type": "string"},
                {"Name": "host_header", "Type": "string"},
                {"Name": "tls_version", "Type": "string"},
            ],
            "Location": self.partitioner.build_partitioned_path(partition_values),
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "com.amazonaws.glue.serde.GrokSerDe",
                "Parameters": {
                    "input.format": "%{NOTSPACE:bucket_owner} %{NOTSPACE:bucket} \\[%{INSIDE_BRACKETS:time}\\] %{NOTSPACE:remote_ip} %{NOTSPACE:requester} %{NOTSPACE:request_id} %{NOTSPACE:operation} %{NOTSPACE:key} \"%{INSIDE_QS:request_uri}\" %{NOTSPACE:http_status} %{NOTSPACE:error_code} %{NOTSPACE:bytes_sent} %{NOTSPACE:object_size} %{NOTSPACE:total_time} %{NOTSPACE:turnaround_time} \"?%{INSIDE_QS:referrer}\"? \"%{INSIDE_QS:user_agent}\" %{NOTSPACE:version_id}(%{2019_OPTION_1}|%{2019_OPTION_2}|%{2019_OPTION_3}|%{2019_OPTION_4})?",  # noqa pylint: disable=C0301
                    "input.grokCustomPatterns": "INSIDE_QS ([^\"]*)\nINSIDE_BRACKETS ([^\\]]*)\nSIGNATURE_VERSION (SigV\\d+|-)\nTLS_VERSION (TLS.+|-)\nS3_FIELDS_2019_001 (%{NOTSPACE:host_id} %{SIGNATURE_VERSION:signature_version} %{NOTSPACE:cipher_suite} %{NOTSPACE:auth_type} %{NOTSPACE:header})\nS3_FIELDS_2019_002 (%{S3_FIELDS_2019_001} %{TLS_VERSION:tls_version})\n2019_OPTION_1 (\\s\\S+\\s%{S3_FIELDS_2019_002})\n2019_OPTION_2 (\\s%{S3_FIELDS_2019_002})\n2019_OPTION_3 (\\s\\S+\\s%{S3_FIELDS_2019_001})\n2019_OPTION_4 (\\s%{NOTSPACE:UNWANTED})"  # noqa pylint: disable=C0301
                }
            },
            "BucketColumns": [],  # Required or SHOW CREATE TABLE fails
            "Parameters": {}  # Required or create_dynamic_frame.from_catalog fails for partitions
        }


class S3AccessConvertedCatalog(BaseCatalogManager):
    """An implementation of BaseCatalogManager for S3 Access converted logs"""

    def get_partitioner(self):
        return DatePartitioner(s3_location=self.s3_location, hive_compatible=True)

    def timestamp_field(self):
        return "time"

    def conversion_actions(self, dynamic_frame):
        from awsglue.transforms import Map
        from dateutil import parser

        def combine_datetime(record):
            """Parse the funky timestamp because python doesn't support %z"""
            parsed_timestamp = parser.parse(record['time'].replace(':', ' ', 1))
            record['time'] = parsed_timestamp.isoformat()
            return record

        mapped_dyf = Map.apply(frame=dynamic_frame, f=combine_datetime)
        return mapped_dyf

    def _build_storage_descriptor(self, partition_values=None):
        if partition_values is None:
            partition_values = []
        return {
            "Columns": [
                {"Name": "bucket_owner", "Type": "string"},
                {"Name": "bucket", "Type": "string"},
                {"Name": "time", "Type": "timestamp"},
                {"Name": "remote_ip", "Type": "string"},
                {"Name": "requester", "Type": "string"},
                {"Name": "request_id", "Type": "string"},
                {"Name": "operation", "Type": "string"},
                {"Name": "key", "Type": "string"},
                {"Name": "request_uri", "Type": "string"},
                {"Name": "http_status", "Type": "string"},
                {"Name": "error_code", "Type": "string"},
                {"Name": "bytes_sent", "Type": "string"},
                {"Name": "object_size", "Type": "string"},
                {"Name": "total_time", "Type": "string"},
                {"Name": "turnaround_time", "Type": "string"},
                {"Name": "referrer", "Type": "string"},
                {"Name": "user_agent", "Type": "string"},
                {"Name": "version_id", "Type": "string"},
                {"Name": "host_id", "Type": "string"},
                {"Name": "signature_version", "Type": "string"},
                {"Name": "cipher_suite", "Type": "string"},
                {"Name": "authentication_type", "Type": "string"},
                {"Name": "host_header", "Type": "string"},
                {"Name": "tls_version", "Type": "string"},
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
