# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at

# http://www.apache.org/licenses/LICENSE-2.0

# or in the "license" file accompanying this file. This file is distributed 
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either 
# express or implied. See the License for the specific language governing 
# permissions and limitations under the License.

"""Catalog Manager implementations for ELB (Classic) service logs

Documentation for the format of ELB logs can be found here:
http://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html

ELB Classic service logs are almost exactly the same as ALB with the exception of 5 columns.
Because of this, these implementations primarily inherit directly from ALB Catalog Manager
classes with minimal changes.
"""
import logging

from athena_glue_service_logs.alb import ALBRawCatalog, ALBConvertedCatalog

# For now, enabe logging directly inside the module
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class ELBRawCatalog(ALBRawCatalog):
    """An implementation for BaseCatalogManager for ELB raw tables.

    The only difference between this and ALB are the columns
    """

    def _build_storage_descriptor(self, partition_values=None):
        if partition_values is None:
            partition_values = []
        return {
            "Columns": [
                {"Name": "time", "Type": "string"},
                {"Name": "elb", "Type": "string"},
                {"Name": "client_ip_port", "Type": "string"},
                {"Name": "target_ip_port", "Type": "string"},
                {"Name": "request_processing_time", "Type": "double"},
                {"Name": "target_processing_time", "Type": "double"},
                {"Name": "response_processing_time", "Type": "double"},
                {"Name": "elb_status_code", "Type": "string"},
                {"Name": "target_status_code", "Type": "string"},
                {"Name": "received_bytes", "Type": "bigint"},
                {"Name": "sent_bytes", "Type": "bigint"},
                {"Name": "request_verb", "Type": "string"},
                {"Name": "request_url", "Type": "string"},
                {"Name": "request_proto", "Type": "string"},
                {"Name": "user_agent", "Type": "string"},
                {"Name": "ssl_cipher", "Type": "string"},
                {"Name": "ssl_protocol", "Type": "string"}
            ],
            "Location": self.partitioner.build_partitioned_path(partition_values),
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "com.amazonaws.glue.serde.GrokSerDe",
                "Parameters": {
                    "input.format": "%{NOTSPACE:time} %{NOTSPACE:elb} %{NOTSPACE:client_ip_port} %{NOTSPACE:target_ip_port} %{BASE10NUM:request_processing_time:double} %{BASE10NUM:target_processing_time:double} %{BASE10NUM:response_processing_time:double} %{NOTSPACE:elb_status_code} %{NOTSPACE:target_status_code} %{NOTSPACE:received_bytes:int} %{NOTSPACE:sent_bytes:int} \"%{NOTSPACE:request_verb} %{NOTSPACE:request_url} %{INSIDE_QS:request_proto}\" %{QS:user_agent} %{NOTSPACE:ssl_cipher} %{NOTSPACE:ssl_protocol}",  # noqa pylint: disable=C0301
                    "input.grokCustomPatterns": "INSIDE_QS ([^\\\"]*)"
                }
            },
            "BucketColumns": [],  # Required or SHOW CREATE TABLE fails
            "Parameters": {}  # Required or create_dynamic_frame.from_catalog fails for partitions
        }


class ELBConvertedCatalog(ALBConvertedCatalog):
    """An implementation of BaseCatalogManager for ELB converted tables.

    The only difference between this and ALB are the columns
    """

    def _build_storage_descriptor(self, partition_values=None):
        # This is currently hard-coded for ALB, but we should maintainer a generator somewhere to be able to handle
        # other types. Possibly this could read a versioned definition file from S3.
        if partition_values is None:
            partition_values = []
        return {
            "Columns": [
                {"Name": "time", "Type": "timestamp"},
                {"Name": "elb", "Type": "string"},
                {"Name": "client_ip_port", "Type": "string"},
                {"Name": "target_ip_port", "Type": "string"},
                {"Name": "request_processing_time", "Type": "double"},
                {"Name": "target_processing_time", "Type": "double"},
                {"Name": "response_processing_time", "Type": "double"},
                {"Name": "elb_status_code", "Type": "string"},
                {"Name": "target_status_code", "Type": "string"},
                {"Name": "received_bytes", "Type": "bigint"},
                {"Name": "sent_bytes", "Type": "bigint"},
                {"Name": "request_verb", "Type": "string"},
                {"Name": "request_url", "Type": "string"},
                {"Name": "request_proto", "Type": "string"},
                {"Name": "user_agent", "Type": "string"},
                {"Name": "ssl_cipher", "Type": "string"},
                {"Name": "ssl_protocol", "Type": "string"}
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
