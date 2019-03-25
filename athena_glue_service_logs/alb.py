# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at

# http://www.apache.org/licenses/LICENSE-2.0

# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Catalog Manager implementations for ALB service logs

Documentation for the format of CloudTrail logs can be found here:
https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-entry-format
"""
import logging

from athena_glue_service_logs.catalog_manager import BaseCatalogManager
from athena_glue_service_logs.partitioners.grouped_date_partitioner import GroupedDatePartitioner


# For now, enabe logging directly inside the module
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class ALBConvertedCatalog(BaseCatalogManager):
    """An implementation of BaseCatalogManager for ALB converted tables"""

    def get_partitioner(self):
        return GroupedDatePartitioner(s3_location=self.s3_location, hive_compatible=True)

    def timestamp_field(self):
        return "time"

    @staticmethod
    def _columns():
        return [
            {"Name": "type", "Type": "string"},
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
            {"Name": "ssl_protocol", "Type": "string"},
            {"Name": "target_group_arn", "Type": "string"},
            {"Name": "trace_id", "Type": "string"},
            {"Name": "domain_name", "Type": "string"},
            {"Name": "chosen_cert_arn", "Type": "string"},
            {"Name": "matched_rule_priority", "Type": "string"},
            {"Name": "request_creation_time", "Type": "timestamp"},
            {"Name": "actions_executed", "Type": "string"},
            {"Name": "redirect_url", "Type": "string"},
            {"Name": "error_reason", "Type": "string"}
        ]

    def _build_storage_descriptor(self, partition_values=None):
        if partition_values is None:
            partition_values = []
        return {
            "Columns": self._columns(),
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

    def _apply_mappings(self, dynamic_frame):
        LOGGER.info("Performing alb custom conversion action: type conversions")

        raw_columns = ALBRawCatalog._columns()
        opt_columns = ALBConvertedCatalog._columns()

        # Build our big list of mappings
        mappings = [
            mapping[0] + mapping[1] for mapping in zip(
                [(f['Name'], f['Type']) for f in raw_columns],
                [(f['Name'], f['Type']) for f in opt_columns]
            )
        ]

        # Include region and date mapping as Glue does not include partitions in original DynamicFrame
        region_mapping = [
            ('region', 'string', 'region', 'string'),
            ('year', 'string', 'year', 'string'),
            ('month', 'string', 'month', 'string'),
            ('day', 'string', 'day', 'string')
        ]

        return dynamic_frame.apply_mapping(mappings + region_mapping)

    def conversion_actions(self, dynamic_frame):
        mappedDynF = self._apply_mappings(dynamic_frame)

        return mappedDynF


class ALBRawCatalog(BaseCatalogManager):
    """An implementation of BaseCatalogManager for ALB raw tables"""

    def get_partitioner(self):
        return GroupedDatePartitioner(s3_location=self.s3_location, hive_compatible=False)

    def timestamp_field(self):
        return "time"

    @staticmethod
    def _columns():
        return [
            {"Name": "type", "Type": "string"},
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
            {"Name": "ssl_protocol", "Type": "string"},
            {"Name": "target_group_arn", "Type": "string"},
            {"Name": "trace_id", "Type": "string"},
            {"Name": "domain_name", "Type": "string"},
            {"Name": "chosen_cert_arn", "Type": "string"},
            {"Name": "matched_rule_priority", "Type": "string"},
            {"Name": "request_creation_time", "Type": "string"},
            {"Name": "actions_executed", "Type": "string"},
            {"Name": "redirect_url", "Type": "string"},
            {"Name": "error_reason", "Type": "string"}
        ]

    def _build_storage_descriptor(self, partition_values=None):
        if partition_values is None:
            partition_values = []
        return {
            "Columns": self._columns(),
            "Location": self.partitioner.build_partitioned_path(partition_values),
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "com.amazonaws.glue.serde.GrokSerDe",
                "Parameters": {
                     "input.format": "%{NOTSPACE:type} %{NOTSPACE:time} %{NOTSPACE:elb} %{NOTSPACE:client_ip_port} %{NOTSPACE:target_ip_port} %{BASE10NUM:request_processing_time:double} %{BASE10NUM:target_processing_time:double} %{BASE10NUM:response_processing_time:double} %{NOTSPACE:elb_status_code} %{NOTSPACE:target_status_code} %{NOTSPACE:received_bytes:int} %{NOTSPACE:sent_bytes:int} \"%{NOTSPACE:request_verb} %{NOTSPACE:request_url} %{INSIDE_QS:request_proto}\" \"%{INSIDE_QS:user_agent}\" %{NOTSPACE:ssl_cipher} %{NOTSPACE:ssl_protocol} %{NOTSPACE:target_group_arn} \"%{INSIDE_QS:trace_id}\" \"%{INSIDE_QS:domain_name}\" \"%{INSIDE_QS:chosen_cert_arn}\"(\s%{NOTSPACE:matched_rule_priority})?(\s%{NOTSPACE:request_creation_time})?(\s\"%{INSIDE_QS:actions_executed}\")?(\s\"%{INSIDE_QS:redirect_url}\")?(\s\"%{INSIDE_QS:error_reason}\")?",  # noqa pylint: disable=C0301
                    "input.grokCustomPatterns": "INSIDE_QS ([^\\\"]*)"
                }
            },
            "BucketColumns": [],  # Required or SHOW CREATE TABLE fails
            "Parameters": {}  # Required or create_dynamic_frame.from_catalog fails for partitions
        }
