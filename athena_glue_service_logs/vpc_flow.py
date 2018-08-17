# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at

# http://www.apache.org/licenses/LICENSE-2.0

# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Implementation for converting and partitioning VPC Flow Logs

VPC Flow Log Record format: https://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/flow-logs.html#flow-log-records
"""
import logging

from athena_glue_service_logs.catalog_manager import BaseCatalogManager
from athena_glue_service_logs.partitioners.date_partitioner import DatePartitioner
from athena_glue_service_logs.partitioners.grouped_date_partitioner import GroupedDatePartitioner

# For now, enabe logging directly inside the module
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class VPCFlowRawCatalog(BaseCatalogManager):
    """An implementation of BaseCatalogManager for raw VPC Flow Logs"""

    def get_partitioner(self):
        return GroupedDatePartitioner(s3_location=self.s3_location, hive_compatible=False)

    def timestamp_field(self):
        return "starttime"

    def _table_parameters(self):
        return {
            "skip.header.line.count": "1"
        }
    
    @staticmethod
    def _columns():
        return [
            {"Name": "version", "Type": "int"},
            {"Name": "account", "Type": "string"},
            {"Name": "interfaceid", "Type": "string"},
            {"Name": "sourceaddress", "Type": "string"},
            {"Name": "destinationaddress", "Type": "string"},
            {"Name": "sourceport", "Type": "string"},
            {"Name": "destinationport", "Type": "string"},
            {"Name": "protocol", "Type": "string"},
            {"Name": "numpackets", "Type": "string"},
            {"Name": "numbytes", "Type": "string"},
            {"Name": "starttime", "Type": "string"},
            {"Name": "endtime", "Type": "string"},
            {"Name": "action", "Type": "string"},
            {"Name": "logstatus", "Type": "string"},
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
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                "Parameters": {
                    "separatorChar": " "
                }
            },
            "BucketColumns": [],  # Required or SHOW CREATE TABLE fails
            "Parameters": {}  # Required or create_dynamic_frame.from_catalog fails for partitions
        }


class VPCFlowConvertedCatalog(BaseCatalogManager):
    """An implementation of BaseCatalogManager for converted VPC Flow Logs"""

    def get_partitioner(self):
        return GroupedDatePartitioner(s3_location=self.s3_location, hive_compatible=True)

    def timestamp_field(self):
        return "starttime"
    
    @staticmethod
    def _columns():
        return [
            {"Name": "version", "Type": "int"},
            {"Name": "account", "Type": "string"},
            {"Name": "interfaceid", "Type": "string"},
            {"Name": "sourceaddress", "Type": "string"},
            {"Name": "destinationaddress", "Type": "string"},
            {"Name": "sourceport", "Type": "int"},
            {"Name": "destinationport", "Type": "int"},
            {"Name": "protocol", "Type": "int"},
            {"Name": "numpackets", "Type": "int"},
            {"Name": "numbytes", "Type": "int"},
            {"Name": "starttime", "Type": "timestamp"},
            {"Name": "endtime", "Type": "timestamp"},
            {"Name": "action", "Type": "string"},
            {"Name": "logstatus", "Type": "string"},
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
    
    def _remove_dashes(self, dynamic_frame):
        LOGGER.info("Performing vpc_flow custom conversion action: removing dashes")
        from awsglue.transforms import Map

        def remove_dashes(record):
            for field in ['sourceaddress', 'destinationaddress', 'action']:
                if record[field] == '-':
                    record[field] = None
            
            return record
        
        mapped_dyf = Map.apply(frame=dynamic_frame, f=remove_dashes)
        return mapped_dyf
    
    def _cast_timestamps(self, dynamic_frame):
        LOGGER.info("Performing vpc_flow custom conversion action: time conversions")
        from awsglue.transforms import Map
        from datetime import datetime

        # Note that this framework currently only supports string timestamps in the source
        def cast_timestamps(record):
            record['endtime'] = datetime.utcfromtimestamp(int(record['endtime'])).isoformat()
            record['starttime'] = datetime.utcfromtimestamp(int(record['starttime'])).isoformat()
            return record

        mapped_dyf = Map.apply(frame=dynamic_frame, f=cast_timestamps)
        return mapped_dyf
    
    def _apply_mappings(self, dynamic_frame):
        LOGGER.info("Performing vpc_flow custom conversion action: type conversions")

        raw_columns = VPCFlowRawCatalog._columns()
        opt_columns = VPCFlowConvertedCatalog._columns()

        # Build our big list of mappings
        mappings = [
            mapping[0] + mapping[1] for mapping in zip(
                [(f['Name'], f['Type']) for f in raw_columns],
                [(f['Name'], f['Type']) for f in opt_columns]
            )
        ]

        # Include region mapping as Glue does not include partitions in original DynamicFrame
        region_mapping = [('region', 'string', 'region', 'string')]

        return dynamic_frame.apply_mapping(mappings + region_mapping)

    def conversion_actions(self, dynamic_frame):
        timestampDynF = self._cast_timestamps(dynamic_frame)
        cleanedDynF = self._remove_dashes(timestampDynF)
        mappedDynF = self._apply_mappings(cleanedDynF)

        return mappedDynF
        

