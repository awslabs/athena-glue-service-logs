# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at

# http://www.apache.org/licenses/LICENSE-2.0

# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Catalog Manager implementations for CloudTrail service logs

Documentation for the format of CloudTrail logs can be found here:
https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-event-reference-record-contents.html

Due to the nature of CloudTrail logs being a centralized location for service logs, some parameters can be
service-specific. For example, `requestParameters` is different depending on the originating AWS service.
"""
import logging

from athena_glue_service_logs.catalog_manager import BaseCatalogManager
from athena_glue_service_logs.partitioners.grouped_date_partitioner import GroupedDatePartitioner

# For now, enabe logging directly inside the module
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

CLOUDTRAIL_TIMESTAMP_FIELD = 'eventtime'


class CloudTrailRawCatalog(BaseCatalogManager):
    """An implementation of BaseCatalogManager for CloudTrail logs"""

    def timestamp_field(self):
        return CLOUDTRAIL_TIMESTAMP_FIELD

    def get_partitioner(self):
        return GroupedDatePartitioner(s3_location=self.s3_location, hive_compatible=False)

    def _table_parameters(self):
        return {
            "compressionType": "gzip",
            "classification": "cloudtrail",
            "jsonPath": "$.Records[*]"
        }

    def _build_storage_descriptor(self, partition_values=None):
        if partition_values is None:
            partition_values = []
        return {
            "Columns": [
                {
                    "Type": "string",
                    "Name": "eventversion"
                },
                {
                    "Type": "struct<type:string,principalId:string,arn:string,accountId:string,accessKeyId:string,sessionContext:struct<attributes:struct<mfaAuthenticated:string,creationDate:string>,sessionIssuer:struct<type:string,principalId:string,arn:string,accountId:string,userName:string>>,invokedBy:string,userName:string>",  # noqa pylint: disable=line-too-long
                    "Name": "useridentity"
                },
                {
                    "Type": "string",
                    "Name": "eventtime"
                },
                {
                    "Type": "string",
                    "Name": "eventsource"
                },
                {
                    "Type": "string",
                    "Name": "eventname"
                },
                {
                    "Type": "string",
                    "Name": "awsregion"
                },
                {
                    "Type": "string",
                    "Name": "sourceipaddress"
                },
                {
                    "Type": "string",
                    "Name": "useragent"
                },
                {
                    "Type": "struct<maxResults:int,instancesSet:string,filterSet:struct<items:array<struct<name:string,valueSet:struct<items:array<struct<value:string>>>>>>,bucketName:string,location:array<string>,policy:array<string>,acl:array<string>,logging:array<string>,maxRecords:int,securityGroupSet:string,securityGroupIdSet:string,spotInstanceRequestIdSet:string,volumeSet:string,trailNameList:array<string>,includeShadowTrails:boolean,CreateBucketConfiguration:struct<LocationConstraint:string,xmlns:string>,AccessControlPolicy:struct<AccessControlList:struct<Grant:array<struct<Grantee:struct<xsi\\:type:string,DisplayName:string,xmlns\\:xsi:string,ID:string,URI:string>,Permission:string>>>,xmlns:string,Owner:struct<DisplayName:string,ID:string>>,limit:int>",  # noqa pylint: disable=line-too-long
                    "Name": "requestparameters"
                },
                {
                    "Type": "struct<clusters:array<string>,snapshots:array<string>>",
                    "Name": "responseelements"
                },
                {
                    "Type": "string",
                    "Name": "requestid"
                },
                {
                    "Type": "string",
                    "Name": "eventid"
                },
                {
                    "Type": "string",
                    "Name": "eventtype"
                },
                {
                    "Type": "string",
                    "Name": "recipientaccountid"
                },
                {
                    "Type": "string",
                    "Name": "errorcode"
                },
                {
                    "Type": "string",
                    "Name": "errormessage"
                },
                {
                    "Type": "string",
                    "Name": "apiversion"
                },
                {
                    "Type": "struct<vpcEndpointId:string>",
                    "Name": "additionaleventdata"
                },
                {
                    "Type": "string",
                    "Name": "vpcendpointid"
                },
                {
                    "Type": "boolean",
                    "Name": "readonly"
                },
                {
                    "Type": "array<string>",
                    "Name": "resources"
                }
            ],
            "Location": self.partitioner.build_partitioned_path(partition_values),
            "SerdeInfo": {
                "Parameters": {}
            },
            "BucketColumns": [],  # Required or SHOW CREATE TABLE fails
            "Parameters": {
                "classification": "cloudtrail",
                "compressionType": "gzip",
                "jsonPath": "$.Records[*]",
            }
        }


class CloudTrailConvertedCatalog(BaseCatalogManager):
    """An implementation of BaseCatalogManager for converted CloudTrail logs"""

    def get_partitioner(self):
        return GroupedDatePartitioner(s3_location=self.s3_location, hive_compatible=True)

    def timestamp_field(self):
        return CLOUDTRAIL_TIMESTAMP_FIELD

    def _build_storage_descriptor(self, partition_values=None):
        if partition_values is None:
            partition_values = []
        return {
            "Columns": [
                {
                    "Type": "string",
                    "Name": "eventversion"
                },
                {
                    "Type": "string",
                    "Name": "json_useridentity"
                },
                {
                    "Type": "timestamp",
                    "Name": "eventtime"
                },
                {
                    "Type": "string",
                    "Name": "eventsource"
                },
                {
                    "Type": "string",
                    "Name": "eventname"
                },
                {
                    "Type": "string",
                    "Name": "awsregion"
                },
                {
                    "Type": "string",
                    "Name": "sourceipaddress"
                },
                {
                    "Type": "string",
                    "Name": "useragent"
                },
                {
                    "Type": "string",
                    "Name": "json_requestparameters"
                },
                {
                    "Type": "string",
                    "Name": "json_responseelements"
                },
                {
                    "Type": "string",
                    "Name": "requestid"
                },
                {
                    "Type": "string",
                    "Name": "eventid"
                },
                {
                    "Type": "string",
                    "Name": "eventtype"
                },
                {
                    "Type": "string",
                    "Name": "recipientaccountid"
                },
                {
                    "Type": "string",
                    "Name": "errorcode"
                },
                {
                    "Type": "string",
                    "Name": "errormessage"
                },
                {
                    "Type": "string",
                    "Name": "apiversion"
                },
                {
                    "Type": "string",
                    "Name": "json_additionaleventdata"
                },
                {
                    "Type": "string",
                    "Name": "vpcendpointid"
                },
                {
                    "Type": "boolean",
                    "Name": "readonly"
                },
                {
                    "Type": "array<struct<arn:string,accountid:string,type:string>>",
                    "Name": "resources"
                }
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
        LOGGER.info("Performing custom conversion action: to_json")

        # The list of fields and mapping tuples we want to convert to a raw JSON string
        raw_fields = ['requestParameters', 'responseElements', 'userIdentity', 'additionalEventData']
        json_mappings = [(name, 'struct', "json_%s" % name.lower(), 'string') for name in raw_fields]

        # Keep the remaining fields the exact same
        static_mappings = [
            (f.name, f.dataType.typeName(), f.name, f.dataType.typeName())
            for f in dynamic_frame.schema() if f.name not in raw_fields
        ]

        # Apply the mapping of the combined field set
        mapped_dyf = dynamic_frame.apply_mapping(static_mappings + json_mappings)

        return mapped_dyf
