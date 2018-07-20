# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at

# http://www.apache.org/licenses/LICENSE-2.0

# or in the "license" file accompanying this file. This file is distributed 
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either 
# express or implied. See the License for the specific language governing 
# permissions and limitations under the License.

"""NullPartitioner reprsents a data source where all files are in a single prefix"""
from athena_glue_service_logs.partitioners.base_partitioner import BasePartitioner


class NullPartitioner(BasePartitioner):
    """NullPartitioner reprsents a data source where all files are in a single prefix"""
    def build_partitions_from_s3(self):
        return []

    def partition_keys(self):
        return []

    def find_recent_partitions(self, existing_partitions):
        return []
