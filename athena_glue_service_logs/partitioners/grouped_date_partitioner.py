# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at

# http://www.apache.org/licenses/LICENSE-2.0

# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""A Partitioner that has date ranges grouped in another partition."""
import time
from collections import defaultdict
from datetime import datetime, timedelta

from athena_glue_service_logs.partitioners.base_partitioner import BasePartitioner
from athena_glue_service_logs.utils import S3Reader


class GroupedDatePartitioner(BasePartitioner):
    """A Partitioner that has date ranges grouped in another partition.

    In most cases, this partition is an AWS region. Many service logs write out to S3
    and group the logs by region. This partitioner is able to identity date partitions in
    each of those regions.
    """
    MAX_RECENT_DAYS = 30

    def build_partitions_from_s3(self):
        partition_values = []
        s3_reader = S3Reader(self.s3_location)

        # Scan for regions
        region_data = s3_reader.get_regions_in_partition()

        # For each region, get first date and add to data catalog
        # Then add all partitions after that date
        for region in region_data:
            first_date_tuple = s3_reader.get_first_date_in_prefix(region)
            first_partition = [region] + first_date_tuple
            partition_values.append(first_partition)
            partition_values += self._build_partition_values_for_region(region, first_date_tuple)

        return partition_values

    def partition_keys(self):
        return [
            {"Name": "region", "Type": "string"},
            {"Name": "year", "Type": "string"},
            {"Name": "month", "Type": "string"},
            {"Name": "day", "Type": "string"}
        ]

    def _build_partition_values_for_region(self, region, date_tuple):
        new_dates = self._get_date_values_since_initial_date(date_tuple)
        return [[region] + d for d in new_dates]

    def find_recent_partitions(self, existing_partitions):
        """Search for recent grouped date partitions on S3 and return a list of the partition values.

        Note that this uses the existing_partitions list to determine the region/grouping names."""
        parts_by_group = defaultdict(list)
        partitions_to_add = []

        # Create a dictionary of date partitions by region
        for part in existing_partitions:
            parts_by_group[part[0]].append(part[1:])

        # Now check to see if, in each region, that a partition day exists for today.
        # If it does not, backfill up to today.
        today = datetime.utcfromtimestamp(time.time()).date()
        day_diff = 0
        for key, values in parts_by_group.items():
            # Go back a set number of days for now and only if S3 objects actually exist...
            for _ in range(self.MAX_RECENT_DAYS):
                new_day = today + timedelta(days=day_diff)
                new_day_tuple = new_day.strftime('%Y-%m-%d').split('-')
                if values[-1] != new_day_tuple:
                    partition_tuple = [key] + new_day_tuple
                    if S3Reader(self.build_partitioned_path(partition_tuple)).does_have_objects():
                        partitions_to_add.append(partition_tuple)
                else:
                    break
                day_diff -= 1

        return partitions_to_add
