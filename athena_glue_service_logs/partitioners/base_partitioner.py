# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at

# http://www.apache.org/licenses/LICENSE-2.0

# or in the "license" file accompanying this file. This file is distributed 
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either 
# express or implied. See the License for the specific language governing 
# permissions and limitations under the License.

"""Base Partitioner that defines how various service logs are structed in S3"""
import abc
import time
from datetime import datetime, timedelta, date


class BasePartitioner(metaclass=abc.ABCMeta):
    """Partioner classes help identify what a given service log's file structure in S3 is."""

    def __init__(self, s3_location, hive_compatible=False):
        """Create a new instance of a partitioner. An S3 location must be provided.

        Optionally, a partition can be hive-compatible. The impact of setting that to true is
        the S3 paths will have the name of the partition in the folder structer. e.g.
            <bucket>/<prefix>/logs/year=12/month=10/day=02

        By default, partitions are not hive-compatible.
        """
        self.s3_location = s3_location
        self.hive_compatible = hive_compatible

    @abc.abstractmethod
    def build_partitions_from_s3(self):
        """Scan S3 and return a list of tuples of partition values."""

    @abc.abstractmethod
    def partition_keys(self):
        """The name and types of the partition keys for the given partition type that this class implements."""

    @abc.abstractmethod
    def find_recent_partitions(self, existing_partitions):
        """Find partitions that have been added recently"""

    def _get_date_values_since_initial_date(self, date_tuple):
        """Takes an intial date tuple and returns a computed set of date tuples until UTC now

        Does _not_ include the original date tuple.
        """
        partitions = []
        date_ints = list(map(int, date_tuple))
        initial_date = date(*date_ints)
        today = datetime.utcfromtimestamp(time.time()).date()  # Similar to date.today() but accounts for UTC

        for i in range((today-initial_date).days):
            new_date = initial_date + timedelta(days=i+1)
            part = new_date.isoformat().split('-')
            partitions.append(part)

        return partitions

    def build_partitioned_path(self, partition_values):
        """Takes a tuple of partition values and returns the S3 URI for that specific partition."""
        base_path = self.s3_location.rstrip('/')
        uri_parts = partition_values

        if self.hive_compatible:
            uri_parts = self._get_hive_partitioned_parts(partition_values)

        location = '/'.join(
            [base_path] + uri_parts
        )

        return location

    def _get_hive_partitioned_parts(self, partition_values):
        """Takes a list of partition values and returns a list of Hive-compatible key names"""
        partition_names = [val['Name'] for val in self.partition_keys()]
        partitions = list(map('='.join, list(zip(partition_names, partition_values))))

        return partitions
