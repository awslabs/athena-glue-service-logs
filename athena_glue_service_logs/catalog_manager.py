# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at

# http://www.apache.org/licenses/LICENSE-2.0

# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Catalog Manager includes classes for dealing with Glue Data Catalogs"""
import abc
import time
import logging

import boto3

# For now, enabe logging directly inside the module
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class BaseCatalogManager(metaclass=abc.ABCMeta):
    """This class manages the Glue Data Catalog and maintains partitions"""
    MAX_PARTITION_INPUT_PER_CALL = 100
    FILE_GROUPING_PARAMS = {'groupFiles': 'inPartition', 'groupSize': '134217728'}

    def __init__(self, region, database_name, table_name, s3_location):
        self.database_name = database_name
        self.table_name = table_name
        self.s3_location = s3_location
        self.glue_client = boto3.client('glue', region_name=region)
        self._partitioner = None

    def initialize_with_partitions(self, partition_values):
        """Create this table in the Data Catalog, and also add the provided partitions

        This performs the same actions as `initialize_table_from_s3`, you can just provide a list of partitions
        you want to initialize.
        """
        if not self.does_database_exist():
            self.create_database()

        self.create_table()

        self.create_partitions(partition_list=partition_values)

    def initialize_table_from_s3(self):
        """Create this table in the Data Catalog and, if partitions exist in S3, add them to the catalog.

        Additional artifacts may be created:
        - Creates a database as well if it does not already exist
        - If table implementation has partitions, initialize and add all partitions
        """
        if not self.does_database_exist():
            self.create_database()

        self.create_table()

        self.get_and_create_partitions()

    def create_table(self):
        """Create this database talbe in the Data Catalog"""
        LOGGER.info("Creating database table %s", self.table_name)
        self.glue_client.create_table(
            DatabaseName=self.database_name,
            TableInput=self._build_table_input()
        )

    def create_database(self):
        """Create this database in the Data Catalog"""
        LOGGER.info("Creating database %s", self.database_name)
        self.glue_client.create_database(
            DatabaseInput={
                'Name': self.database_name
            }
        )

    def get_and_create_partitions(self):
        """Create partitions in this database table based off of the files that exist in S3.

        The subclass is responsible for implementing build_partitions_from_s3 that defines how
        partitions are structured.
        """
        partition_list = self.partitioner.build_partitions_from_s3()
        self.create_partitions(partition_list)

    def create_partitions(self, partition_list):
        """Create the specified partitions in this database table"""
        # Instead of importing math.ceil and converting to floats, round up if we need to
        floor_parts = (len(partition_list) // self.MAX_PARTITION_INPUT_PER_CALL)
        remainder_exists = (len(partition_list) % self.MAX_PARTITION_INPUT_PER_CALL > 0)
        num_calls = floor_parts + remainder_exists

        for i in range(num_calls):
            start_from = i * self.MAX_PARTITION_INPUT_PER_CALL
            end_at = start_from + self.MAX_PARTITION_INPUT_PER_CALL
            self.create_partition_from_slice(partition_list, start_from, end_at)

    def create_partition_from_slice(self, partition_list, start, end):
        """Call the Glue API with the specified slice of the partition list"""
        # Create Glue PartitionInput objects from a slice of the values passed in
        partition_slice = partition_list[start:end]
        partition_inputs = [self._build_partition_input(vals) for vals in partition_slice]

        query = {
            "DatabaseName": self.database_name,
            "TableName": self.table_name,
            "PartitionInputList": partition_inputs
        }

        LOGGER.info("Batch adding %d partitions", len(partition_slice))
        self.glue_client.batch_create_partition(**query)

    def conversion_actions(self, dynamic_frame):
        """Optimized classes can optionally override this method in order to take log-specific
        actions on the raw DynamicFrame.

        The method will be responsible for importing any additional Glue Transforms or modules it might need.

        By default, this method does nothing and returns the unmodified DynamicFrame
        """
        return dynamic_frame

    def does_table_exist(self):
        """Determine if this table exists in the Data Catalog

        The Glue client will raise an exception if it does not exist.
        """
        try:
            self.glue_client.get_table(DatabaseName=self.database_name, Name=self.table_name)
            return True
        except self.glue_client.exceptions.EntityNotFoundException:
            return False

    def does_database_exist(self):
        """Determine if this database exists in the Data Catalog

        The Glue client will raise an exception if it does not exist.
        """
        try:
            self.glue_client.get_database(Name=self.database_name)
            return True
        except self.glue_client.exceptions.EntityNotFoundException:
            return False

    def get_database_name(self):
        """Database accessor"""
        return self.database_name

    def get_table_name(self):
        """Table accessor"""
        return self.table_name

    def get_partition_values(self):
        """Partition value accessor

        Returns a sorted list of tuples that make up the partition values.
        """
        partition_values = []
        args = {'DatabaseName': self.database_name, 'TableName': self.table_name}

        while True:
            partition_data = self.glue_client.get_partitions(**args)
            partition_values.extend([p['Values'] for p in partition_data['Partitions']])
            if 'NextToken' in partition_data:
                args['NextToken'] = partition_data.get('NextToken')
            else:
                break

        partition_values.sort()

        return partition_values

    def get_s3_location(self):
        """S3 URI accessor"""
        return self.s3_location

    def _build_table_input(self):
        """Build the TableInput structure for the Glue API"""
        return {
            "Name": self.table_name,
            "StorageDescriptor": self._build_storage_descriptor(),
            "PartitionKeys": self.partitioner.partition_keys(),
            "TableType": "EXTERNAL_TABLE",
            "Parameters": self._table_parameters(),  # Required or Glue create_dynamic_frame.from_catalog fails
            "LastAccessTime": time.time()
        }

    @abc.abstractmethod
    def get_partitioner(self):
        """Every data source has an associated partitioner. The partitioner is responsible for knowing the following:
            - If the data source is partitioned or not
            - What the names of the partitions are
            - If the partitions are hive-compatible or not
            - How to find new data in the partitions
            - How to build S3 prefixes for any given partition value
        """
        raise NotImplementedError

    @property
    def partitioner(self):
        """Get or create the partitioner for an implementation of this class.
        :rtype: BasePartitioner
        """
        if self._partitioner is None:
            self._partitioner = self.get_partitioner()

        return self._partitioner

    @abc.abstractmethod
    def timestamp_field(self):
        """Each data catalog may have a timestamp field. If it exists, the conversion
        process will convert the type of the column to a timestamp.
        """
        raise NotImplementedError

    def _build_partition_input(self, partition_values):
        return {
            "Values": partition_values,
            "StorageDescriptor": self._build_storage_descriptor(partition_values),
            "Parameters": {}
        }

    def _table_parameters(self):
        return {}

    @abc.abstractmethod
    def _build_storage_descriptor(self, partition_values=None):
        raise NotImplementedError

    def add_recent_partitions(self):
        """Search for new date partitions on S3 and add them if they do not exist"""
        existing_partitions = self.get_partition_values()
        partitions_to_add = self.partitioner.find_recent_partitions(existing_partitions)

        if partitions_to_add:
            self.create_partitions(partition_list=partitions_to_add)

        return len(partitions_to_add)

    def parameters_with_grouping(self, original_params={}):
        """Glue utilizes these parameters to ultimately control output file size. For ideal Athena performance,
        we want to control output file size to approximately 128MB.

        Any Raw catalog implementation should incorporate these parameters if the Converted catalog wants to
        control the file size.
        """

        # We'll always update the default params with the provided params so the consuming class can override them
        table_params = self.FILE_GROUPING_PARAMS.copy()
        table_params.update(original_params)

        return table_params
