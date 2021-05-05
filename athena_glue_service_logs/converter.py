# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at

# http://www.apache.org/licenses/LICENSE-2.0

# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Converter is responsible for performing the actual conversion of files from their source data to partioned Parquet"""
import logging

# For now, enabe logging directly inside the module
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class DataConverter:
    """This class takes data from a source table and converts it into Parquet format and partitions the data

    The class used for the `optimized_catalog` variable can have a `conversion_actions` method that
    performs arbitrary conversions. It both takes and returns a Glue Dynamic Frame.
    """

    def __init__(self, glue_context, data_catalog_ref, optimized_catalog_ref):
        self.glue_context = glue_context
        self.data_catalog = data_catalog_ref
        self.optimized_catalog = optimized_catalog_ref

    def run(self):
        """Extract data from the data catalog and convert it to parquet, partitioning it along the way"""
        from awsglue.transforms import DropNullFields

        # Retrieve the source data from the Glue catalog
        source_data = self.glue_context.create_dynamic_frame.from_catalog(
            database=self.data_catalog.get_database_name(),
            table_name=self.data_catalog.get_table_name(),
            transformation_ctx="source_data"
        )

        # Perform any data-source-specific conversions
        optimized_transforms = self.optimized_catalog.conversion_actions(source_data)

        # Remove nulls and convert to dataframe - dataframe is only needed for replacing the date partitions.
        # It was previously used to repartition, but Glue supports that now.
        drop_nulls = DropNullFields.apply(frame=optimized_transforms, transformation_ctx="drop_nulls")
        data_frame = drop_nulls.toDF()

        # We might have no data - if that's the case, short-circuit
        if not data_frame.head(1):
            LOGGER.info("No data returned, skipping conversion.")
            return

        # Create Y-m-d partitions out of the optimized table's timestamp field
        df_partitions = self._replace_date_partitions(data_frame, self.data_catalog.timestamp_field())

        # Write out to partitioned parquet. We repartition to reduce the number of files to optimize Athena performance.
        # Athena queries will slow down even at 1,000 files, so we tradeoff having large files per partition rather
        # than many small files.
        (
            df_partitions
            .repartition(*self._partition_columns())
            .write
            .mode('append')
            .partitionBy(*self._partition_columns())
            .parquet(self.optimized_catalog.get_s3_location())
        )

    def _partition_columns(self):
        return [x['Name'] for x in self.optimized_catalog.partitioner.partition_keys()]  # pylint: disable=w0212

    def _replace_date_partitions(self, data_frame, date_column):
        """Extracts date partitions from the specified column, returns the dataframe with the new columns"""
        from pyspark.sql.functions import col, date_format, split
        date_col = col(date_column)
        field_names = ['year', 'month', 'day']
        expressions = [col("*")] + [split(date_format(date_column, 'yyyy-MM-dd'), '-').getItem(i).alias(name) for
                                    i, name in enumerate(field_names)]
        return (
            data_frame
            .withColumn(date_column, date_col.cast("timestamp"))
            .drop('year', 'month', 'day')
            .select(*expressions)
        )
