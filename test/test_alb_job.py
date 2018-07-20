# pylint: skip-file
"""When ALB tables are first created, the raw and converted tables should have the same partitions"""
import pytest
from datetime import datetime, timedelta
import botocore

from athena_glue_service_logs.alb import ALBRawCatalog, ALBConvertedCatalog


@pytest.fixture(scope="module")
def raw_catalog():
    return ALBRawCatalog(
        'us-west-2',
        'test_db',
        'test_table',
        's3://some-bucket/a_prefix/with_logs'
    )


@pytest.fixture(scope="module")
def converted_catalog():
    return ALBConvertedCatalog(
        'us-west-2',
        'test_db',
        'test_table',
        's3://some-bucket/a_prefix/with_logs'
    )


def return_s3_objects(*args, **kwargs):
    # First ListObjects looking for regions
    if args[0] == 'ListObjectsV2' and 'Delimiter' in args[1]:
        prefix = args[1]['Prefix']
        return {
            'IsTruncated': False,
            'Name': 'string',
            'Prefix': '/',
            'Delimiter': 'string',
            'MaxKeys': 10,
            'KeyCount': 1,
            'CommonPrefixes': [{'Prefix': '%s/us-west-2/' % prefix}]
        }
    # Second ListObjects looking for objects in the region
    elif args[0] == 'ListObjectsV2' and 'Delimiter' not in args[1]:
        two_days_ago = (datetime.utcnow() - timedelta(days=2)).date()
        prefix = args[1]['Prefix']
        two_days_ago_str = two_days_ago.strftime("%Y/%m/%d")
        return {
            'IsTruncated': False,
            'Contents': [
                {
                    'Key': '%s/us-west-2/%s/some_data.json.gz' % (prefix, two_days_ago_str),
                    'LastModified': two_days_ago,
                    'ETag': 'string',
                    'Size': 123,
                    'StorageClass': 'STANDARD',
                    'Owner': {
                        'DisplayName': 'string',
                        'ID': 'string'
                    }
                }
            ],
            'Name': 'string',
            'Prefix': '/',
            'Delimiter': 'string',
            'MaxKeys': 10,
            'KeyCount': 1,
        }


def test_raw_table_creation(raw_catalog, mocker):
    """When initailizing a table from S3, there will be 5 AWS calls for ALB/DateGroupedPartitioner
        1. Check if table exists in Glue Data Catalog (for the purpose of this test, it will not)
        2. Create table
        3. Get list of regions in S3
        4. Get list of objects in each region
        5. Batch create partitions in the table
    """
    mocker.patch('botocore.client.BaseClient._make_api_call', side_effect=return_s3_objects)
    raw_catalog.initialize_table_from_s3()

    # Ensure all expected API calls were made
    assert botocore.client.BaseClient._make_api_call.call_count == 5

    # Check the last method called to make sure we inserted 3 (two days ago up until today) partitions
    args, _ = botocore.client.BaseClient._make_api_call.call_args
    assert args[0] == 'BatchCreatePartition'
    assert len(args[1]['PartitionInputList']) == 3

    # Ensure the partitions created are _not_ hive-compatible
    s3_location = args[1]['PartitionInputList'][0]['StorageDescriptor']['Location']
    assert '=' not in s3_location
    assert '/us-west-2/' in s3_location
