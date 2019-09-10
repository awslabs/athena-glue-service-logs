# pylint: skip-file
import time
from datetime import datetime, timedelta

from athena_glue_service_logs.partitioners.grouped_date_partitioner import GroupedDatePartitioner

from utils import S3Stubber


def region_s3_keys():
    return [
        {
            'Key': 'some_logs/us-west-2/2017/08/11/some_data.json.gz',
            'LastModified': datetime(2017, 1, 23),
            'ETag': 'string',
            'Size': 123,
            'StorageClass': 'STANDARD',
            'Owner': {
                'DisplayName': 'string',
                'ID': 'string'
            }
        },
        {
            'Key': 'some_logs/us-east-1/2018/01/09/other_data.json.gz',
            'LastModified': datetime(2018, 1, 9),
            'ETag': 'string',
            'Size': 456,
            'StorageClass': 'STANDARD',
            'Owner': {
                'DisplayName': 'string',
                'ID': 'string'
            }
        }
    ]


def response_with_prefixes():
    # Takes the sample response and extracts the prefixes.
    # Adds a trailing slash as well
    prefix_from_keys = ['/'.join(x['Key'].split('/')[0:2] + ['']) for x in region_s3_keys()]
    return {
        'IsTruncated': False,
        'Name': 'string',
        'Prefix': '/',
        'Delimiter': 'string',
        'MaxKeys': 10,
        'KeyCount': len(region_s3_keys()),
        'CommonPrefixes': [{'Prefix': x} for x in prefix_from_keys]
    }


def response_for_objects(objects):
    if not isinstance(objects, list):
        objects = [objects]
    return {
        'IsTruncated': False,
        'Contents': objects,
        'Name': 'string',
        'Prefix': '/',
        'Delimiter': 'string',
        'MaxKeys': 10,
        'KeyCount': len(objects),
    }


def delimiter_request():
    return {'Bucket': 'nowhere', 'Delimiter': '/', 'Prefix': 'some_logs/'}


def prefix_request(prefix):
    return {'Bucket': 'nowhere', 'MaxKeys': 10, 'Prefix': prefix}


def test_partition_scanner(mocker):
    date_part = GroupedDatePartitioner(s3_location="s3://nowhere/some_logs")
    today = datetime.utcfromtimestamp(time.time()).date()

    s3_stub = S3Stubber('list_objects_v2')

    s3_stub.add_response(response_with_prefixes(), delimiter_request())
    # Add in a response for each region
    for i in range(2):
        s3_stub.add_response(
            response_for_objects(region_s3_keys()[i]),
            prefix_request(response_with_prefixes()['CommonPrefixes'][i]['Prefix'])
        )

    with mocker.patch('boto3.client', return_value=s3_stub.client):
        with s3_stub.stubber:
            new_tuples = date_part.build_partitions_from_s3()

    # The first partition will be the first key in the sample data
    # The last partition will be today in the region of the last key
    assert new_tuples[0] == ['us-west-2', '2017', '08', '11']
    assert new_tuples[-1] == ['us-east-1'] + today.__str__().split('-')


def test_partition_key():
    """We should have a region in the partition key"""
    date_part = GroupedDatePartitioner(s3_location="s3://nowhere")
    key_names = [x['Name'] for x in date_part.partition_keys()]
    assert 'region' in key_names


# TODO: Abstract these out - they're hardcoded for the test below
def today_objects_request():
    today = datetime.utcfromtimestamp(time.time()).date()
    return {'Bucket': 'nowhere', 'MaxKeys': 10, 'Prefix': 'us-west-2/' + today.strftime("%Y/%m/%d")}


# TODO: Abstract these out - they're hardcoded for the test below
def today_objects():
    today = datetime.utcfromtimestamp(time.time()).date()
    return [
        {
            'Key': '/us-west-2/%s/some_data.json.gz' % today.strftime("%Y/%m/%d"),
            'LastModified': datetime(2017, 1, 23),
            'ETag': 'string',
            'Size': 123,
            'StorageClass': 'STANDARD',
            'Owner': {
                'DisplayName': 'string',
                'ID': 'string'
            }
        }
    ]


def test_find_new_partitions(mocker):
    date_part = GroupedDatePartitioner(s3_location="s3://nowhere")
    today = datetime.utcfromtimestamp(time.time()).date()
    yesterday = today - timedelta(days=1)
    existing_part = ['us-west-2'] + yesterday.__str__().split('-')

    s3_stub = S3Stubber.for_single_request('list_objects_v2', today_objects_request(), today_objects())
    with mocker.patch('boto3.client', return_value=s3_stub.client):
        with s3_stub.stubber:
            new_partitions = date_part.find_recent_partitions([existing_part])

    assert len(new_partitions) == 1
    assert new_partitions == [['us-west-2'] + today.__str__().split('-')]
