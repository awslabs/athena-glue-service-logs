# pylint: skip-file
import time
from datetime import datetime, timedelta

from athena_glue_service_logs.partitioners.date_partitioner import DatePartitioner

from utils import S3Stubber


def today():
    return datetime.utcfromtimestamp(time.time()).date()


def yesterday():
    return datetime.utcfromtimestamp(time.time()).date() - timedelta(days=1)


def basic_s3_key():
    return {
        'Key': '/2017/08/11/some_data.json.gz',
        'LastModified': datetime(2017, 1, 23),
        'ETag': 'string',
        'Size': 123,
        'StorageClass': 'STANDARD',
        'Owner': {
            'DisplayName': 'string',
            'ID': 'string'
        }
    }


def request_params():
    return {'Bucket': 'nowhere', 'MaxKeys': 10, 'Prefix': '/'}


def today_objects():
    return [
        {
            'Key': '/%s/some_data.json.gz' % today().strftime("%Y/%m/%d"),
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
            'Key': '/%s/more_data.json.gz' % today().strftime("%Y/%m/%d"),
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


def yesterday_objects():
        return [
            {
                'Key': '/%s/some_data.json.gz' % yesterday().strftime("%Y/%m/%d"),
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


def list_request_for_ts(timestamp):
    return {'Bucket': 'nowhere', 'MaxKeys': 10, 'Prefix': timestamp.strftime("%Y/%m/%d")}


def today_objects_request():
    return {'Bucket': 'nowhere', 'MaxKeys': 10, 'Prefix': today().strftime("%Y/%m/%d")}


def test_partition_scanner(mocker):
    date_part = DatePartitioner(s3_location="s3://nowhere")
    today = datetime.utcfromtimestamp(time.time()).date()

    s3_stub = S3Stubber.for_single_request('list_objects_v2', request_params(), [basic_s3_key()])
    with mocker.patch('boto3.client', return_value=s3_stub.client):
        with s3_stub.stubber:
            new_tuples = date_part.build_partitions_from_s3()

    assert new_tuples[0] == ['2017', '08', '11']
    assert new_tuples[-1] == today.__str__().split('-')


def test_partition_builder():
    date_part = DatePartitioner(s3_location="s3://nowhere")
    response = date_part.build_partitioned_path(['2017', '08', '11'])

    assert response == 's3://nowhere/2017/08/11'


def test_partition_key_order():
    """Partition keys should be returned in order"""
    date_part = DatePartitioner(s3_location="s3://nowhere")
    key_names = [x['Name'] for x in date_part.partition_keys()]
    assert key_names == ['year', 'month', 'day']


def test_find_new_partitions(mocker):
    date_part = DatePartitioner(s3_location="s3://nowhere")
    existing_part = yesterday().__str__().split('-')

    s3_stub = S3Stubber.for_single_request('list_objects_v2', today_objects_request(), today_objects())
    with mocker.patch('boto3.client', return_value=s3_stub.client):
        with s3_stub.stubber:
            new_partitions = date_part.find_recent_partitions([existing_part])

    assert len(new_partitions) == 1
    assert new_partitions == [today().__str__().split('-')]


def test_find_all_new_partitions(mocker):
    date_part = DatePartitioner(s3_location="s3://nowhere")

    requests = []
    # Create request parameters for every day since (and including) today
    for i in range(DatePartitioner.MAX_RECENT_DAYS):
        requests.append(list_request_for_ts(today() - timedelta(days=i)))

    s3_stub = S3Stubber.for_multiple_requests(
        'list_objects_v2',
        requests,
        [today_objects(), yesterday_objects()] + [[]]*(DatePartitioner.MAX_RECENT_DAYS-2)
    )
    with mocker.patch('boto3.client', return_value=s3_stub.client):
        with s3_stub.stubber:
            new_partitions = date_part.find_recent_partitions([])

    # Only 2 days have data
    assert len(new_partitions) == 2
    assert new_partitions == [today().__str__().split('-'), yesterday().__str__().split('-')]
