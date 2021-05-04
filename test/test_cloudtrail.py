# pylint: skip-file
import pytest
from datetime import datetime, timedelta
from random import choice, randrange

from athena_glue_service_logs.cloudtrail import CloudTrailRawCatalog, CloudTrailConvertedCatalog
from utils import GlueStubber


@pytest.fixture(scope="module")
def raw_catalog():
    return CloudTrailRawCatalog(
        'us-west-2',
        'test',
        'test_converted',
        's3://some-bucket/a_prefix'
    )


@pytest.fixture(scope="module")
def converted_catalog():
    return CloudTrailConvertedCatalog(
        'us-west-2',
        'test',
        'test_converted',
        's3://some-bucket/a_prefix'
    )


def test_converted_build_partitioned_path(converted_catalog):
    """Converted catalog will have have-compatible partitions"""
    part_values = ['us-west-2', '2017', '12', '16']
    path = converted_catalog.partitioner.build_partitioned_path(part_values)

    assert path == 's3://some-bucket/a_prefix/region=us-west-2/year=2017/month=12/day=16'


def test_raw_build_partitoined_path(raw_catalog):
    """Raw catalog will not have have-compatible partitions"""
    part_values = ['us-west-2', '2017', '12', '16']
    path = raw_catalog.partitioner.build_partitioned_path(part_values)

    assert path == 's3://some-bucket/a_prefix/us-west-2/2017/12/16'


def test_storage_descriptor_with_no_partitions(raw_catalog, converted_catalog):
    descriptor = raw_catalog._build_storage_descriptor()
    assert 'Columns' in descriptor
    assert descriptor['Location'] == 's3://some-bucket/a_prefix'

    descriptor = converted_catalog._build_storage_descriptor()
    assert 'Columns' in descriptor
    assert descriptor['Location'] == 's3://some-bucket/a_prefix'


def test_storage_descriptor_with_partitions(raw_catalog, converted_catalog):
    descriptor = raw_catalog._build_storage_descriptor(['us-west-2', '2017', '12', '25'])
    assert 'Columns' in descriptor
    assert descriptor['Location'] == 's3://some-bucket/a_prefix/us-west-2/2017/12/25'

    descriptor = converted_catalog._build_storage_descriptor(['us-west-2', '2017', '12', '25'])
    assert 'Columns' in descriptor
    assert descriptor['Location'] == 's3://some-bucket/a_prefix/region=us-west-2/year=2017/month=12/day=25'


def test_timestamp_field(raw_catalog, converted_catalog):
    assert raw_catalog.timestamp_field() == 'eventtime'
    assert converted_catalog.timestamp_field() == 'eventtime'


def test_table_params(raw_catalog):
    """CloudTrail also has custom table parameters it needs for Glue compatibility"""
    assert raw_catalog._table_parameters()['classification'] == 'cloudtrail'
    assert raw_catalog._table_parameters()['jsonPath'] == '$.Records[*]'

    # Let's try with the full table_input as well
    table_input = raw_catalog._build_table_input()
    assert 'jsonPath' in table_input['Parameters']


def test_partition_pagination(raw_catalog, mocker):
    """Tests that we properly find new partitions when partition count exceeds what is returned by Glue API call"""
    params = {'DatabaseName': 'dcortesi', 'TableName': 'partest'}
    single_glue_stub = GlueStubber()
    single_glue_stub.add_response_for_method('get_partitions', build_glue_response(num=5), params)

    multi_glue_stub = GlueStubber()
    token = 'deadbeef=='
    multi_glue_stub.add_response_for_method('get_partitions', build_glue_response(num=5, next_token=token), params)
    next_params = params.copy()
    next_params['NextToken'] = token
    multi_glue_stub.add_response_for_method('get_partitions', build_glue_response(num=7), next_params)

    # First just make sure the get_partition_values method works
    with mocker.patch('boto3.client', return_value=single_glue_stub.client):
        with single_glue_stub.stubber:
            c = CloudTrailRawCatalog('us-west-2', params.get('DatabaseName'), params.get('TableName'), 's3://blah')
            resp = c.get_partition_values()
            assert len(resp) == 5

    # Next make sure that when there are multiple partition results, we get them all
    with mocker.patch('boto3.client', return_value=multi_glue_stub.client):
        with multi_glue_stub.stubber:
            c = CloudTrailRawCatalog('us-west-2', params.get('DatabaseName'), params.get('TableName'), 's3://blah')
            resp = c.get_partition_values()
            assert len(resp) == 12


def build_glue_response(num, next_token=None):
    """Helper to programatically create a Glue API response"""
    available_regions = [
        'ap-northeast-1',
        'ap-northeast-2',
        'ap-northeast-3',
        'ap-south-1',
        'ap-southeast-1',
        'ap-southeast-2',
        'ca-central-1',
        'eu-central-1',
        'eu-north-1',
        'eu-west-1',
        'eu-west-2',
        'eu-west-3',
        'sa-east-1',
        'us-east-1',
        'us-east-2',
        'us-west-1',
        'us-west-2'
    ]
    response = {'Partitions': []}
    if next_token is not None:
        response['NextToken'] = next_token
    start_date = datetime.utcnow().date()
    part_date = start_date

    for i in range(num):
        part = [choice(available_regions)] + part_date.strftime("%Y-%m-%d").split("-")
        part_date = part_date + timedelta(days=-randrange(30))
        response['Partitions'].append({'Values': part})

    return response
