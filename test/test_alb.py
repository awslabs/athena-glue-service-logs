# pylint: skip-file
import pytest

from athena_glue_service_logs.alb import ALBConvertedCatalog, ALBRawCatalog


@pytest.fixture(scope="module")
def raw_catalog():
    return ALBRawCatalog(
        'us-west-2',
        'test',
        'test_converted',
        's3://some-bucket/a_prefix'
    )


@pytest.fixture(scope="module")
def converted_catalog():
    return ALBConvertedCatalog(
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
    assert raw_catalog.timestamp_field() == 'time'
    assert converted_catalog.timestamp_field() == 'time'
