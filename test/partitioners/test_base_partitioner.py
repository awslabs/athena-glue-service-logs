# pylint: skip-file
import time
from datetime import datetime

from athena_glue_service_logs.partitioners.base_partitioner import BasePartitioner


def test_class_init(mocker):
    mocker.patch.multiple(BasePartitioner, __abstractmethods__=set())

    base_part = BasePartitioner(s3_location="s3://nowhere")
    assert base_part.s3_location == 's3://nowhere'
    assert base_part.hive_compatible is False

    base_part = BasePartitioner('s3://nowhere', True)
    assert base_part.hive_compatible is True


def test_date_generator(mocker):
    mocker.patch.multiple(BasePartitioner, __abstractmethods__=set())
    base_part = BasePartitioner(s3_location="s3://nowhere")
    today = datetime.utcfromtimestamp(time.time()).date()

    date_tuple = ['2017', '08', '10']
    new_tuples = base_part._get_date_values_since_initial_date(date_tuple)

    assert new_tuples[0] == ['2017', '08', '11']
    assert new_tuples[-1] == today.__str__().split('-')


def test_nonhive_partitioned_path(mocker):
    mocker.patch.multiple(BasePartitioner, __abstractmethods__=set())
    base_part = BasePartitioner(s3_location="s3://nowhere")

    response = base_part.build_partitioned_path(['2017', '10', '20'])
    assert response == 's3://nowhere/2017/10/20'


def test_hive_partitioned_path(mocker):

    class TestPartitioner(BasePartitioner):
        def partition_keys(self):
            return [{'Name': 'part1'}, {'Name': 'part2'}]
    
    # Ignore other methods as we don't need them
    mocker.patch.multiple(TestPartitioner, __abstractmethods__=set())

    base_part = TestPartitioner(s3_location="s3://nowhere", hive_compatible=True)

    response = base_part.build_partitioned_path(['2017', '10'])
    assert response == 's3://nowhere/part1=2017/part2=10'
