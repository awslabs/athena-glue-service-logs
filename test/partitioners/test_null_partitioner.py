# pylint: skip-file
from athena_glue_service_logs.partitioners.null_partitioner import NullPartitioner


def test_null_partitioner():
    null_part = NullPartitioner(s3_location="s3://nowhere")

    assert not null_part.partition_keys()
    assert not null_part.build_partitions_from_s3()
