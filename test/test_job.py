import sys
import pytest

pytest.importorskip("athena_glue_service_logs.job")
pytest.importorskip("athena_glue_service_logs.alb")

from athena_glue_service_logs.job import JobRunner
from athena_glue_service_logs.alb import ALBRawCatalog, ALBConvertedCatalog


@pytest.mark.xfail(reason="requires awsglue libraries")
def test_job_creation(mocker):
    testargs = [
        '--',
        '--JOB_NAME', 'JOB_NAME',
        '--raw_database_name', 'RAW_DATABASE_NAME',
        '--raw_table_name', 'RAW_TABLE_NAME',
        '--converted_database_name', 'CONVERTED_DATABASE_NAME',
        '--converted_table_name', 'CONVERTED_TABLE_NAME',
        '--s3_source_location', 'S3_SOURCE_LOCATION',
        '--s3_converted_target', 'S3_CONVERTED_TARGET',

    ]
    mocker.patch.object(sys, 'argv', testargs)
    job = JobRunner(service_name='alb')

    assert isinstance(job.raw_catalog, ALBRawCatalog)
    assert isinstance(job.optimized_catalog, ALBConvertedCatalog)
