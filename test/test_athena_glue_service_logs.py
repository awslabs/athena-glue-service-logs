import pytest

from athena_glue_service_logs import version


def test_version():
    assert len(version.__version__.split('.')) == 3, "Incorrect version format"
