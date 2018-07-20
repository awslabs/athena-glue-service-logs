# pylint: skip-file
from athena_glue_service_logs.catalog_manager import BaseCatalogManager


def test_class_init(mocker):
    mocker.patch.multiple(BaseCatalogManager, __abstractmethods__=set())

    base_catalog = BaseCatalogManager('us-west-2', 'dbname', 'tablename', 's3://somewhere')
    assert base_catalog.database_name == 'dbname'
    assert base_catalog.s3_location == 's3://somewhere'
    assert base_catalog.table_name == 'tablename'


def test_init_with_partitions(mocker):
    mocker.patch.multiple(BaseCatalogManager, __abstractmethods__=set())
    mocker.patch('athena_glue_service_logs.catalog_manager.BaseCatalogManager.does_database_exist', return_value=True)
    mocker.patch('athena_glue_service_logs.catalog_manager.BaseCatalogManager.create_database')
    mocker.patch('athena_glue_service_logs.catalog_manager.BaseCatalogManager.create_table')
    mocker.patch('athena_glue_service_logs.catalog_manager.BaseCatalogManager.create_partitions')

    base_catalog = BaseCatalogManager('us-west-2', 'dbname', 'tablename', 's3://somewhere')
    base_catalog.initialize_with_partitions(['a', 'b', 'c'])

    assert BaseCatalogManager.create_database.call_count == 0
    BaseCatalogManager.create_table.assert_called_once()
    BaseCatalogManager.create_partitions.assert_called_once_with(partition_list=['a', 'b', 'c'])

    mocker.patch('athena_glue_service_logs.catalog_manager.BaseCatalogManager.does_database_exist', return_value=False)
    base_catalog.initialize_with_partitions(['a', 'b', 'c'])
    assert BaseCatalogManager.create_database.call_count == 1
