# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from .base_resource import *
from enum import Enum

class UpdateBehavior(Enum):
    LOG = "LOG"
    UPDATE_IN_DATABASE = "UPDATE_IN_DATABASE"

    @staticmethod
    def has_value(item):
        return item in [v.value for v in UpdateBehavior.__members__.values()]

class DeleteBehavior(Enum):
    LOG = "LOG"
    DELETE_FROM_DATABASE = "DELETE_FROM_DATABASE"
    DEPRECATE_IN_DATABASE = "DEPRECATE_IN_DATABASE"

    @staticmethod
    def has_value(item):
        return item in [v.value for v in DeleteBehavior.__members__.values()]

class CatalogTarget(BaseResource):
    def __init__(self, **kwargs):
        self.__dict__['_validator'] = {
            'DatabaseName': (str, True),
            'Tables': (list, True)
        }
        self.DatabaseName = kwargs.get('DatabaseName')
        self.Tables = kwargs.get('Tables')

class DynamoDBTarget(BaseResource):
    def __init__(self, **kwargs):
        self.__dict__['_validator'] = {
            'Path': (str, True)
        }
        self.Path = kwargs.get('Path')

class S3Target(BaseResource):
    def __init__(self, **kwargs):
        self.__dict__['_validator'] = {
            'Path': (str, True),
            'Exclusions': (list, False)
        }
        self.Path = kwargs.get('Path')
        self.Exclusions = kwargs.get('Exclusions')

class JDBCTarget(BaseResource):
    def __init__(self, **kwargs):
        self.__dict__['_validator'] = {
            'ConnectionName': (str, True),
            'Path': (str, True),
            'Exclusions': (list, False)
        }
        self.ConnectionName = kwargs.get('ConnectionName')
        self.Path = kwargs.get('Path')
        self.Exclusions = kwargs.get('Exclusions')

class SchemaChangePolicy(BaseResource):
    def __init__(self, **kwargs):
        self.__dict__['_validator'] = {
            'DeleteBehavior': (str, False),
            'UpdateBehavior': (str, False)
        }
        self.DeleteBehavior = kwargs.get('DeleteBehavior')
        self.UpdateBehavior = kwargs.get('UpdateBehavior')

    def __setattr__(self, key, value):
        if key in self._validator and value:
            value = ensure_type(key, value, self._validator[key][0])
            if key == 'DeleteBehavior' and not DeleteBehavior.has_value(value):
                raise TypeError('{} is not a valid value for key {} in {}'.format(value, key, self.__class__.__name__))
            if key == 'UpdateBehavior' and not UpdateBehavior.has_value(value):
                raise TypeError('{} is not a valid value for key {} in {}'.format(value, key, self.__class__.__name__))
            super().__setattr__(key, value)

class Targets(BaseResource):
    def __init__(self, **kwargs):
        self.__dict__['_validator'] = {
            'S3Targets': (list, False),
            'JDBCTargets': (list, False),
            'CatalogTargets': (list, False),
            'DynamoDBTargets': (list, False)
        }
        self.S3Targets = kwargs.get('S3Targets')
        self.JDBCTargets = kwargs.get('JDBCTargets')
        self.CatalogTargets = kwargs.get('CatalogTargets')
        self.DynamoDBTargets = kwargs.get('DynamoDBTargets')

    def validate(self):
        if self.S3Targets:
            for s3_target in self.S3Targets: S3Target(**s3_target).validate()
        if self.JDBCTargets:
            for jdbc_target in self.JDBCTargets: JDBCTarget(**jdbc_target).validate()
        if self.CatalogTargets:
            for catalog_target in self.CatalogTargets: CatalogTarget(**catalog_target).validate()
        if self.DynamoDBTargets:
            for dynamodb_target in self.DynamoDBTargets: DynamoDBTarget(**dynamodb_target).validate()

class Crawler(BaseResource):
    def __init__(self, **kwargs):
        self.__dict__['_validator'] = {
            'Name': (str, True),
            'Description': (str, False),
            'Role': (str, True),
            'Targets': (dict, True),
            'DatabaseName': (str, False),
            'Classifiers': (list, False),
            'SchemaChangePolicy': (dict, False),
            'TablePrefix': (str, False),
            'Configuration': (str, False),
            'CrawlerSecurityConfiguration': (str, False),
            'Tags': (dict, False),
            'DependsOn': (dict, False),
            'Schedule': (str, False),
            'WaitForDependencies': (str, False)
        }
        self.Name = kwargs.get('Name')
        self.Description = kwargs.get('Description')
        self.Role = kwargs.get('Role')
        self.Targets = kwargs.get('Targets')
        self.DatabaseName = kwargs.get('DatabaseName')
        self.Classifiers = kwargs.get('Classifiers')
        self.SchemaChangePolicy = kwargs.get('SchemaChangePolicy')
        self.TablePrefix = kwargs.get('TablePrefix')
        self.Configuration = kwargs.get('Configuration')
        self.CrawlerSecurityConfiguration = kwargs.get('CrawlerSecurityConfiguration')
        self.Tags = kwargs.get('Tags')
        self.DependsOn = kwargs.get('DependsOn')
        self.Schedule = kwargs.get('Schedule')
        self.WaitForDependencies = kwargs.get('WaitForDependencies')

    def __setattr__(self, key, value):
        if key in self._validator and value:
            ensure_type(key, value, self._validator[key][0])
            if key == 'Targets':
                Targets(**value).validate()
            elif key == 'SchemaChangePolicy':
                SchemaChangePolicy(**value).validate()
            super().__setattr__(key,value)