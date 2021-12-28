# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from .base_resource import *

class CommandName(Enum):
    GLUE_ETL = "glueetl"
    PYTHON_SHELL = "pythonshell"
    GLUE_STREAMING = "gluestreaming"

    @staticmethod
    def has_value(item):
        return item in [v.value for v in CommandName.__members__.values()]

class WorkerType(Enum):
    STANDARD = "Standard"
    G1_X = "G.1X"
    G2_X = "G.2X"

    @staticmethod
    def has_value(item):
        return item in [v.value for v in WorkerType.__members__.values()]

class Command(BaseResource):
    def __init__(self, **kwargs):
        self.__dict__['_validator'] = {
            'Name': (str, True),
            'ScriptLocation': (str, True),
            'PythonVersion': (str, False)
        }
        self.Name = kwargs.get('Name')
        self.ScriptLocation = kwargs.get('ScriptLocation')
        self.PythonVersion = kwargs.get('PythonVersion')

    def __setattr__(self, key, value):
        if key in self._validator and value:
            value = ensure_type(key, value, self._validator[key][0])
            if key == 'Name' and not CommandName.has_value(value):
                raise TypeError('{} is not a valid value for key {} in {}'.format(value, key, self.__class__.__name__))
            super().__setattr__(key, value)

class ExecutionProperty(BaseResource):
    def __init__(self, **kwargs):
        self.__dict__['_validator'] = {
            'MaxConcurrentRuns': (int, False),
        }
        self.MaxConcurrentRuns = kwargs.get('MaxConcurrentRuns')

class Job(BaseResource):
    def __init__(self, **kwargs):
        ''' Field name -> (expected type, required bool) '''
        self.__dict__['_validator'] = {
            'Name': (str, True),
            'Description': (str, False),
            'LogUri': (str, False),
            'Role': (str, True),
            'ExecutionProperty': (dict, False),
            'Command': (dict, True),
            'DefaultArguments': (dict, False),
            'NonOverridableArguments': (dict, False),
            'Connections': (dict, False),
            'MaxRetries': (int, False),
            'AllocatedCapacity': (int, False),
            'Timeout': (int, False),
            'MaxCapacity': (float, False),
            'SecurityConfiguration': (str, False),
            'Tags': (dict, False),
            'NotificationProperty': (dict, False),
            'GlueVersion': (str, False),
            'NumberOfWorkers': (int, False),
            'WorkerType': (str, False),
            'DependsOn': (dict, False),
            'WaitForDependencies': (str, False)
        }
        self.Name = kwargs.get('Name')
        self.Description = kwargs.get('description')
        self.LogUri = kwargs.get('LogUri')
        self.Role = kwargs.get('Role')
        self.ExecutionProperty = kwargs.get('ExecutionProperty')
        self.Command = kwargs.get('Command')
        self.DefaultArguments = kwargs.get('DefaultArguments')
        self.NonOverridableArguments = kwargs.get('NonOverridableArguments')
        self.Connections = kwargs.get('Connections')
        self.MaxRetries = kwargs.get('MaxRetries')
        self.AllocatedCapacity = kwargs.get('AllocatedCapacity')
        self.Timeout = kwargs.get('Timeout')
        self.MaxCapacity = kwargs.get('MaxCapacity')
        self.WorkerType = kwargs.get('WorkerType')
        self.NumberOfWorkers = kwargs.get('NumberOfWorkers')
        self.SecurityConfiguration = kwargs.get('SecurityConfiguration')
        self.NotificationProperty = kwargs.get('NotificationProperty')
        self.GlueVersion = kwargs.get('GlueVersion')
        self.Tags = kwargs.get('Tags')
        self.DependsOn = kwargs.get('DependsOn')
        self.WaitForDependencies = kwargs.get('WaitForDependencies')

    def __setattr__(self, key, value):
        if key in self._validator and value:
            value = ensure_type(key, value, self._validator[key][0])
            if key == 'Command':
                Command(**value).validate()
            elif key == 'ExecutionProperty':
                ExecutionProperty(**value).validate()
            elif key == 'WorkerType' and not WorkerType.has_value(value):
                raise TypeError('{} is not a valid value for worker type'.format(value))
            super().__setattr__(key, value)