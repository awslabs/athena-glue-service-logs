# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

def ensure_type(key, value, types):
    if value is None or isinstance(value, types):
        return value
    else:
        raise TypeError('Value {value} for key {key} should be of type {types}!'.format(value=value, key=key, types=types))

class BaseResource(object):
    ''' Only set attribute key = value if key is a valid argument with the correct type'''
    def __setattr__(self, key, value):
        if key in self._validator:
            super().__setattr__(key, ensure_type(key, value, self._validator[key][0]))

    def _getMissingFields(self):
        ''' We've determined that a required field is missing, let's find it '''
        required = set(i[0] for i in self._validator.items() if i[1][1])
        return [i for i in required if i not in self.__dict__]

    def validate(self):
        ''' Ensure all required fields are present '''
        if all(field in self.__dict__ for field, required in self._validator.items() if required[1]):
            return True
        raise TypeError('Required Fields for {} were missing: {}'.format(self.__class__.__name__, self._getMissingFields()))

    def to_json(self):
        self.validate()
        return {key: value for key, value in self.__dict__.items() if key in self._validator}