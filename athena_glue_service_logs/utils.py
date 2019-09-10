# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at

# http://www.apache.org/licenses/LICENSE-2.0

# or in the "license" file accompanying this file. This file is distributed 
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either 
# express or implied. See the License for the specific language governing 
# permissions and limitations under the License.

"""Utility methods for AthenaGlueServiceLogs module"""
import re
from urllib.parse import urlparse

import boto3


class S3Reader:
    """Utility class to access S3

    Provide an S3 URI (s3://<bucket>/<prefix>) to initialize. All operations will execute in the
    provided bucket.
    """
    def __init__(self, s3_uri):
        self.s3_client = boto3.client("s3")
        self.s3_uri = s3_uri

        s3_url = urlparse(s3_uri)
        self.s3_bucket = s3_url.netloc
        self.s3_path = self._strip_slashes(s3_url.path)

    def get_regions_in_partition(self, prefix=None, delimiter='/'):
        """Scan a given prefix and return all regions in that partition with an optional delimiter

        Returns a list of AWS region names that exist in prefix
        """
        if prefix is None:
            prefix = self.s3_path
        else:
            prefix = self._strip_slashes(prefix)

        query_params = {
            'Bucket': self.s3_bucket,
            'Prefix': prefix + '/',
            'Delimiter': delimiter
        }

        # We currently should be able to get all regions in a single request
        # TODO: Fail if we get a next token - there's more to this prefix than meets the eye
        region_list = []
        response = self.s3_client.list_objects_v2(**query_params)
        for c_prefix in response.get('CommonPrefixes', []):
            region = self._extract_region_from_prefix(c_prefix)
            if region:
                region_list.append(region)

        return region_list

    def get_first_date_in_prefix(self, prefix=None):
        """A simple method to return the first date in a given prefix that is partiioned like Y/m/d
        If a prefix is provided, it will be appended to the original path provided to this class.

        Returns a tuple of the date in [year, month, date] format.
        """
        first_key = self._get_first_key_in_prefix(prefix)

        # Verify we have a matching format
        if re.search(r'/\d{4}/\d{2}/\d{2}/', first_key) is None:
            raise Exception("No date partitions found in prefix: s3://%s/%s" % (self.s3_bucket, prefix))

        date_tuple = first_key.split('/')[-4:-1]

        return date_tuple
    
    def get_first_hivecompatible_date_in_prefix(self, partition_keys, prefix=None):
        """A method to return the first date in a given prefix, when using hive-compatible partitions"""
        first_key = self._get_first_key_in_prefix(prefix)

        # Verify we have a matching format
        regexp = "/" + '/'.join(["%s=\d+" % key for key in partition_keys]) + "/"
        if re.search(regexp, first_key) is None:
            raise Exception("No Hive-compatible date partitions found in prefix: s3://%s/%s" % (self.s3_bucket, prefix))

        # Do we want to return ['Y', 'M', 'D'] or ['year=Y', 'month=M', 'day=D'?
        date_tuple = [part.split('=')[1] for part in first_key.split('/')[-4:-1]]

        return date_tuple


    def does_have_objects(self):
        """Let's us know if there are objects in whatever path was provided to S3Reader"""
        response = self.s3_client.list_objects_v2(
            Bucket=self.s3_bucket,
            MaxKeys=10,
            Prefix=self.s3_path
        )

        return response.get('KeyCount', 0) > 0

    def _extract_region_from_prefix(self, prefix_object):
        match = re.search(r'.*/(\w+-\w+-\d)/$', prefix_object['Prefix'])
        if match:
            return match.group(1)
        else:
            return None

    def _strip_slashes(self, value):
        return value.lstrip('/').rstrip('/')
    
    def _get_first_key_in_prefix(self, prefix=None):
        if prefix is None:
            prefix = self.s3_path
        else:
            prefix = '/'.join([self.s3_path, self._strip_slashes(prefix)])
        
        # We only will ever need the first response, so set MaxKeys to a low number to reduce overhead
        query_params = {
            'Bucket': self.s3_bucket,
            'Prefix': prefix + '/',
            'MaxKeys': 10
        }

        # Get the first key in this prefix and extract the date partitions from the S3 Key
        response = self.s3_client.list_objects_v2(**query_params)
        first_object = response.get('Contents')[0].get('Key')

        return first_object