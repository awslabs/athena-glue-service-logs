# pylint: skip-file
import boto3
from botocore.stub import Stubber


class S3Stubber:
    def __init__(self, for_method):
        self.client = boto3.client("s3")
        self.stubber = Stubber(self.client)
        self.method_name = for_method

    @classmethod
    def for_single_request(cls, method_name, request_params, response_objects):
        stub = cls(method_name)

        response = {
            'IsTruncated': False,
            'Contents': response_objects,
            'Name': 'string',
            'Prefix': '/',
            'Delimiter': 'string',
            'MaxKeys': 10,
            'KeyCount': len(response_objects)
        }
        stub.add_response(response, request_params)
        return stub

    @classmethod
    def for_multiple_requests(cls, method_name, request_params_list, response_objects_list):
        stub = cls(method_name)

        for req, resp in zip(request_params_list, response_objects_list):
            response = {
                'IsTruncated': False,
                'Contents': resp,
                'Name': 'string',
                'Prefix': '/',
                'Delimiter': 'string',
                'MaxKeys': 10,
                'KeyCount': len(resp)
            }
            stub.add_response(response, req)
        return stub

    def add_response(self, response_body, request_params):
        self.stubber.add_response(self.method_name, response_body, request_params)


class GlueStubber(object):
    def __init__(self):
        self.client = boto3.client("glue")
        self.stubber = Stubber(self.client)

    def add_response_for_method(self, method_name, response_body, request_params):
        self.stubber.add_response(method_name, response_body, request_params)
