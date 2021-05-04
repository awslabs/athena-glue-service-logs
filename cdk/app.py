#!/usr/bin/env python3
import os

from aws_cdk import core as cdk

from cdk.cdk_stack import CdkStack

# Region is required for ELBv2 access logging
region = os.environ.get('CDK_DEFAULT_REGION')

app = cdk.App()
CdkStack(app, "CdkStack",env=cdk.Environment(region=region))

app.synth()
