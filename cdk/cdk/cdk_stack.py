from aws_cdk import core as cdk
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_ecs_patterns as ecs_patterns
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3d
from aws_cdk import aws_cloudfront as cloudfront
from aws_cdk import aws_cloudfront_origins as origins


class CdkStack(cdk.Stack):
    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create a centralized bucket for all our logging
        log_bucket = s3.Bucket(
            self,
            "agsl-log-bucket",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        cdk.CfnOutput(self, "agsl-log-bucket-uri", value=log_bucket.s3_url_for_object())

        # Create a VPC for our ECS cluster to live in
        vpc = ec2.Vpc(self, "agsl-vpc")
        vpc.add_flow_log(
            "flowlog-s3",
            destination=ec2.FlowLogDestination.to_s3(
                bucket=log_bucket, key_prefix="logs/vpcflow/"
            ),
        )

        self.create_s3_app(log_bucket)
        self.create_ecs_app(log_bucket, vpc)

    def create_s3_app(self, log_bucket: s3.Bucket):
        """
        Create an S3 bucket with a demo 2048 app that is served by CloudFront.

        Note that you can _either_ add the `website_index_document` to the s3 bucket *OR*
        add the `default_root_object` to the CloudFront Distribution, but if you do the former
        the bucket has to be publicly accessible.
        More details here: https://github.com/aws/aws-cdk/issues/14019
        """
        app_bucket = s3.Bucket(
            self,
            "app-bucket",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            server_access_logs_bucket=log_bucket,
            server_access_logs_prefix="logs/s3/",
        )

        deployment = s3d.BucketDeployment(
            self,
            "deployment-2048",
            destination_bucket=app_bucket,
            sources=[s3d.Source.asset("./2048")],
        )

        distribution = cloudfront.Distribution(
            self,
            "www-s3-2048",
            default_behavior=cloudfront.BehaviorOptions(
                origin=origins.S3Origin(app_bucket),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
            ),
            default_root_object="index.html",
            log_bucket=log_bucket,
            log_file_prefix="logs/cloudfront/",
            enable_logging=True,
        )
        cdk.CfnOutput(self, "cloudfront-endpoint", value=distribution.domain_name)

    def create_ecs_app(self, log_bucket: s3.Bucket, vpc: ec2.Vpc):
        alb_service = ecs_patterns.ApplicationLoadBalancedFargateService(
            self,
            "www-ecs-2048",
            vpc=vpc,
            memory_limit_mib=512,
            cpu=256,
            task_image_options={
                "image": ecs.ContainerImage.from_registry("alexwhen/docker-2048")
            },
        )
        alb_service.load_balancer.log_access_logs(bucket=log_bucket, prefix="logs/alb")
        cdk.CfnOutput(
            self, "alb-endpoint", value=alb_service.load_balancer.load_balancer_dns_name
        )
