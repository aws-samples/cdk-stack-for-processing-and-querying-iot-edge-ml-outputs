from aws_cdk import (
    Aws,
    Stack,
    Duration,
    CfnParameter,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_events,
    aws_s3 as s3,
    aws_iam as iam,
    aws_iot as iot,
    aws_sns as sns,
    aws_sqs as sqs,
    aws_sns_subscriptions as subscriptions,
    aws_glue as glue,
    aws_logs as logs
)
import aws_cdk as cdk

region = Aws.REGION
account = Aws.ACCOUNT_ID

from constructs import Construct

# CDK Stack for
# 1. Create S3 bucket
# 2. Create Glue Database and Crawlers
class MQTTS3GlueSNSStack(Stack):
    """
    This stack manages data collection from ML models deployed to the edge.
    The models on IoT Greengrass publish to MQTT topics in the target account.

    Data flow:
    MQTT -> IoT Core Rule -> SNS -> SQS -> Lambda -> S3 + SNS -> Glue/Athena
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3 bucket
        bucket = s3.Bucket(self,
            "mqttdata", 
            auto_delete_objects=True,
            removal_policy=cdk.RemovalPolicy.DESTROY
        )

        # Make the lambda role
        lambda_role = iam.Role(
            self,
            "s3-sns-message-lambda-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )
        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaBasicExecutionRole"
            )
        )
        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSNSFullAccess")
        )
        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
        )

        # Grant access to the bucket
        bucket.grant_write(lambda_role)

        # Add sqs/sns queue for alerting!
        email_topic = sns.Topic(self, "MQTTTopic")
        email_address_parameter = CfnParameter(self, "targetEmail")
        email_topic.add_subscription(
            subscriptions.EmailSubscription(email_address_parameter.value_as_string)
        )

        # Add sqs/sns queue for decoupling the MQTT topic from the lambda
        # this is a well architected best practice that hardness the lambda's
        # ability to parallize and scale
        # sns -> sqs -> lambda
        processing_trigger_queue = sqs.Queue(
            self,
            id="processing_trigger_sqs_queue_id",
            visibility_timeout=Duration.seconds(30),
        )

        sqs_subscription = subscriptions.SqsSubscription(
            processing_trigger_queue, raw_message_delivery=True
        )

        processing_trigger_event_topic = sns.Topic(self, id="processing_trigger_sns_topic_id")

        # This binds the SNS Topic to the SQS Queue
        processing_trigger_event_topic.add_subscription(sqs_subscription)

        # Create Lambda function for record data transform
        lamb_fun_message_transform = _lambda.Function(
            self,
            "MessageTransform",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset("lambda-function"),
            handler="s3-sns-message-lambda.lambda_handler",
            role=lambda_role,
            environment={"email_topic": email_topic.topic_arn},
        )
        event_invoke_config = _lambda.EventInvokeConfig(
            self,
            "SMDS-DataSim-Function-EventInvokeConfig",
            function=lamb_fun_message_transform,
            max_event_age=Duration.seconds(600),
            retry_attempts=0,
        )

        # This binds the lambda to the SQS Queue
        iot_log_group = logs.LogGroup(self, "mqtt-iot-rule-errors-log-group",
            log_group_name="mqtt-iot-rule-errors-log-group",
            removal_policy=cdk.RemovalPolicy.DESTROY
        )
        invoke_event_source = lambda_events.SqsEventSource(processing_trigger_queue, batch_size=1)
        lamb_fun_message_transform.add_event_source(invoke_event_source)

        # Assign MQTT Message Routing role and method
        iot_rule_role = iam.Role(
            self,
            "iot-rule-role",
            assumed_by=iam.ServicePrincipal("iot.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "CloudWatchLogsFullAccess"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSNSFullAccess"
                ),
            ],
        )
        topic_rule = iot.CfnTopicRule(
            self,
            id="mqtt-rule",
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                actions=[
                    iot.CfnTopicRule.ActionProperty(
                        sns=iot.CfnTopicRule.SnsActionProperty(
                            role_arn=iot_rule_role.role_arn,
                            target_arn=processing_trigger_event_topic.topic_arn,
                            message_format="RAW",
                        )
                    ),
                ],
                sql="SELECT * FROM 'out/topic'",
                error_action=iot.CfnTopicRule.ActionProperty(
                    cloudwatch_logs=iot.CfnTopicRule.CloudwatchLogsActionProperty(
                        log_group_name=iot_log_group.log_group_name,
                        role_arn=iot_rule_role.role_arn
                    )
                )
            ),
        )

        # Create Glue Database
        glue_role = iam.Role(
            self,
            "AWSGlueServiceRole-mqttdata",
            role_name="AWSGlueServiceRole-mqttdata",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonS3ReadOnlyAccess"
                ),
            ],
        )

        glue_database_name = "mqttdata-db"
        database = glue.CfnDatabase(
            self,
            id=glue_database_name,
            catalog_id=account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                description=f"Glue database '{glue_database_name}'",
                name=glue_database_name,
            ),
        )
        glue_crawler_name = "mqttdata-crawler"
        glue_crawler = glue.CfnCrawler(
            self,
            glue_crawler_name,
            description="Glue Crawler for IoT MQTT",
            name=glue_crawler_name,
            database_name=glue_database_name,
            schedule={"scheduleExpression": "cron(0/30 * * * ? *)"},
            role=glue_role.role_arn,
            targets={"s3Targets": [{"path": bucket.bucket_name + "/mqttdata"}]},
            table_prefix="mqttdata-table",
        )