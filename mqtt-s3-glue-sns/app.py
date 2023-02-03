#!/usr/bin/env python3
import os

import aws_cdk as cdk

from cdk.mqtt_s3_glue_sns import MQTTS3GlueSNSStack


app = cdk.App()
MQTTS3GlueSNSStack(app, "MQTTS3GlueSNSStack",)

app.synth()
