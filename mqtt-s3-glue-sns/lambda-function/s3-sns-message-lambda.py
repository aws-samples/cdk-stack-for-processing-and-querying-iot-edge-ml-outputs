import base64
import logging
import os, json, boto3
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] {%(filename)s} %(levelname)s - %(message)s",)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
email_topic = os.environ["email_topic"]
logger.info("Loading function")

######################################################################################
S3_OBJECT_OUTPUT =  "mqttdata"
######################################################################################

class S3Uploads:
    def __init__(self):
        s3_bucket_name_contains = "mqttdata"
        self.s3_client =  boto3.client('s3')
        all_current_buckets = self.s3_client.list_buckets()
        self.s3_bucket = ""
        for b in all_current_buckets['Buckets']:
            if s3_bucket_name_contains in b['Name']:
                self.s3_bucket = b['Name']
        logger.info(f"S3 Bucket: {self.s3_bucket}")
    def upload(self, output_dict):
        start_time_toupload = time.time()
        date_time_now = datetime.utcnow()
        # Storing the file as a datetime partition in S3 as: s3://BUCKET/OBJECT/YYYY/MM/DD/HH/YYYY-MM-DD-HH-MM-SS-MMSECS.json
        date_time_filename = date_time_now.strftime('%Y/%m/%d/%H/') + date_time_now.strftime('%Y-%m-%d-%H-%M-%S-%f.json')
        resp = self.s3_client.put_object(Body=json.dumps(output_dict), Bucket=self.s3_bucket, Key=S3_OBJECT_OUTPUT + '/' + date_time_filename)
        logger.info(f"S3 Upload Response: {resp}")
        end_time_toupload = time.time()
        logger.info(f"Upload time: {end_time_toupload - start_time_toupload} seconds")

class SNSAlerts:
    def __init__(self):
        self.sns_client =  boto3.client('sns')
        self.topicname = "MQTTTopic"
        # Create Topic only if it is not present
        bool_topic_present = False
        all_current_topics = self.sns_client.list_topics()['Topics']
        for t in all_current_topics:
            if self.topicname in t['TopicArn']: 
                bool_topic_present = True
                self.topic = t
        if not bool_topic_present:
            self.topic = self.sns_client.create_topic(Name=self.topicname)
    
    def alerts(self, output_dict):
        start_time_toalert = time.time()
        record_message = f"Sending an Alert ... at time: {datetime.utcnow()}"
        record_message += f"\nMessage Body: \n {json.dumps(output_dict)}"
        resp = self.sns_client.publish(
            TopicArn=self.topic["TopicArn"],
            Subject=f"Alert Message",
            Message=f"Record Message: \n{record_message}.",
        )
        logger.info(f"SNS Response: {resp}")
        end_time_toalert = time.time()
        logger.info(f"Alert time: {end_time_toalert - start_time_toalert} seconds")

######################################################################################
s3upload_obj = S3Uploads()
snsalerts_obj = SNSAlerts()
######################################################################################

def lambda_handler(event, context):
    logger.info(f'EVENT DATA: {event}')
    
    body = event['Records'][0]['body']
    output_dict = json.loads(body)
    
    logger.debug('DECODED BODY STR')
    
    s3upload_obj.upload(output_dict)
    snsalerts_obj.alerts(output_dict)
    
    return {
        'statusCode': 200,
        'body': body
    }