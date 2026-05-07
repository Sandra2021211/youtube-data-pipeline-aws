import json
import os 
import logging
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
import awswrangler as wr
import pandas as pd

# Logging
logger=logging.getLogger()
logger.setLevel(logging.INFO)

# Config
SILVER_BUCKET=os.environ["S3_BUCKET_SILVER"]
GLUE_DB=os.environ.get("GLUE_DB_SILVER", "yt_pipeline_silver_dev")
GLUE_TABLE=os.environ.get("GLUE_TABLE_REFERENCE", "clean_reference_data")
SNS_TOPIC=os.environ.get("SNS_ALERT_TOPIC_ARN", "")
SILVER_PATH=f"s3://yt-data-pipeline-silver-ap-south2-dev/youtube/raw_statistics_reference_data/"

# AWS Clients
s3_client=boto3.client("s3")
sns_client=boto3.client("sns")



