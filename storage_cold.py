# storage_cold.py
# enables storing cold data to AWS S3

import boto3, os
from dotenv import load_dotenv
load_dotenv()  # Load from .env file

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

s3 = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

def cold_upload(file_name=None, bucket=BUCKET_NAME, s3_key=None):
    s3.upload_file(file_name, BUCKET_NAME, s3_key)
    print(f"Uploaded {file_name} to S3 bucket '{BUCKET_NAME}' at '{s3_key}'.")