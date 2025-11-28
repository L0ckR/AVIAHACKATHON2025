import boto3
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define Cloud.ru S3 configuration
S3_ENDPOINT_URL = "https://s3.cloud.ru"
S3_REGION_NAME = "ru-central-1"

# Retrieve credentials from environment variables
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

# Check if credentials are set
if not aws_access_key_id or not aws_secret_access_key:
    print("Error: AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY are not set.")
    s3_client = None
else:
    # Initialize the S3 client
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT_URL,
            region_name=S3_REGION_NAME,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        print("Cloud.ru S3 client initialized successfully.")
    except Exception as e:
        print(f"Error initializing S3 client: {e}")
        s3_client = None

# Path to file or directory on local disk
LOCAL_PATH = r"D:\datasetPSB" 

# Bucket name
BUCKET_NAME = "bucket-56dd60"

# S3 prefix (folder inside the bucket)
S3_PREFIX = "datasetPSB"

def upload_single_file(local_path, bucket, s3_path):
    try:
        # print(f"Uploading {local_path} -> {s3_path}...")
        s3_client.upload_file(local_path, bucket, s3_path)
        print(f"Uploaded: {s3_path}")
    except Exception as e:
        print(f"{s3_path}: Failed to upload {local_path} to {bucket}/{s3_path}: {e}")

if s3_client:
    if os.path.isdir(LOCAL_PATH):
        print(f"Directory detected: {LOCAL_PATH}. Starting recursive upload...")
        
        # Walk through all files in directory and subdirectories
        for root, dirs, files in os.walk(LOCAL_PATH):
            for filename in files:
                local_file_path = os.path.join(root, filename)
                
                # Calculate relative path to maintain folder structure in S3
                relative_path = os.path.relpath(local_file_path, LOCAL_PATH)
                
                # Create S3 path (replace Windows backslashes with forward slashes)
                s3_object_name = os.path.join(S3_PREFIX, relative_path).replace("\\", "/")
                
                upload_single_file(local_file_path, BUCKET_NAME, s3_object_name)
                
    elif os.path.isfile(LOCAL_PATH):
        print(f"File detected: {LOCAL_PATH}")
        filename = os.path.basename(LOCAL_PATH)
        s3_object_name = os.path.join(S3_PREFIX, filename).replace("\\", "/")
        upload_single_file(LOCAL_PATH, BUCKET_NAME, s3_object_name)
    else:
        print(f"Error: Path {LOCAL_PATH} does not exist or is inaccessible.")
