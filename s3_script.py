import boto3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
S3_ENDPOINT_URL = "https://s3.cloud.ru"
S3_REGION_NAME = "ru-central-1"
BUCKET_NAME = "bucket-56dd60"
S3_PREFIX = "datasetPSB"

# Local path
# Adding \\?\ prefix to handle long paths on Windows
LOCAL_PATH = r"D:\datasetPSB"
if os.name == 'nt' and not LOCAL_PATH.startswith('\\\\?\\'):
     LOCAL_PATH = f"\\\\?\\{os.path.abspath(LOCAL_PATH)}"

# Retrieve credentials
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

if not aws_access_key_id or not aws_secret_access_key:
    print("Error: keys not found in .env")
    s3_client = None
else:
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT_URL,
            region_name=S3_REGION_NAME,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
    except Exception as e:
        print(f"Init error: {e}")
        s3_client = None

def upload_single_file(local_path, bucket, s3_path):
    try:
        s3_client.upload_file(local_path, bucket, s3_path)
        print(f"Uploaded: {s3_path}")
    except Exception as e:
        print(f"FAILED: {s3_path} -> {e}")

if s3_client:
    # Remove \\?\ for display and logic where relative paths are calculated
    display_path = LOCAL_PATH.replace('\\\\?\\', '')
    
    if os.path.isdir(LOCAL_PATH):
        print(f"Scanning directory: {display_path}")
        
        for root, dirs, files in os.walk(LOCAL_PATH):
            # Print current directory being scanned to debug recursion
            rel_root = os.path.relpath(root, LOCAL_PATH)
            if rel_root == ".":
                print(f"Processing root folder... Found {len(files)} files, {len(dirs)} subfolders.")
            else:
                print(f"--> Entering folder: {rel_root} (Files: {len(files)})")
            
            for filename in files:
                local_file_path = os.path.join(root, filename)
                
                # Calculate relative path for S3
                # We use the path without the long-path prefix for relative calculation to avoid issues
                relative_path = os.path.relpath(local_file_path, LOCAL_PATH)
                
                s3_object_name = os.path.join(S3_PREFIX, relative_path).replace("\\", "/")
                upload_single_file(local_file_path, BUCKET_NAME, s3_object_name)
                
    elif os.path.isfile(LOCAL_PATH):
        filename = os.path.basename(LOCAL_PATH)
        s3_object_name = os.path.join(S3_PREFIX, filename).replace("\\", "/")
        upload_single_file(LOCAL_PATH, BUCKET_NAME, s3_object_name)
    else:
        print(f"Error: Path {display_path} does not exist.")