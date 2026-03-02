import boto3
import os
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError, ClientError

# Load environment variables from .env file
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, '.env'))

# Retrieve keys
ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')
REGION_NAME = os.getenv('AWS_REGION')

# Define local file and S3 file name
LOCAL_FILE = os.path.join(BASE_DIR, 'data', 'employees.csv')
S3_FILE_NAME = 'employees.csv' 

def upload_file():
    # Check if keys loaded correctly
    if not ACCESS_KEY or not SECRET_KEY:
        print("Error: AWS keys are not set properly.")
        return

    s3 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION_NAME
    )

    try:
        print(f"Uploading {LOCAL_FILE} to {BUCKET_NAME}...")
        s3.upload_file(LOCAL_FILE, BUCKET_NAME, S3_FILE_NAME)
        print(f"Upload Successful!")
        print(f"File is now at: s3://{BUCKET_NAME}/{S3_FILE_NAME}")
        
    except FileNotFoundError:
        print(f"Error: The file '{LOCAL_FILE}' was not found locally.")
    except NoCredentialsError:
        print(f"Error: Credentials not available.")
    except ClientError as e:
        print(f"AWS Error: {e}")

if __name__ == "__main__":
    upload_file()