import csv
import requests
import boto3
from botocore.exceptions import NoCredentialsError
from urllib.parse import urlparse
from dotenv import load_dotenv
import os

load_dotenv()

# AWS credentials
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')

# S3 bucket details
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Cloudflare Storage endpoint
CLOUDFLARE_STORAGE_ENDPOINT = os.getenv('CLOUDFLARE_STORAGE_ENDPOINT')

def upload_to_s3(content, object_key):
    try:
        s3 = boto3.client('s3', endpoint_url=CLOUDFLARE_STORAGE_ENDPOINT, aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name="auto")
        s3.put_object(Body=content, Bucket=S3_BUCKET_NAME, Key=object_key)
        print(f"Successfully uploaded content to S3 with object key: {object_key}")
    except NoCredentialsError:
        print("Credentials not available or incorrect.")

def download_file(url):
    response = requests.get(url)
    return response.content, response.headers.get('Content-Type', 'application/octet-stream')

def read_csv_file(file_path):
    links = []
    try:
        with open(file_path, 'r') as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                if row:  # Check if the row is not empty
                    url = row[0]
                    links.append(url)
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

    return links

if __name__ == "__main__":
    csv_file_name = 'file.csv'
    csv_file_path = f'./{csv_file_name}'
    links = read_csv_file(csv_file_path)

    for index, url in enumerate(links, start=1):
        try:
            binary_content, content_type = download_file(url)
            file_name = urlparse(url).path.split("/")[-1]
            object_key = f"{file_name}"

            # Upload content to S3
            upload_to_s3(binary_content, object_key)
        except Exception as e:
            print(f"Failed to retrieve content from URL: {url}. Error: {e}")