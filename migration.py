import boto3
from botocore.client import Config
import os

def uploadDirectory(path,s3_bucket):
    for root,dirs,files in os.walk(path):
        for file in files:
            s3_bucket.upload_file(os.path.join(root,file),f"{root}/{file}")
            print(f"Загружен {root}/{file}")
            
# Создаем соединение с Minio
s3 = boto3.resource(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='secretkey',
    aws_secret_access_key='secretkey',
    config=Config(signature_version='s3v4')
)

s3_bucket = s3.Bucket('tuning')

uploadDirectory('input',s3_bucket)

# Загрузка файла some.txt в папку tuning/event_logs
s3.Bucket('tuning').upload_file('infra.sh', 'event_logs/some.txt')

# Загрузка файла some.txt в папку tuning/config_logs
s3.Bucket('tuning').upload_file('infra.sh', 'config_logs/some.txt')

