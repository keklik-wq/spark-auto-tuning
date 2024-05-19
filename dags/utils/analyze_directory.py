from datetime import datetime, timedelta
import pyarrow.parquet as pq
import pandas as pd
import boto3
from io import BytesIO

# Определение функций для анализа файлов
def analyze_parquet_file(s3_client, bucket, key):
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        data = obj['Body'].read()
        parquet_file = pq.ParquetFile(BytesIO(data))
        
        num_rows = parquet_file.metadata.num_rows
        num_columns = parquet_file.metadata.num_columns
        serialized_size = parquet_file.metadata.serialized_size
        
        return {
            'num_rows': int(num_rows),
            'num_columns': int(num_columns),
            'serialized_size': int(serialized_size)
        }
    except Exception as e:
        print(f"Error processing {key}: {e}")
        return None

def analyze_directory(bucket_name, endpoint_url,aws_access_key_id,aws_secret_access_key, **kwargs):
    s3_client = boto3.client('s3',endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,)
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name)

    results = []
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('.parquet'):
                    analysis_result = analyze_parquet_file(s3_client, bucket_name, key)
                    if analysis_result:
                        print(f"{key}: {analysis_result}")
                        results.append(analysis_result)
    
    if results:
        df_results = pd.DataFrame(results)
        df_results.info()
        aggregated_data = {
            'total_num_rows': df_results['num_rows'].sum(),
            'max_num_columns': df_results['num_columns'].max(),
            'total_serialized_size': df_results['serialized_size'].sum()
        }

        for key in aggregated_data:
            print(f"{key}:{aggregated_data[key]}")
        # Сохранение результирующей статистики в XCom для использования в других тасках
        kwargs['ti'].xcom_push(key='total_num_rows', value=aggregated_data['total_num_rows'])
        kwargs['ti'].xcom_push(key='max_num_columns', value=aggregated_data['max_num_columns'])
        kwargs['ti'].xcom_push(key='total_serialized_size', value=aggregated_data['total_serialized_size'])

        return aggregated_data
    else:
        print("No valid Parquet files found.")
        return None

if __name__ == "__main__":
    analyze_directory("tuning","http://localhost:9000","secretkey","secretkey",)