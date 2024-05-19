import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
import random
import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from utils.analyze_directory import analyze_directory

task_id = "spark_job"
dag_id = "spark_job_csv"
type = "csv"
row_count = 10000

input_path = f"s3a://tuning/input/{type}_{row_count}"
socket = "http://minio:9000"
access_key = "secretkey"
secret_key = "secretkey"
bucket = "tuning"

dag = DAG(
    dag_id = dag_id,
    default_args = {
        "owner": "Ivan Dudkov",
        "start_date": datetime.datetime.now(),
        "depends_on_past": True
    },
    schedule_interval = datetime.timedelta(minutes=5)
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

analyze_directory_task = PythonOperator(
    task_id='analyze_directory',
    python_callable=analyze_directory,
    op_kwargs={'bucket_name': bucket,
               'endpoint_url': socket,
               'aws_access_key_id': access_key,
               'aws_secret_access_key': secret_key
               },
    provide_context=True,
    dag=dag
)


spark_job = SparkSubmitOperator(
    task_id=task_id,
    conn_id="spark-conn",
    application="jobs/tuning/SparkJob.py",
    dag=dag,
    env_vars = {
    "socket": socket,
    "access_key": access_key,
    "secret_key": secret_key,
    "app_name": dag_id,
    "master_url": "spark://spark-master:7077",
    "job": type,
    "partitions": "1",
    "input_path": input_path,
    "output_path": "s3a://tuning/output",
    "config_log_dir": f"s3a://tuning/config_logs",
    "spark.eventLog.dir": f"s3a://tuning/event_logs",
    "need_to_join": "true",
    "spark.executor.memory": "451m",
    "spark.executor.cores": "2",
    "opp_parameters": "spark.sql.shuffle.partitions=1000;spark.default.parallelism=2000;spark.sql.adaptive.enabled=false",
    "total_num_rows": "{{ ti.xcom_pull(task_ids='analyze_directory', key = 'total_num_rows')}}",
    "max_num_columns": "{{ ti.xcom_pull(task_ids='analyze_directory', key = 'max_num_columns')}}",
    "total_serialized_size": "{{ ti.xcom_pull(task_ids='analyze_directory', key = 'total_serialized_size')}}"
    },
    jars = "jars/hadoop-aws-3.3.4.jar,jars/aws-java-sdk-bundle-1.12.353.jar"
)


end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> analyze_directory_task >> spark_job >> end


