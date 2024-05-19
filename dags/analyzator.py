import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "analyzator",
    default_args = {
        "owner": "Ivan Dudkov",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

analyzator = SparkSubmitOperator(
    task_id="analyzator",
    conn_id="spark-conn",
    application="jobs/analyzator/AnalyzatorSpark.py",
    dag=dag,
    env_vars = {
    "socket": "http://host.docker.internal:9000",
    "access_key": "secretkey",
    "secret_key": "secretkey",
    "app_name": "Spark App",
    "master_url": "spark://spark-master:7077",
    "config_log_dir": "s3a://tuning/config_logs",
    "spark.eventLog.dir": "s3a://tuning/event_logs",
    "bool": "true",
    "spark.executor.memory": "1g",
    },
    jars = "jars/hadoop-aws-3.3.4.jar,jars/aws-java-sdk-bundle-1.12.353.jar"
)


end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> analyzator >> end
