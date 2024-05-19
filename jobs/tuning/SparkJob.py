import time
from typing import List, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, concat, lit, DataFrame, count
from config.AppConfig import AppConfig
from botocore.client import Config
import logging
import os
import json
import boto3 
import datetime
import os

class SparkJob:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.s3_config = config.s3_config
        self.spark_config = config.spark_config
        self.logger = logging.getLogger(__name__)

    def run(self) -> None:
        spark: SparkSession = SparkSession.builder \
            .config("spark.hadoop.fs.s3a.access.key", self.s3_config.access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", self.s3_config.secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", self.s3_config.socket) \
            .config("spark.hadoop.fs.s3a.path.style.access", True) \
            .config("spark.executor.memory",self.config.spark_executor_memory) \
            .config("spark.eventLog.enabled", True) \
            .config("spark.history.provider","org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.eventLog.dir", self.config.spark_config.event_log_dir) \
            .config("spark.logConf", True) \
            .config("spark.executor.cores",self.config.spark_config.spark_executor_cores) \
            .appName(self.spark_config.app_name) \
            .master(self.spark_config.master_url) \
            .getOrCreate()
        
        self.save_job_params(spark)

        time_start = datetime.datetime.now()
        
        spark_parameters = self.parse_opp_parameters(self.spark_config.opp_parameters)
        self.update_spark_parameters(spark=spark,spark_parameters=spark_parameters)

        for elem in spark.sparkContext.getConf().getAll():
            print(elem)

        main_df: DataFrame = self.read_data(self.config.job, spark)
        
        print(f"Non cached main_df partitons: {main_df.rdd.getNumPartitions()}")

        main_df.cache()
        print(f"MAIN_DF: {main_df.count()}")

        df_with_fio: DataFrame = main_df.withColumn("fio", concat_ws(" ", "surname", "name", "patronymic"))
        df_with_fio.cache()
        print(f"df_with_fio: {df_with_fio.count()}")
        self.logger.info(f"DFwithFIO: {df_with_fio.count()}")

        df_with_address: DataFrame = main_df.withColumn("address", concat_ws(",", "street", "city"))
        df_with_address.cache()
        print(f"df_with_address: {df_with_address.count()}")
        self.logger.info(f"DFwithAddress: {df_with_address.count()}")

        df_with_document = main_df.withColumn(
            "document",
            concat_ws(" ", "series_passport", "number_passport")
        )
        df_with_document.cache()
        self.logger.info(f"DFwithDocument: {df_with_document.count()}")

        renamed_df: DataFrame = df_with_address.withColumnsRenamed({
            "surname": "another_surname",
            "name": "another_name",
            "patronymic": "another_patronymic",
            "phone": "another_phone",
            "street": "another_street",
            "city": "another_city",
            "series_passport": "another_series_passport",
            "number_passport": "another_number_passport",
            "birthday": "another_birthday",
            "created_dt": "another_created_dt",
            "updated_dt": "another_updated_dt",
            "address": "another_address",
        })


        # Предположим, что у нас есть два DataFrame: main_df и renamed_df
        main_df_surname_counts = main_df.groupBy("surname").agg(count("*").alias("count")).sort("count",ascending = False)
        renamed_df_another_surname_counts = renamed_df.groupBy("another_surname").agg(count("*").alias("count")).sort("count",ascending = False)

        # Выводим результаты
        print("Counts for main_df.surname:")
        main_df_surname_counts.show()

        print("Counts for renamed_df.another_surname:")  
        renamed_df_another_surname_counts.show()

        print("Partitions for main_df:", main_df.rdd.getNumPartitions()) 
        print("Partitions for renamed_df:", renamed_df.rdd.getNumPartitions()) 
        
        if self.config.need_to_join:
            joined_df = main_df.join(renamed_df, main_df.surname == renamed_df.another_surname, "inner")
            joined_df.cache()
            print(f"joinedDF: {joined_df.count()}")
            print("Partitions for joined_df:", joined_df.rdd.getNumPartitions()) 

            joined_df.write.mode("overwrite").parquet(self.config.output_path)
        
            time_end = datetime.datetime.now()
            print(f"time: {time_end - time_start}")


    def read_data(self, format, spark):
        if format == "csv":
            return spark.read.options(header="true").csv(self.config.input_path)
        elif format == "parquet":
            return spark.read.parquet(self.config.input_path)
        
    def save_job_params(self,spark: SparkSession) -> None:
        """
        Сохраняет параметры Spark job в JSON Lines файл.

        Args:
            spark (pyspark.sql.SparkSession): Сессия Spark.
            output_dir (str): Директория для сохранения файла.
        """

        app_id = spark.sparkContext.applicationId

        session = boto3.Session()
        s3 = session.client(
            's3',
            endpoint_url=self.config.s3_config.socket,
            aws_access_key_id=self.config.s3_config.access_key,
            aws_secret_access_key=self.config.s3_config.secret_key,
            config=Config(signature_version='s3v4')
        )

        config_dict = self.config.to_dict()
        for name, value in os.environ.items():
            config_dict.update({name: value})
        
        for item in self.parse_opp_parameters(self.spark_config.opp_parameters):
            config_dict.update({item[0]:item[1]})

        config_dict.update({"application_attempt": app_id})

        # Преобразуем объект в JSON и сохраняем в файле
        json_data = json.dumps(config_dict)
        json_name = f"{app_id}.json"
        with open(json_name, 'w') as f:
            f.write(json_data)

        # Загружаем файл на S3
        s3.upload_file(json_name, self.config.s3_config.bucket, f"config_logs/{json_name}")
        os.remove(json_name)
        self.logger.info(f'File {json_name} with config logs was downloaded to S3: s3a://{self.s3_config.bucket}/{json_name}')

    def parse_opp_parameters(self,opp_parameters: str) -> List[Tuple[str, str]]:
        divided_opp_parameters = opp_parameters.split(";")
        result_list = []
        for item in divided_opp_parameters:
            pairs = item.split("=")
            pair = (pairs[0], pairs[1])
            result_list.append(pair)
        self.logger.info(f"Parsed opp_parameters: {result_list}")
        return result_list
    
    def update_spark_parameters(self,spark: SparkSession, spark_parameters: List[Tuple[str, str]]) -> None:
            spark.sparkContext._conf.setAll(spark_parameters)

if __name__ == "__main__":
    config = AppConfig()
    job = SparkJob(config)
    job.run()