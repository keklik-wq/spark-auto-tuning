import os 
import random

class S3Config():
    def __init__(self):
        self.socket = os.getenv("socket","http://localhost:9000")
        self.access_key = os.getenv("access_key","secretkey")
        self.secret_key = os.getenv("secret_key","secretkey")
        self.bucket = os.getenv("bucket","tuning")

class SparkConfig():
    def __init__(self):
        self.app_name = os.getenv("app_name","app")
        self.master_url = os.getenv("master_url","local[*]")
        self.event_log_dir = os.getenv("spark.eventLog.dir","tuning/event_logs")
        self.config_log_dir = os.getenv("config_log_dir","tuning/config_logs")
        self.opp_parameters = os.getenv("opp_parameters","spark.sql.shuffle.partitions=150;spark.sql.autoBroadcastJoinThreshold=10485760;spark.executor.cores=2")
        self.spark_executor_cores = int(os.getenv("spark_executor_cores", 2 ))
        #self.opt_properties = os.getenv("opt_properties")


class AppConfig():
    def __init__(self): 
        #files_size = [1000,10000,100000] 
        files_size = [10000]
        self.s3_config: S3Config = S3Config()
        self.spark_config: SparkConfig = SparkConfig()
        self.job: str = os.getenv("job","csv")
        self.partitions: int = int(os.getenv("partitions","200"))
        self.input_path: str = os.getenv("input_path",f"input/{self.job}_{random.choice(files_size)}")
        self.output_path: str = os.getenv("output_path",f"tuning/output/{self.job}")
        self.need_to_join: bool = bool(os.getenv("need_to_join","true"))
        self.spark_executor_memory: str = os.getenv("spark.executor.memory","1g")

    def to_dict(self):
        return {
            "s3_config": self.s3_config.__dict__,
            "spark_config": self.spark_config.__dict__,
            "job": self.job,
            "partitions": self.partitions,
            "input_path": self.input_path,
            "output_path": self.output_path,
            "need_to_join": self.need_to_join,
            "spark_executor_memory": self.spark_executor_memory
        }