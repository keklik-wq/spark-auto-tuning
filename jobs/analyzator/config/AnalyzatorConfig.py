import os

class S3Config():
    def __init__(self):
        self.socket = os.getenv("socket","http://localhost:9000")
        self.access_key = os.getenv("access_key","secretkey")
        self.secret_key = os.getenv("secret_key","secretkey")
        self.bucket = os.getenv("bucket","tuning")

class SparkConfig():
    def __init__(self):
        self.app_name = os.getenv("app_name","app")
        self.master_url = os.getenv("master_url","local[2]")
        self.event_log_dir = os.getenv("spark.eventLog.dir","tuning/event_logs_1")
        self.config_log_dir = os.getenv("config_log_dir","tuning/config_logs_1")
        #self.opt_properties = os.getenv("opt_properties")

class AnalyzatorConfig():
    def __init__(self):
        self.s3_config = S3Config()
        self.spark_config = SparkConfig()