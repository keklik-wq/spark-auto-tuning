from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.ml.regression import IsotonicRegression, IsotonicRegressionModel
from config.AnalyzatorConfig import AnalyzatorConfig
import logging


current_spark_exector_memory = 1*1024*1024*1024
current_spark_fraction = 0.6

def convert_to_bytes(value):
    if "g" in value:
        return int(value.replace("g", "")) * 1073741824
    elif "m" in value:
        return int(value.replace("m", "")) * 1048576
    else:
        return int(value)

class Analyzator:
    def __init__(self,config: AnalyzatorConfig):
          self.config = config
          self.s3_config = config.s3_config
          self.spark_config = config.spark_config
          self.logger = logging.getLogger(__name__)

    def run(self):
        spark: SparkSession = SparkSession.builder \
            .config("spark.hadoop.fs.s3a.access.key", self.s3_config.access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", self.s3_config.secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", self.s3_config.socket) \
            .config("spark.hadoop.fs.s3a.path.style.access", True) \
            .config("spark.executor.memory","1g") \
            .config("spark.executor.cores",1) \
            .appName(self.spark_config.app_name) \
            .master(self.spark_config.master_url) \
            .getOrCreate()
        
        convert_to_bytes_udf = spark.udf.register("convert_to_bytes_udf", convert_to_bytes)

        memory: DataFrame = spark \
            .read \
            .json(self.spark_config.config_log_dir) \
            .withColumn("spark_executor_memory",convert_to_bytes_udf("spark_executor_memory"))
        
        memory.show()
        
        events: DataFrame = spark \
            .read \
            .json(self.spark_config.event_log_dir) \
            .withColumn("filename", input_file_name()) \
            .withColumn("application_attempt",element_at(split(col("filename"),"/"),-1)) 

        task_end_metrics: DataFrame = events.filter("Event='SparkListenerTaskEnd'")

        prediction1 = task_end_metrics \
            .selectExpr("application_attempt", "`Task Metrics`.*","`Task Info`.*",) \
            .selectExpr("application_attempt","`Finish Time` - `Launch Time` as duration", "`JVM GC Time`") \
            .groupBy("application_attempt") \
            .agg(sum(col("duration").cast("integer")).alias("total_duration"), \
                 sum("JVM GC Time").alias("total_gc")) \
            .withColumn("current_spark_executor_memory",lit(current_spark_exector_memory)) \
            .withColumn("prediction1", expr("(total_gc / total_duration - 0.1) * current_spark_executor_memory")) \
            .withColumn("prediction1_mb",ceil(col("prediction1") / 1048576))
            
        prediction2 = task_end_metrics \
            .selectExpr("application_attempt", "`Task Metrics`.*") \
            .groupBy("application_attempt") \
            .agg(max("`Memory Bytes Spilled`").alias("mem_spill"), max("`Disk Bytes Spilled`").alias("disk_spill")) \
            .withColumn("prediction2", expr("(mem_spill + disk_spill) / 2")) \

        metrics1: DataFrame = task_end_metrics \
        .selectExpr("application_attempt", "`Task Metrics`.*") \
        .select(
            col("`Input Metrics`.`Bytes Read`") + col("`Shuffle Read Metrics`.`Remote Bytes Read`") +
            col("`Result Size`") + col("`Shuffle Write Metrics`.`Shuffle Bytes Written`") +
            col("`Shuffle Read Metrics`.`Local Bytes Read`") + col("`Shuffle Read Metrics`.`Remote Bytes Read To Disk`") +
            col("`Output Metrics`.`Bytes Written`"), "application_attempt") \
            .toDF("bytes_all","application_attempt") \
            .groupBy("application_attempt") \
            .agg(max("bytes_all").alias("max_bytes_all")) \
            .cache()

        metrics2: DataFrame = task_end_metrics \
            .selectExpr("application_attempt", "explode(`Task Info`.Accumulables) as acc") \
            .selectExpr("application_attempt", "acc.*") \
            .where("Name = 'internal.metrics.peakExecutionMemory'") \
            .groupBy("application_attempt") \
            .agg(max(col("Update").cast("long")).alias("max_peak_mem")) \
            .cache()

        prediction3: DataFrame = metrics1 \
            .join(metrics2, "application_attempt") \
            .withColumn("current_spark.executor.memory",lit(current_spark_exector_memory)) \
            .withColumn("current_spark.memory.fraction",lit(current_spark_fraction)) \
            .withColumn("prediction3", expr("(max_bytes_all + max_peak_mem) / `current_spark.executor.memory`") - expr("`current_spark.memory.fraction` / 2") * col("`current_spark.executor.memory`")) \
            .withColumn("prediction3_mb",ceil(col("prediction3") /  1048576))
        
        prediction1.show()
        prediction2.show()
        prediction3.show()

        old_points = prediction3 \
            .join(prediction2,"application_attempt") \
            .join(prediction1,"application_attempt") \
            .join(memory,"application_attempt") \
            .withColumn("prediction",least("prediction1","prediction2","prediction3")) \
            .withColumn("mem",col("prediction") + col("spark_executor_memory")) \
            .withColumnRenamed("limit","rows")

        old_points.show()        
        old_points.printSchema()    

        train_data = old_points.select(col("mem").cast("double"),col("rows").cast("double"))   
        

        """oldModel: IsotonicRegressionModel = ...
        stats: DataFrame = old_points.select(col("limit").alias("rows"),col("spark_executor_memory"),"prediction") # rows: Double, sparkExecutorMemory: Double, prediction: Double
        oldPoints = oldModel.boundaries.zip(oldModel.predictions).toSeq.toDF("rows", "mem")
        newPoints = stats.select(col("rows"), (col("prediction") + col("spark_executor_memory")).alias("mem"))

        newTrainDF = oldPoints \
            .join(newPoints, oldPoints("rows") == newPoints("rows")) \
            .select(coalesce(newPoints("rows"), oldPoints("rows")).alias("rows"), \
            coalesce(newPoints("mem"), oldPoints("rows")).alias("mem"))
        """

        train_data.withColumn("mem",ceil(col("mem") /  1048576)).show()
        train_data.printSchema()

        new_model = IsotonicRegression(
            featuresCol="rows",
            labelCol="mem")
        
        model = new_model.fit(train_data)

        some_test = [
            (1,50),
            (2,100),
            (3,1000),
            (4,10000),
            (5,10000000)
            ]
        some_test_df: DataFrame = spark.createDataFrame(data = some_test,schema = ["id","rows"]).withColumn("rows",col("rows").cast("double")).select("rows")
        model.transform(some_test_df).withColumn("memory_prediction",ceil(col("prediction") /  1048576)).show()
        

if __name__ == "__main__":
    config = AnalyzatorConfig()
    job = Analyzator(config)
    job.run()