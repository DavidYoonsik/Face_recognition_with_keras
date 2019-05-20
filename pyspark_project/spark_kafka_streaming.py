import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, IntegerType, StringType
from pyspark.sql import *


def getSparkSessionInstance(sparkConf):
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).getOrCreate()
    return globals()['sparkSessionSingletonInstance']


if __name__ == "__main__":
    conf = SparkConf()
    conf.set("partition.assignment.strategy", "range")
    spark = SparkSession.builder.config(conf=SparkConf()).appName('hello').getOrCreate()


    df = spark.readStream.format("kafka").option('kafka.bootstrap.servers', '192.168.2.12:2181').option('subscribe', 'suricata12').option("partition.assignment.strategy", "range").load()
    df.printSchema()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    schema = StructType().add("a", IntegerType()).add("b", StringType())
    df.select(col("key").cast("string"), from_json(col("value").cast("string"), schema))
    schema = StructType().add("a", IntegerType()).add("b", StringType())
    query = df.selectExpr("CAST(value AS STRING) AS key", "to_json(struct(*)) AS value").writeStream.outputMode("complete").format("console").queryName("counts").start()
    # df.head()
    query.awaitTermination()