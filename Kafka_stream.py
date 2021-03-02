# This file contains the code to read the retail data from Kafka
import os
import sys

os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_161/jre"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] + "/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] + "/pyspark.zip")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Source Kafka Details
host_server = "18.211.252.152"
port = "9092"
topic = "real-time-project"

spark = SparkSession.builder.appName("retailDataAnalyzer").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

order_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", host_server + ":" + port) \
    .option("startingOffsets", "latest") \
    .option("subscribe", topic) \
    .option("failOnDataLoss", False) \
    .load()

dataSchema = StructType() \
    .add("invoice_no", LongType()) \
    .add("country", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("type", StringType()) \
    .add("items", ArrayType(StructType([
    StructField("SKU", StringType()),
    StructField("title", StringType()),
    StructField("unit_price", DoubleType()),
    StructField("quantity", IntegerType())
])))

order_data = order_data.selectExpr("cast(value as string)")
order_data_stream = order_data.select(from_json(col="value", schema=dataSchema).alias("retail_data")).select(
    "retail_data.*")

output_data = order_data_stream \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

output_data.awaitTermination()

