# This file contains the code for reading and processing the retail data using PySpark
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_161/jre"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] + "/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] + "/pyspark.zip")

# Source Kafka Details
host_server = "18.211.252.152"
port = "9092"
topic = "real-time-project"


# Create UDFs
def total_order_value(items, order_type):
    """
    Function to calculate the total cost for each Invoice.
    If the type is order, the value is positive else Negative
    :param items: (List) List of items in a single order
    :param order_type:(String) Order or Return
    :return: (Double) Total cost of items in the Invoice
    """
    cost = 0
    type_flag = 1 if order_type == "ORDER" else -1
    for item in items:
        cost += type_flag * (item['unit_price'] * item['quantity'])

    return cost


def total_items(items):
    """
    Function to calculate the total quantity for all items for each Invoice.
    :param items: (List) List of items in a single order
    :return: (Integer) Total quantity for all items for each Invoice.
    """
    quantity = 0
    for item in items:
        quantity += item['quantity']

    return quantity


def order_flag(order_type):
    """
    Function to check if the order type is order
    :param order_type:(String) Order Type
    :return: (Integer) Flag
    """
    return 1 if order_type.upper() == 'ORDER' else 0


def return_flag(order_type):
    """
    Function to check if the order type is return
        :param order_type:(String) Order Type
        :return: (Integer) Flag
        """
    return 1 if order_type.upper() == 'RETURN' else 0


spark = SparkSession.builder.appName("retailDataAnalyzer").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read the data stream from Kafka to Spark
order_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", host_server + ":" + port) \
    .option("startingOffsets", "latest") \
    .option("subscribe", topic) \
    .option("failOnDataLoss", False) \
    .load()

# Define the schema object for the data
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
# Attach the schema to the spark dataframe
order_data_stream = order_data.select(from_json(col="value", schema=dataSchema).alias("retail_data")).select(
    "retail_data.*")

# Define the UDF Functions
add_total_order_value = udf(total_order_value, DoubleType())
add_total_quantity = udf(total_items, IntegerType())
add_order_flag = udf(order_flag, IntegerType())
add_return_flag = udf(return_flag, IntegerType())

# Create the transformed data with all udf functions
Transformed_data = order_data_stream \
    .withColumn('total_cost', add_total_order_value(order_data_stream['items'], order_data_stream['type'])) \
    .withColumn('total_quantity', add_total_quantity(order_data_stream['items'])) \
    .withColumn('is_order', add_order_flag(order_data_stream['type'])) \
    .withColumn('is_return', add_return_flag(order_data_stream['type']))

# Calculate the KPIs
# Time based KPIs with 1 minute tumbling window
time_based_KPIs = Transformed_data \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "1 minute", "1 minute")) \
    .agg(format_number(sum("total_cost"), 2).alias("total_sales_volume"),
         avg("total_cost"),
         count("invoice_no").alias("OPM"),
         avg("is_return")) \
    .select("window", "OPM", "total_sales_volume",
            format_number("avg(total_cost)", 2).alias("average_transaction_cost"),
            format_number("avg(is_return)", 2).alias("rate_of_return"))

# Time and country based KPIs with 1 minute tumbling window
time_and_country_based_KPIs = Transformed_data \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "1 minute", "1 minute"), "country") \
    .agg(format_number(sum("total_cost"), 2).alias("total_sales_volume"),
         count("country").alias("OPM"),
         avg("is_return")) \
    .select("window", "country", "OPM", "total_sales_volume",
            format_number("avg(is_return)", 2).alias("rate_of_return"))

# Write Time based KPIs to json files
time_based_output_data = time_based_KPIs \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("truncate", False) \
    .option("path", "tmp/time_based_kpis") \
    .option("checkpointLocation", "tmp/time_kpi_check") \
    .trigger(processingTime="1 minute") \
    .start()

# Write Time and country based KPIs to json files
time_and_country_based_output_data = time_and_country_based_KPIs \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("truncate", False) \
    .option("path", "tmp/time_country_based_kpis") \
    .option("checkpointLocation", "tmp/time_country_kpi_check") \
    .trigger(processingTime="1 minute") \
    .start()

# Print the input transformed data to the console
output_data = Transformed_data \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="1 minute") \
    .start()

output_data.awaitTermination()
