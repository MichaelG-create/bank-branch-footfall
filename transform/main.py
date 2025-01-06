from pyspark.sql import SparkSession
from transform.read_data import ReadData


# Initialize SparkSession
read_data = ReadData(SparkSession.builder \
                    .appName("Read CSV Files") \
                    .getOrCreate())

