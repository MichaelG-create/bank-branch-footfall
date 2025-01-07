from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F

from transform.read_data import ReadData

# Initialize SparkSession
spark = SparkSession.builder.appName("Read CSV Files").getOrCreate()

# Define schema for the CSV columns
schema = StructType(
    [
        StructField("date_time", StringType(), False),
        StructField("agency_name", StringType(), False),
        StructField("counter_id", IntegerType(), True),
        StructField("visitor_count", IntegerType(), False),
        StructField("unit", StringType(), True),
    ]
)

reader = ReadData(spark, schema)

df = reader.read_csvs("data/raw/*.csv")

df = reader.get_date_time_from_str(df)

