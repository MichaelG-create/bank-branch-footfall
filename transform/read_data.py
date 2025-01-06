from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read CSV Files") \
    .getOrCreate()

# Define schema for the CSV columns
schema = StructType([
    StructField("date_time", StringType(), False),
    StructField("agency_name", StringType(), False),
    StructField("counter_id", IntegerType(), True),
    StructField("visitor_count", IntegerType(), False),
    StructField("unit", StringType(), True)
])

# Read all files into a single DataFrame
df = spark.read.csv("data/raw/*.csv", header=True, schema=schema)

# Convert date_time to timestamp and then to date (without time)
df = df.withColumn("date_time", F.to_timestamp(df["date_time"], "yyyy-MM-dd_HH"))
df = df.withColumn("date_time", F.to_date(df["date_time"], "yyyy-MM-dd"))

# Check for any non-numeric values in visitor_count
df = df.withColumn(
    "visitor_count",
    F.when(F.col("visitor_count").rlike("^\d+$"),
           F.col("visitor_count").cast(IntegerType())).otherwise(None)
)

# Replace any nulls in visitor_count with 0
df = df.na.fill({"visitor_count": 0})

# # Verify data types
# df.show()
# print(df.dtypes)

# Perform aggregation by date_time, agency_name, and unit
result_df = df.groupBy('date_time', 'agency_name', 'unit') \
    .agg(F.sum('visitor_count').alias('total_visitor_count'))


result_df_month = result_df.withColumn("month", F.month("date_time"))

# result_df_month.show()

# Perform aggregation by month(date_time), agency_name, and unit
result_df = result_df_month.groupBy('month', 'agency_name') \
    .agg(F.sum('total_visitor_count').alias('monthly_visitor_count'))

result_df.sort(F.col("month"), F.col("monthly_visitor_count") ).show(2000)


# Stop SparkSession
spark.stop()