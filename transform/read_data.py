""" Module to read from csvs to spark df """
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F

class ReadData:
    """ Classe in charge of reading csvs to spark df"""
    def __init__(self, spark_session: SparkSession(), data_schema: StructType):
        self.spark_session = spark_session
        self.data_schema = data_schema


    def read_csvs(self, csv_spath:str)-> pyspark.sql.dataframe.DataFrame:
        """ read all csvs in a folder and load them in a Spark dataframe """
        # Read all files into a single DataFrame
        return self.spark_session.read.csv(csv_spath, header=True, schema=self.data_schema)

    @staticmethod
    def get_date_time_from_str(data_frame:pyspark.sql.DataFrame()):
        # Convert date_time to timestamp and then to date (without time)
        data_frame = data_frame.withColumn("date_time", F.to_timestamp(data_frame["date_time"], "yyyy-MM-dd_HH"))
        data_frame = data_frame.withColumn("date_time", F.to_date(data_frame["date_time"], "yyyy-MM-dd"))
        return data_frame

----------- HERE I AM , converting code into fucntions


    # Replace all negative values with zero in the 'visitor_count' column
    # (e.g. : closed moments(-1) or broken sensor(-10)
    df = df.withColumn("visitor_count",
                       F.when(F.col("visitor_count") < 0, 0)
                        .otherwise(F.col("visitor_count")))

    # Check for any non-numeric values in visitor_count and replace them by 0
    df = df.withColumn(
        "visitor_count",
        F.when(F.col("visitor_count").rlike(r"^\d+$"),
               F.col("visitor_count").cast(IntegerType())).otherwise(0)
    )

    # # Replace any nulls in visitor_count with 0
    # df = df.na.fill({"visitor_count": 0})

    # # Verify data types
    # df.show()
    # print(df.dtypes)

    # Perform aggregation by date_time, agency_name, and unit
    daily_count_df = df.groupBy('date_time', 'agency_name', 'unit') \
        .agg(F.sum('visitor_count').alias('total_visitor_count'))

    result_df_month = daily_count_df.withColumn("month", F.month("date_time"))

    # Perform aggregation by month(date_time), agency_name, and unit
    monthly_count_df = result_df_month.groupBy('month', 'agency_name') \
        .agg(F.sum('total_visitor_count').alias('monthly_visitor_count'))

    monthly_count_df.sort(F.col("month"), F.col("monthly_visitor_count") ).show(2000)

    # Stop SparkSession
    spark_session.stop()