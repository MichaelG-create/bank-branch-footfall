from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------------
class DataPipeline:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    # --------------------------------------------------------------------------------------
    def extract(self):
        """ extract data from path according to schema"""
        schema = self.config["schema"]
        file_path = self.config["file_path"]
        return self.spark.read.csv(file_path, schema=schema, header=True)

    def extract_date(self,df):
        # Convert string date_time to timestamp, to date (without time)
        return  df.withColumn("date",
                           F.to_date(F.to_timestamp(df["date_time"], "yyyy-MM-dd_HH"),
                       "yyyy-MM-dd"))
    # --------------------------------------------------------------------------------------
    def clean(self, df):
        """ clean data in date_time, agency_name, counter_id, visitor_count, and unit """
        df = self.clean_agency_name(df)
        df = self.clean_visitor_count(df)
        return df

    def clean_visitor_count(self, df):
        """ Check for any non-numeric values in visitor_count and replace them by 0
        (includes null) """
        df = self.remove_non_numeric_visitor_count(df)
        df = self.drop_negative_visitor_count(df)
        return df

    def remove_non_numeric_visitor_count(self, df):
        """ Check for any non-numeric values in visitor_count and replace them by 0
        (includes null) """
        return df.withColumn("visitor_count",
                    F.when(F.col("visitor_count").rlike(r"^\d+$"),
                        F.col("visitor_count").cast(IntegerType()))
                        .otherwise(0))

    def drop_negative_visitor_count(self, df):
        """ closed times (-1) or broken sensor (-10) -> visitor_count = 0 """
        return df.filter(F.col("visitor_count") > 0)

    def clean_agency_name(self, df):
        """ drop null, lower and replace white spaces in agency_names by underscores """
        df = df.filter(F.col("agency_name").isNotNull())

        return df.withColumn("agency_name",
                               F.regexp_replace(
                                   F.lower(F.col("agency_name")), " ", "_"))

    # --------------------------------------------------------------------------------------
    def aggregate_visitor_count_daily(self, df):
        """ Perform aggregation by date_time, agency_name, and unit """
        return df.groupBy('date', 'agency_name', 'unit') \
                    .agg(F.sum('visitor_count').alias('daily_visitor_count'))

    def aggregate_visitor_count_monthly(self, df):
        """ Perform aggregation by month(date), agency_name, and unit """
        df_with_month = df.withColumn("month", F.month("date")) \
                          .withColumn("year", F.year("date"))
        # df_with_month.show()
        # monthly_count_df.sort(F.col("month"), F.col("monthly_visitor_count") ).show(2000)
        return df_with_month.groupBy('year', 'month', 'agency_name') \
            .agg(F.sum('daily_visitor_count').alias('monthly_visitor_count'))

    # -----------------------------------------------------------------------------------------
    def run(self):
        """ Pipeline execution """
        # extract
        data = self.extract()
        # data.show()
        data_with_date = self.extract_date(data)
        # data_with_date.show()

        # clean
        clean_data = self.clean(data_with_date)
        # clean_data.show()

        # transform
        aggregated_daily_data = self.aggregate_visitor_count_daily(clean_data)
        # aggregated_daily_data.show()

        aggregated_monthly_data = self.aggregate_visitor_count_monthly(aggregated_daily_data)
        (aggregated_monthly_data.sort(F.col("month"),
                                     F.col("agency_name"),
                                     F.col("monthly_visitor_count") )
                                    .show(2000))

        # write
        aggregated_monthly_data.write.parquet(self.config["output_path"], mode="overwrite")

# ---------------------------------------------------------------------------------


# Example Usage
if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder.appName("Agencies_Visitor_Count_Pipeline").getOrCreate()

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
    config = {
        "schema": schema,
        "file_path": "data/raw/2024/*.csv",
        "output_path": "data/2024_agencies_month_visitor_count.parquet"
    }

    pipeline = DataPipeline(spark, config)
    pipeline.run()

    # Stop SparkSession
    spark.stop()