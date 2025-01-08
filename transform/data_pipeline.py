""" Data pipeline module to transform data from raw CSVs """

import duckdb

from fuzzywuzzy import process


from pyspark.sql.functions import udf

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F


def get_closest_match(word):
    """Use fuzzywuzzy's process.extractOne to find the closest match"""
    word_list = word_list_broadcast.value  # pylint: disable=E0606
    closest_match = process.extractOne(word, word_list)
    return closest_match[0] if closest_match else None


# Register the UDF
get_closest_match_udf = udf(get_closest_match, StringType())


# ---------------------------------------------------------------------------------
class DataPipeline:
    """data pipeline to transform data from raw CSVs"""

    def __init__(self, spark_session, config_dict):
        self.spark_session = spark_session
        self.config_dict = config_dict

    # --------------------------------------------------------------------------------------
    def extract(self):
        """extract data from path according to schema"""
        schema_config = self.config_dict["schema"]
        file_path = self.config_dict["file_path"]
        return self.spark_session.read.csv(file_path, schema=schema_config, header=True)

    def extract_date(self, df):
        """Convert string date_time to timestamp, to date (without time)"""
        return df.withColumn(
            "date",
            F.to_date(F.to_timestamp(df["date_time"], "yyyy-MM-dd_HH"), "yyyy-MM-dd"),
        )

    # --------------------------------------------------------------------------------------
    def clean(self, df):
        """clean data in date_time, agency_name, counter_id, visitor_count, and unit"""
        df = self.clean_agency_name(df)
        df = self.clean_visitor_count(df)
        return df

    def clean_agency_name(self, df):
        """drop null, lower and replace white spaces in agency_names by underscores"""
        df = df.filter(F.col("agency_name").isNotNull())
        df = df.withColumn("agency_name", F.lower(F.col("agency_name")))
        df = df.withColumn(
            "agency_name", F.regexp_replace(F.col("agency_name"), " ", "_")
        )
        print("here's the matching words df")

        filtered_df = self.find_matching_agency_name(df)
        df = self.correct_agency_name(df, filtered_df)

        # df.select("agency_name").distinct().sort("agency_name").show(1000, truncate=False)

        return df

    def find_matching_agency_name(self, df):
        """lists the distinct agencies names from df
        find the match between :
        - the agency_names_from_df words and
        - the agency_names_from_db words"""
        # Select distinct words from the 'words' column
        distinct_words_df = df.select("agency_name").distinct()
        word_list = self.config_dict["agency_names"]

        # Filter out values in `distinct_words_df` that are in `word_list` (no need to change them)
        filtered_df = distinct_words_df.filter(
            ~distinct_words_df["agency_name"].isin(word_list)
        )

        # Apply the UDF to the filtered DataFrame
        result_df = filtered_df.withColumn(
            "closest_match", get_closest_match_udf(filtered_df["agency_name"])
        )
        # closest word correspondance research
        return result_df

    def correct_agency_name(self, df, filtered_df):
        """Perform Left Join on 'agency_name'"""
        joined_df = df.alias("l").join(
            filtered_df.alias("r"),
            on=(F.lower("l.agency_name") == F.lower("r.agency_name")),
            # Adjust the join condition as needed
            how="left",
        )
        # Use COALESCE to get the closest match or original name
        result_df = joined_df.withColumn(
            "new_agency_name", F.coalesce("r.closest_match", "l.agency_name")
        )

        # Drop the useless 'closest_match' column
        result_df = result_df.drop("closest_match")

        # Drop the old 'agency_name' column
        result_df = result_df.drop("agency_name")

        # Rename 'new_agency_name' to 'agency_name'
        result_df = result_df.withColumnRenamed("new_agency_name", "agency_name")

        # Show the result
        # result_df.show(truncate=False)

        return result_df

    # --------------------------------------------------------------------------------------

    def clean_visitor_count(self, df):
        """Check for any non-numeric values in visitor_count and replace them by 0
        (includes null)"""
        df = self.remove_non_numeric_visitor_count(df)
        df = self.drop_negative_visitor_count(df)
        return df

    def remove_non_numeric_visitor_count(self, df):
        """Check for any non-numeric values in visitor_count and replace them by 0
        (includes null)"""
        return df.withColumn(
            "visitor_count",
            F.when(
                F.col("visitor_count").rlike(r"^\d+$"),
                F.col("visitor_count").cast(IntegerType()),
            ).otherwise(0),
        )

    def drop_negative_visitor_count(self, df):
        """closed times (-1) or broken sensor (-10) -> visitor_count = 0"""
        return df.filter(F.col("visitor_count") > 0)

    # --------------------------------------------------------------------------------------
    def aggregate_visitor_count_daily(self, df):
        """Perform aggregation by date_time, agency_name, and unit"""
        return df.groupBy("date", "agency_name", "unit").agg(
            F.sum("visitor_count").alias("daily_visitor_count")
        )

    def aggregate_visitor_count_monthly(self, df):
        """Perform aggregation by month(date), agency_name, and unit"""
        df_with_month = df.withColumn("month", F.month("date")).withColumn(
            "year", F.year("date")
        )
        # df_with_month.show()
        # monthly_count_df.sort(F.col("month"), F.col("monthly_visitor_count") ).show(2000)
        return df_with_month.groupBy("year", "month", "agency_name").agg(
            F.sum("daily_visitor_count").alias("monthly_visitor_count")
        )

    # -----------------------------------------------------------------------------------------
    def run(self):
        """Pipeline execution"""
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

        aggregated_monthly_data = self.aggregate_visitor_count_monthly(
            aggregated_daily_data
        )
        (
            aggregated_monthly_data.sort(
                F.col("agency_name"), F.col("month"), F.col("monthly_visitor_count")
            ).show(2000)
        )

        # write
        aggregated_monthly_data.write.parquet(
            self.config_dict["output_path"], mode="overwrite"
        )


# ---------------------------------------------------------------------------------
def load_agency_name_list_from_db(path: str, table: str) -> list[str]:
    """load agency_names in a list from the db"""
    # Connect to the DuckDB database
    conn = duckdb.connect(path)

    # Execute the query and load the result directly into a pandas DataFrame
    query = f"SELECT agency_name FROM {table}"
    result = conn.execute(query).fetchall()

    # lower to ease the matching
    column_values = [row[0].lower() for row in result]

    # Close the connection
    conn.close()

    return column_values


# ---------------------------------------------------------------------------------

# Example Usage
if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder.appName(
        "Agencies_Visitor_Count_Pipeline"
    ).getOrCreate()

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

    agency_names = load_agency_name_list_from_db(
        "api/data_app/db/agencies.duckdb", "agencies"
    )
    # Broadcast word_list to make it accessible in all nodes
    word_list_broadcast = spark.sparkContext.broadcast(agency_names)

    config = {
        "schema": schema,
        "file_path": "data/raw/2024/*.csv",
        "output_path": "data/2024_agencies_month_visitor_count.parquet",
        "agency_names": agency_names,
    }

    pipeline = DataPipeline(spark, config)
    pipeline.run()

    # Stop SparkSession
    spark.stop()
