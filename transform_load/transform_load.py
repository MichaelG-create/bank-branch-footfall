# pylint: disable=duplicate-code
"""Data pipeline module to transform raw CSVs
with possible errors to clean parquet"""

import logging
import os
import subprocess

import duckdb
from fuzzywuzzy import process
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


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

    # pylint: disable=R0904

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
            F.to_date(
                F.to_timestamp(df["date_time"], "yyyy-MM-dd HH:mm"), "yyyy-MM-dd"
            ),
        )

    # --------------------------------------------------------------------------------------
    def clean(self, df):
        """clean data in date_time, agency_name, counter_id, visitor_count, and unit"""
        df = self.clean_agency_name(df)
        df = self.clean_visitor_count(df)
        df = self.clean_counter_id(df)
        df = self.clean_unit(df)

        return df

    # --------------------------------------------------------------------------------------
    def clean_agency_name(self, df):
        """drop nulls, lower agency_name, replace white spaces by underscores"""
        df = df.filter(F.col("agency_name").isNotNull())
        df = df.withColumn("agency_name", F.lower(F.col("agency_name")))
        df = df.withColumn(
            "agency_name", F.regexp_replace(F.col("agency_name"), " ", "_")
        )

        filtered_df = self.find_matching_agency_name(df)
        return self.correct_agency_name(df, filtered_df)

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
        """Drop any non-numeric or zero or negative values in visitor_count
        (includes null)"""
        df = self.drop_non_numeric_visitor_count(df)
        df = self.drop_negative_visitor_count(df)
        return df

    def drop_non_numeric_visitor_count(self, df):
        """Drop any non-numeric values in visitor_count
        (includes null)"""
        return df.filter(F.col("visitor_count").rlike(r"^\d+$"))

    def drop_negative_visitor_count(self, df):
        """drop lines : closed times (-1) or broken sensor (-10)"""
        return df.filter(F.col("visitor_count") > 0)

    # --------------------------------------------------------------------------------------
    def clean_counter_id(self, df):
        """drop non-numeric values in counter_id (includes null)"""
        df = self.drop_non_numeric_counter_id(df)
        df = self.drop_negative_counter_id(df)
        df = self.drop_outlier_counter_id(df)
        return df

    def drop_non_numeric_counter_id(self, df):
        """Drop non-numeric values in counter_id (includes null)"""
        return df.filter(F.col("counter_id").rlike(r"^\d+$"))

    def drop_negative_counter_id(self, df):
        """counter_id must be >=0 and int"""
        return df.filter(F.col("counter_id") >= 0)

    def drop_outlier_counter_id(self, df):
        """counter_id must be >=0 and int"""
        return df.filter(F.col("counter_id") <= 10)

    # --------------------------------------------------------------------------------------
    def clean_unit(self, df):
        """drop nulls, lower 'unit', keep only exact matching to 'visitors'"""
        df = df.filter(F.col("unit").isNotNull())
        df = df.withColumn("unit", F.lower(F.col("unit")))
        return df.filter(F.col("unit") == "visitors")

    # --------------------------------------------------------------------------------------
    def aggregate_visitor_count_daily(self, df):
        """Perform aggregation by date_time, agency_name, and unit"""
        return df.groupBy("date", "agency_name", "counter_id", "unit").agg(
            F.sum("visitor_count").alias("daily_visitor_count")
        )

    def aggregate_visitor_count_monthly(self, df):
        """Perform aggregation by month(date), agency_name, and unit"""
        df_with_month = df.withColumn("month", F.month("date")).withColumn(
            "year", F.year("date")
        )
        return df_with_month.groupBy("year", "month", "agency_name", "counter_id").agg(
            F.sum("daily_visitor_count").alias("monthly_visitor_count")
        )

    def add_weekday(self, df):
        """Add weekday column to df"""
        return df.withColumn("weekday", F.dayofweek(F.col("date")))

    def add_mov_avg_4_previous_same_weekdays(self, df):
        """Moving average 4 previous weekdays"""
        window_spec = (
            Window.partitionBy("agency_name", "counter_id", "weekday")
            .orderBy("date")
            .rowsBetween(-3, 0)
        )  # 3 preceding lines up to current one
        # avg over the window
        return df.withColumn(
            "avg_visits_4_weekday",
            F.round(F.avg(F.col("daily_visitor_count")).over(window_spec)),
        )

    def add_previous_mv_avg_4(self, df):
        """add previous column of moving average 4 previous weekdays"""
        window_spec = Window.partitionBy(
            "agency_name", "counter_id", "weekday"
        ).orderBy("date")
        return df.withColumn(
            "prev_avg_4_visits", F.lag("avg_visits_4_weekday", 1).over(window_spec)
        )

    def add_pct_change_over_mv_avg_4(self, df):
        """add pct change over moving average 4 previous weekdays"""
        return df.withColumn(
            "pct_change",
            F.round(
                100 * (F.col("daily_visitor_count") / F.col("prev_avg_4_visits") - 1)
            ),
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
        agg_daily_data = self.aggregate_visitor_count_daily(clean_data)
        # agg_daily_data.show()

        # # in case it's used later on for stats
        # agg_monthly_data = self.aggregate_visitor_count_monthly(
        #     agg_daily_data
        # )

        agg_daily_data_with_weekday = self.add_weekday(agg_daily_data)

        daily_with_mv_avg_4_weekdays = self.add_mov_avg_4_previous_same_weekdays(
            agg_daily_data_with_weekday
        )

        daily_mv_avg_4_w_prev_mv_avg = self.add_previous_mv_avg_4(
            daily_with_mv_avg_4_weekdays
        )

        daily_with_percent_change = self.add_pct_change_over_mv_avg_4(
            daily_mv_avg_4_w_prev_mv_avg
        )

        # (
        #     daily_with_percent_change.sort(
        #         F.col("agency_name"),
        #         F.col("counter_id"),
        #         F.col("weekday"),
        #         F.col("date"),
        #     ).show(2000)
        # )

        # daily_with_percent_change.show()

        output_path = f'{self.config_dict["output_path"]}'

        if os.path.exists(output_path):
            existing_data = spark.read.schema(  # pylint: disable=E0606
                self.config_dict["parquet_schema"]
            ).parquet(output_path)
            existing_data.show()
            # df = spark.read.parquet("file:///path/to/your/file.parquet")
            # existing_data.printSchema()

            # Assuming there is a unique identifier column, e.g., 'id'
            # You can adjust this logic based on the key or the structure of your data
            filtered_data = daily_with_percent_change.join(
                existing_data,
                on=[
                    "date",
                    "agency_name",
                    "counter_id",
                ],  # Adjust this list to match the key column(s) for uniqueness
                how="left_anti",
            )
            # Combine the existing data with the new data (filtered_data) to ensure no duplicates
            final_data = existing_data.union(filtered_data).distinct()
            final_data = final_data.orderBy("agency_name", "counter_id", "date")
            final_data.show()
            # Write the filtered data in append mode
            (
                final_data.write
                # .partitionBy("agency_name", "counter_id", "date")
                .mode("overwrite").parquet(output_path)
            )

        else:
            # Write a new file if it doesn't exist
            (
                daily_with_percent_change.write.mode(
                    "overwrite"
                ).parquet(  # Use "overwrite" to write a new file
                    output_path
                )
                # .schema(self.config_dict["parquet_schema"])
            )


# ---------------------------------------------------------------------------------
def load_agency_name_list_from_db(path: str, table: str) -> list[str]:
    """load agency_names in a list from the db"""
    # Connect to the DuckDB database
    # Check if the database exists
    if not os.path.exists(path):
        # Si elle n'existe pas, initialiser la base de données
        # If connection fails, initialize the database by running the init script
        logging.warning("Database not found at %s. Initializing database.",path)
        subprocess.run(["python", "data/data_base/init_agencies_db.py"], check=True)

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
    logging.info("Running data_pipeline")
    PROJECT_PATH = ""
    DB_PATH = PROJECT_PATH + "data/data_base/agencies.duckdb"
    TABLE = "agencies"

    # Initialize SparkSession
    spark = SparkSession.builder.appName(
        "Agencies_Visitor_Count_Pipeline"
    ).getOrCreate()

    # Define schema for the CSV columns
    schema = StructType(
        [
            StructField("date_time", StringType(), False),
            StructField("agency_name", StringType(), False),
            StructField("counter_id", IntegerType(), False),
            StructField("visitor_count", IntegerType(), False),
            StructField("unit", StringType(), True),
        ]
    )

    # Define schema for the parquet columns
    parquet_schema = StructType(
        [
            StructField("date", DateType(), True),
            StructField("agency_name", StringType(), True),
            StructField("counter_id", IntegerType(), True),
            StructField("unit", StringType(), True),
            StructField("daily_visitor_count", LongType(), True),
            StructField("weekday", IntegerType(), True),
            StructField("avg_visits_4_weekday", DoubleType(), True),
            StructField("prev_avg_4_visits", DoubleType(), True),
            StructField("pct_chge", DoubleType(), True),
        ]
    )

    agency_names = load_agency_name_list_from_db(DB_PATH, TABLE)
    # Broadcast word_list to make it accessible in all nodes
    word_list_broadcast = spark.sparkContext.broadcast(agency_names)

    config = {
        "schema": schema,
        "parquet_schema": parquet_schema,
        # "file_path": "data/raw/2024/*.csv",
        "file_path": PROJECT_PATH + "data/raw/cli/*.csv",
        "output_path": PROJECT_PATH + "data/filtered",
        "agency_names": agency_names,
    }

    pipeline = DataPipeline(spark, config)
    pipeline.run()

    # Stop SparkSession
    spark.stop()
