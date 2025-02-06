"""
agencies app maker
"""

import logging
import os

import duckdb
import pandas as pd

from api.data_app.agency import Agency
from api.data_app.db.init_agencies_db import create_agencies_db


def create_agencies(db_path: str, table_name: str) -> dict[str, Agency]:
    """
    Create the list of all agencies objects in our api
    from the agencies database
    :return: dict('agency_name' : Agency())
    """

    # if table does not exist, create it
    if not os.path.exists(db_path):
        create_agencies_db(db_path, table_name)

    # load agencies characteristics
    agencies_df = load_agencies_from_db_to_dataframe(db_path, table_name)

    # Create an empty dictionary to store the agencies
    data_dict = {}

    # Iterate through the DataFrame row by row
    for _, row in agencies_df.iterrows():
        # Create an Agency object for each row
        agency_row = Agency(
            row["agency_name"],
            row["agency_size"],
            row["location_type"],
            row["base_traffic"],
            row["counter_number"],
        )

        # Add the agency to the dictionary, using the agency_name as the key
        data_dict[row["agency_name"]] = agency_row

    return data_dict


def load_agencies_from_db_to_dataframe(path: str, table: str) -> pd.DataFrame:
    """load all agencies form the db in the path, in the table TO a df"""
    # Connect to the DuckDB database
    conn = duckdb.connect(path)

    # Execute the query and load the result directly into a pandas DataFrame
    query = f"SELECT * FROM {table}"
    df = conn.execute(query).fetchdf()

    # Close the connection
    conn.close()

    return df


if __name__ == "__main__":
    PROJECT_PATH = ""
    PATH_TO_DB = PROJECT_PATH + "api/data_app/db/agencies.duckdb"
    TABLE_NAME = "agencies"

    logging.info(create_agencies(PATH_TO_DB, TABLE_NAME))
