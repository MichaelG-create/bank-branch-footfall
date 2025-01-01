"""
agencies app maker
"""
import duckdb
import pandas as pd

from src.API.data_app.agency import Agency

def create_agencies() -> dict:
    """
    Create the available agencies in our API
    from a database containing the agencies
    Each agency is stored in a dictionary
    As well as different break and malfunction percentages
    (Not realistic, but we keep things simple)
    :return: dict (Agencies) indexed by their 'name'
    """
    # read everything from the table
    agencies_df = load_agencies_from_db_to_dataframe()

    # Create an empty dictionary to store the agencies
    data_dict = {}

    # Iterate through the DataFrame row by row
    for _, row in agencies_df.iterrows():
        # Create an Agency object for each row
        agency_row = Agency(row['AgencyName'], row['Size'], row['LocationType'], row['BaseTraffic'])

        # Add the agency to the dictionary, using the AgencyName as the key
        data_dict[row['AgencyName']] = agency_row

    return data_dict

def load_agencies_from_db_to_dataframe():
    # Connect to the DuckDB database (or in-memory database)
    conn = duckdb.connect('db/AgencyDetails.duckdb')

    # Execute the query and load the result directly into a pandas DataFrame
    query = 'SELECT * FROM AgencyDetails'
    df = conn.execute(query).fetchdf()

    # Close the connection
    conn.close()

    return df

if __name__ == '__main__':
    print(create_agencies())