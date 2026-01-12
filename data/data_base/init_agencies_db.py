"""
module to initiate the database agencies
"""

import logging
from enum import Enum

import duckdb


# ----------------------------------------------------------------
#         Define sizes, location_type of agencies
#         and according average traffic TRAFFIC_DATA
# ----------------------------------------------------------------
class AgencySize(Enum):
    """enum class for the agency size"""

    SMALL = "small"
    MEDIUM = "medium"
    BIG = "big"


class LocationType(Enum):
    """enum class for the location type"""

    COUNTRYSIDE = "countryside"
    MID_SIZED_CITY = "mid_sized_city"
    METROPOLIS = "metropolis"


# Dictionnaire des moyennes de trafic
BASE_TRAFFIC_DATA = {
    (AgencySize.SMALL, LocationType.COUNTRYSIDE): 20,
    (AgencySize.SMALL, LocationType.MID_SIZED_CITY): 50,
    (AgencySize.SMALL, LocationType.METROPOLIS): 80,
    (AgencySize.MEDIUM, LocationType.COUNTRYSIDE): 40,
    (AgencySize.MEDIUM, LocationType.MID_SIZED_CITY): 100,
    (AgencySize.MEDIUM, LocationType.METROPOLIS): 200,
    (AgencySize.BIG, LocationType.COUNTRYSIDE): 100,
    (AgencySize.BIG, LocationType.MID_SIZED_CITY): 300,
    (AgencySize.BIG, LocationType.METROPOLIS): 500,
}


# function to get traffic_data
def get_base_traffic(size: AgencySize, location: LocationType) -> int:
    """function to get base traffic_data"""
    return BASE_TRAFFIC_DATA.get((size, location), "Data not available")


# Dictionnaire des moyennes de trafic
COUNTER_NUM_DATA = {
    (AgencySize.SMALL, LocationType.COUNTRYSIDE): 1,
    (AgencySize.SMALL, LocationType.MID_SIZED_CITY): 1,
    (AgencySize.SMALL, LocationType.METROPOLIS): 1,
    (AgencySize.MEDIUM, LocationType.COUNTRYSIDE): 2,
    (AgencySize.MEDIUM, LocationType.MID_SIZED_CITY): 2,
    (AgencySize.MEDIUM, LocationType.METROPOLIS): 2,
    (AgencySize.BIG, LocationType.COUNTRYSIDE): 3,
    (AgencySize.BIG, LocationType.MID_SIZED_CITY): 3,
    (AgencySize.BIG, LocationType.METROPOLIS): 3,
}


# function to get traffic_data
def get_num_counter(size: AgencySize, location: LocationType) -> int:
    """function to get traffic_data"""
    return COUNTER_NUM_DATA.get((size, location), "Data not available")


# ----------------------------------------------------------------
#         Define agency names 'City_#agency_in_the_city'
# ----------------------------------------------------------------
class AgencyName(Enum):
    """enum class with all the agencies' names"""

    LYON_1 = "Lyon_1"
    LYON_2 = "Lyon_2"
    LYON_3 = "Lyon_3"
    GRENOBLE_1 = "Grenoble_1"
    GRENOBLE_2 = "Grenoble_2"
    CHAMBERY_1 = "Chambery_1"
    CHAMBERY_2 = "Chambery_2"
    AIX_LES_BAINS_1 = "Aix_les_bains_1"
    LA_BIOLLE_1 = "La_Biolle_1"
    COGNIN_1 = "Cognin_1"
    LA_MOTTE_SERVOLEX_1 = "La_Motte_Servolex_1"


# ----------------------------------------------------------------
# Define a dictionary to associate each city with a size and location
# ----------------------------------------------------------------
AGENCY_DETAILS = {
    AgencyName.LYON_1: (AgencySize.BIG, LocationType.METROPOLIS),
    AgencyName.LYON_2: (AgencySize.MEDIUM, LocationType.METROPOLIS),
    AgencyName.LYON_3: (AgencySize.SMALL, LocationType.METROPOLIS),
    AgencyName.GRENOBLE_1: (AgencySize.BIG, LocationType.METROPOLIS),
    AgencyName.GRENOBLE_2: (AgencySize.MEDIUM, LocationType.METROPOLIS),
    AgencyName.CHAMBERY_1: (AgencySize.BIG, LocationType.MID_SIZED_CITY),
    AgencyName.CHAMBERY_2: (AgencySize.SMALL, LocationType.MID_SIZED_CITY),
    AgencyName.AIX_LES_BAINS_1: (AgencySize.SMALL, LocationType.MID_SIZED_CITY),
    AgencyName.LA_BIOLLE_1: (AgencySize.SMALL, LocationType.COUNTRYSIDE),
    AgencyName.COGNIN_1: (AgencySize.MEDIUM, LocationType.COUNTRYSIDE),
    AgencyName.LA_MOTTE_SERVOLEX_1: (AgencySize.MEDIUM, LocationType.COUNTRYSIDE),
}


# ----------------------------------------------------------------
# create a database to store the agencies characteristics
# ----------------------------------------------------------------
def create_agencies_db(path, table_name):
    """create a database to store the agencies characteristics"""
    conn = duckdb.connect(path)

    # Create the table if it doesn't exist
    logging.info("I will create %s here: %s", table_name, path)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            agency_name VARCHAR PRIMARY KEY,
            agency_size VARCHAR,
            location_type VARCHAR,
            base_traffic INTEGER,
            counter_number INTEGER
            );
        """
    )

    # Insert data into the table
    for agency_name, (agency_size, location_type) in AGENCY_DETAILS.items():
        base_traffic = get_base_traffic(agency_size, location_type)
        num_counter = get_num_counter(agency_size, location_type)
        conn.execute(
            """
    INSERT OR REPLACE INTO agencies
    (agency_name, agency_size, location_type, base_traffic, counter_number)
    VALUES (?, ?, ?, ?, ?)
    """,
            (
                agency_name.value,
                agency_size.value,
                location_type.value,
                base_traffic,
                num_counter,
            ),
        )
    # Commit changes (although DuckDB typically does this automatically)
    conn.commit()

    # Close the connection
    conn.close()


def read_agency_db(path, table_name):
    """
    for debug purpose only
    :param path:
    :param table_name:
    :return:
    """
    # Connect to the DuckDB database
    conn = duckdb.connect(path)

    # Fetch all agency data from the table
    result = conn.execute(
        f"""
        SELECT
            agency_name,
            agency_size,
            location_type,
            base_traffic,
            counter_number
        FROM {table_name}
    """
    ).fetchall()

    # Close the connection
    conn.close()

    # Return the result as a list of dictionaries for easier handling
    agencies = [
        {
            "agency_name": row[0],
            "agency_size": row[1],
            "location_type": row[2],
            "base_traffic": row[3],
            "counter_number": row[4],
        }
        for row in result
    ]

    return agencies


if __name__ == "__main__":
    logging.info("launching agencies.duckdb creation")
    PROJECT_PATH = ""
    DB_PATH = PROJECT_PATH + "data/data_base/agencies.duckdb"
    DB_TABLE = "agencies"
    create_agencies_db(DB_PATH, DB_TABLE)  # agency_details defined up here
    logging.info(read_agency_db(DB_PATH, DB_TABLE))
