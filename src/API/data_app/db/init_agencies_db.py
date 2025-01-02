"""
module to initiate the database agencies
"""

from enum import Enum

import duckdb

# ----------------------------------------------------------------
#         Define sizes, location_type of agencies
#         and according average traffic TRAFFIC_DATA
# ----------------------------------------------------------------
class Size(Enum):
    SMALL = "small"
    MEDIUM = "medium"
    BIG = "big"

class LocationType(Enum):
    COUNTRYSIDE = "Countryside"
    MID_SIZED_CITY = "Mid_sized_city"
    METROPOLIS = "Metropolis"

# Dictionnaire des moyennes de trafic
BASE_TRAFFIC_DATA = {
    (Size.SMALL, LocationType.COUNTRYSIDE): 20,
    (Size.SMALL, LocationType.MID_SIZED_CITY): 50,
    (Size.SMALL, LocationType.METROPOLIS): 80,
    (Size.MEDIUM, LocationType.COUNTRYSIDE): 40,
    (Size.MEDIUM, LocationType.MID_SIZED_CITY): 100,
    (Size.MEDIUM, LocationType.METROPOLIS): 200,
    (Size.BIG, LocationType.COUNTRYSIDE): 100,
    (Size.BIG, LocationType.MID_SIZED_CITY): 300,
    (Size.BIG, LocationType.METROPOLIS): 500,
}

# function to get traffic_data
def get_base_traffic(size: Size, location: LocationType) -> int:
    return BASE_TRAFFIC_DATA.get((size, location), "Data not available")

# Dictionnaire des moyennes de trafic
NUM_COUNTER_DATA = {
    (Size.SMALL, LocationType.COUNTRYSIDE): 1,
    (Size.SMALL, LocationType.MID_SIZED_CITY): 1,
    (Size.SMALL, LocationType.METROPOLIS): 1,
    (Size.MEDIUM, LocationType.COUNTRYSIDE): 2,
    (Size.MEDIUM, LocationType.MID_SIZED_CITY): 2,
    (Size.MEDIUM, LocationType.METROPOLIS): 2,
    (Size.BIG, LocationType.COUNTRYSIDE): 3,
    (Size.BIG, LocationType.MID_SIZED_CITY): 3,
    (Size.BIG, LocationType.METROPOLIS): 3,
}

# function to get traffic_data
def get_num_counter(size: Size, location: LocationType) -> int:
    return NUM_COUNTER_DATA.get((size, location), "Data not available")


# ----------------------------------------------------------------
#         Define agency names 'City_#agency_in_the_city'
# ----------------------------------------------------------------
class AgencyNames(Enum):
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
agency_details = {
    AgencyNames.LYON_1: (Size.BIG, LocationType.METROPOLIS),
    AgencyNames.LYON_2: (Size.MEDIUM, LocationType.METROPOLIS),
    AgencyNames.LYON_3: (Size.SMALL, LocationType.METROPOLIS),
    AgencyNames.GRENOBLE_1: (Size.BIG, LocationType.METROPOLIS),
    AgencyNames.GRENOBLE_2: (Size.MEDIUM, LocationType.METROPOLIS),
    AgencyNames.CHAMBERY_1: (Size.BIG, LocationType.MID_SIZED_CITY),
    AgencyNames.CHAMBERY_2: (Size.SMALL, LocationType.MID_SIZED_CITY),
    AgencyNames.AIX_LES_BAINS_1: (Size.SMALL, LocationType.MID_SIZED_CITY),
    AgencyNames.LA_BIOLLE_1: (Size.SMALL, LocationType.COUNTRYSIDE),
    AgencyNames.COGNIN_1: (Size.MEDIUM, LocationType.COUNTRYSIDE),
    AgencyNames.LA_MOTTE_SERVOLEX_1: (Size.MEDIUM, LocationType.COUNTRYSIDE)
}
# ----------------------------------------------------------------
# create a database to store the agencies characteristics
# ----------------------------------------------------------------
def create_agencies_db(path, table_name):
    conn = duckdb.connect(path)

    # Create the table if it doesn't exist
    conn.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            AgencyName VARCHAR PRIMARY KEY,
            Size VARCHAR,
            LocationType VARCHAR,
            BaseTraffic INTEGER,
            NumCounter INTEGER
            );
        ''')

    # Insert data into the table
    for agency_name, (size, location) in agency_details.items():
        base_traffic = get_base_traffic(size, location)
        num_counter = get_num_counter(size, location)
        conn.execute('''
    INSERT OR REPLACE INTO AgencyDetails (AgencyName, Size, LocationType, BaseTraffic, NumCounter)
    VALUES (?, ?, ?, ?, ?)
    ''', (agency_name.value, size.value, location.value, base_traffic, num_counter))
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
    result = conn.execute(f'''
        SELECT AgencyName, Size, LocationType, BaseTraffic, NumCounter
        FROM {table_name}
    ''').fetchall()

    # Close the connection
    conn.close()

    # Return the result as a list of dictionaries for easier handling
    agencies = [
        {
            'AgencyName': row[0],
            'Size': row[1],
            'LocationType': row[2],
            'BaseTraffic': row[3],
            'NumCounter': row[4]
        }
        for row in result
    ]

    return agencies


if __name__ == '__main__':
    db_path = 'AgencyDetails.duckdb'
    db_table = 'AgencyDetails'
    # create_agencies_db(db_path, db_table)
    print(read_agency_db(db_path, db_table))
