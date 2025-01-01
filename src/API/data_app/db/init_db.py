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
    AgencyNames.LYON_1: (Size.SMALL, LocationType.COUNTRYSIDE),
    AgencyNames.LYON_2: (Size.MEDIUM, LocationType.MID_SIZED_CITY),
    AgencyNames.LYON_3: (Size.BIG, LocationType.METROPOLIS),
    AgencyNames.GRENOBLE_1: (Size.MEDIUM, LocationType.COUNTRYSIDE),
    AgencyNames.GRENOBLE_2: (Size.BIG, LocationType.MID_SIZED_CITY),
    AgencyNames.CHAMBERY_1: (Size.SMALL, LocationType.COUNTRYSIDE),
    AgencyNames.CHAMBERY_2: (Size.MEDIUM, LocationType.MID_SIZED_CITY),
    AgencyNames.AIX_LES_BAINS_1: (Size.SMALL, LocationType.COUNTRYSIDE),
    AgencyNames.LA_BIOLLE_1: (Size.SMALL, LocationType.COUNTRYSIDE),
    AgencyNames.COGNIN_1: (Size.SMALL, LocationType.MID_SIZED_CITY),
    AgencyNames.LA_MOTTE_SERVOLEX_1: (Size.SMALL, LocationType.MID_SIZED_CITY)
}
# ----------------------------------------------------------------
# create a database to store the agencies characteristics
# ----------------------------------------------------------------
# Connect to DuckDB (it creates the database file if it doesn't exist)
conn = duckdb.connect('AgencyDetails.duckdb')

# Create the table if it doesn't exist
conn.execute('''
CREATE TABLE IF NOT EXISTS AgencyDetails (
    AgencyName VARCHAR PRIMARY KEY,
    Size VARCHAR,
    LocationType VARCHAR,
    BaseTraffic INTEGER
    );
''')

# Insert data into the table
for agency_name, (size, location) in agency_details.items():
    base_traffic = get_base_traffic(size, location)
    conn.execute('''
    INSERT OR REPLACE INTO AgencyDetails (AgencyName, Size, LocationType, BaseTraffic)
    VALUES (?, ?, ?, ?)
    ''', (agency_name.value, size.value, location.value, base_traffic))

# Commit changes (although DuckDB typically does this automatically)
conn.commit()

# Query the data to confirm it's inserted
result = conn.execute("SELECT * FROM AgencyDetails").fetchall()
column_names = [desc[0] for desc in conn.description]
print(column_names)
for row in result:
    print(row)

# Close the connection
conn.close()
