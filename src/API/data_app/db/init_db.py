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
class CityNames(Enum):
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
city_details = {
    CityNames.LYON_1: (Size.SMALL, LocationType.COUNTRYSIDE),
    CityNames.LYON_2: (Size.MEDIUM, LocationType.MID_SIZED_CITY),
    CityNames.LYON_3: (Size.BIG, LocationType.METROPOLIS),
    CityNames.GRENOBLE_1: (Size.MEDIUM, LocationType.COUNTRYSIDE),
    CityNames.GRENOBLE_2: (Size.BIG, LocationType.MID_SIZED_CITY),
    CityNames.CHAMBERY_1: (Size.SMALL, LocationType.COUNTRYSIDE),
    CityNames.CHAMBERY_2: (Size.MEDIUM, LocationType.MID_SIZED_CITY),
    CityNames.AIX_LES_BAINS_1: (Size.SMALL, LocationType.COUNTRYSIDE),
    CityNames.LA_BIOLLE_1: (Size.SMALL, LocationType.COUNTRYSIDE),
    CityNames.COGNIN_1: (Size.SMALL, LocationType.MID_SIZED_CITY),
    CityNames.LA_MOTTE_SERVOLEX_1: (Size.SMALL, LocationType.MID_SIZED_CITY)
}
# ----------------------------------------------------------------
# create a database to store the agencies characteristics
# ----------------------------------------------------------------
# Connect to DuckDB (it creates the database file if it doesn't exist)
conn = duckdb.connect('AgencyDetails.duckdb')

# Create the table if it doesn't exist
conn.execute('''
CREATE TABLE IF NOT EXISTS AgencyDetails (
    CityName VARCHAR PRIMARY KEY,
    Size VARCHAR,
    LocationType VARCHAR,
    BaseTraffic INTEGER
    );
''')

# Insert data into the table
for city, (size, location) in city_details.items():
    base_traffic = get_base_traffic(size, location)
    conn.execute('''
    INSERT OR REPLACE INTO AgencyDetails (CityName, Size, LocationType, BaseTraffic)
    VALUES (?, ?, ?, ?)
    ''', (city.value, size.value, location.value, base_traffic))

# Commit changes (although DuckDB typically does this automatically)
conn.commit()

# Query the data to confirm it's inserted
result = conn.execute("SELECT * FROM AgencyDetails").fetchall()
for row in result:
    print(row)

# Close the connection
conn.close()
