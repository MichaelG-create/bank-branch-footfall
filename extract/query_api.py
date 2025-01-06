"""
Module usable in CLI
to query the api directly on render.com here:
(example of request)
https://simulated-banking-agency-traffic-counters.onrender.com/get_visitor_count?
date_time=2025-05-29_09:00&agency_name=Lyon_1&counter_id=0
Loop on multiple dates and hours to create analytical reports
"""

import calendar
import re
import string
import sys
from datetime import datetime

import duckdb
import requests
import numpy as np
import pandas as pd

from api.data_app.random_seed import RandomSeed

# ----------------------------------------------------------------------------------------------
#                       API class
# ----------------------------------------------------------------------------------------------


class Api:  # pylint: disable=R0903
    """
    api class used to request the api directly on render.com
    methods : request_api
    (sends GET request and get JSON response back)
    """

    def __init__(self, base_url: str, get_route: str):
        self.base_url = base_url
        self.get_route = get_route

    def request_api(
        self, date_string: str, name_of_agency: str, id_of_counter: int = -1
    ) -> dict | str:
        """
        Sends a GET request to the api and print the response
        :return: json response | str error text
        """
        try:
            url = (
                f"{self.base_url}{self.get_route}?"
                f"date_time={date_string}&"
                f"agency_name={name_of_agency}&"
                f"counter_id={id_of_counter}"
            )
            # print(f"requesting {url}")
            response = requests.get(url, timeout=5)

            # check if the request was successful
            if response.status_code == 200:
                return response.json()

            return response.text
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return str(e)


# ----------------------------------------------------------------------------------------------
#                       SINGLE DATE REQUEST FROM CLI PARAMETERS
# ----------------------------------------------------------------------------------------------
def perform_single_date_request(target_api):
    """
    perform a single request using CLI parameters
    writes the line obtained in 'data.csv' to disk
    TESTING purpose
    """
    validate_cli_parameters()
    date_string, agency_nam, id_counter = get_cli_parameters()
    validate_date_format(date_string)
    # request api and obtain JSON response
    json_respons = target_api.request_api(date_string, agency_nam, id_counter)
    # Load JSON data into a pandas DataFrame

    try:
        data_f = pd.DataFrame([json_respons])
        # Save DataFrame to CSV
        data_f.to_csv("data.csv", index=False)

    except ValueError as e:
        print(e)
        print(json_respons)


def validate_cli_parameters():
    """Check parameters validity for CLI"""
    # check passed arguments
    if not 3 <= len(sys.argv) <= 4:
        print("Usage: python3 query_api.py <date_string> <agency_name> <counter_id>")
        sys.exit(1)  # stops with an error code


def get_cli_parameters():
    """return the 3 parameters from CLI as a tuple"""
    date_string = sys.argv[1]
    agency_name_string = sys.argv[2]
    # get the facultative argument counter_id
    if len(sys.argv) == 4:
        counter_id_int = int(sys.argv[3])
    else:
        counter_id_int = -1
    return date_string, agency_name_string, counter_id_int


# ----------------------------------------------------------------------------------------------


def validate_date_format(date_string: str):
    """
    Validate that date_string corresponds to format 'YYYY-MM-DD_HH:MM'.
    """
    pattern = r"^\d{4}-\d{2}-\d{2}_\d{2}:\d{2}$"
    if not re.match(pattern, date_string):
        raise ValueError(
            f"Invalid date format: {date_string}. Expected format: YYYY-MM-DD_HH:MM"
        )
    # check that the date is valid
    try:
        datetime.strptime(date_string, "%Y-%m-%d_%H:%M")
    except ValueError as e:
        raise ValueError(f"Invalid date content: {date_string}. {e}") from e


def load_agency_name_counter_num_from_db(path, table_name):
    """Load from agencies table in db : (agency_name, counter_number)"""
    # load AgenciesDetails.duckdb database and look in AgenciesDetails table
    conn = duckdb.connect(path)
    data_f = conn.execute(
        f"""
        SELECT agency_name, counter_number 
        FROM {table_name}
    """
    ).fetchdf()

    conn.close()

    return data_f


def add_random_error_to_row(row_df: dict) -> dict:
    """Randomly add bad fields or value in the row field :
    - agency_name: Null or value not in the agencies db : 'Paris', 'New York',
    or badly written 'Cognen_1', 'Chambéry_1', 'Aix-les-bains_1', 'La motte servolex 1'
    - counter_id = -1 or Null or *100
    - visitor_count = visitor_count*10
    - unit = 'L', 'kg', Null
    uses random seed based on random in api/data_app/random_seed
    :Returns:  the possibly modified dataframe row
    """
    # Prepare the randomness
    # Convert to datetime object for the seed
    # print(row_df)
    date_string = row_df["date_time"]
    date_time = datetime.strptime(date_string, "%Y-%m-%d_%H:%M")

    random_seed = RandomSeed(date_time)  # default: takes only date from the date_time
    time = date_time.strftime("%H%M")

    seed = random_seed.generate_seed(
        time,
        row_df["agency_name"],
        row_df["counter_id"],
        row_df["visitor_count"],
        row_df["unit"],
    )

    np.random.seed(seed)
    pct_error = 0.01
    return {
        "date_time": f'{date_time.strftime("%Y-%m-%d_%H")}',  ## to remove useless minutes and seconds
        "agency_name": add_error_or_keep(row_df["agency_name"], pct_error),
        "counter_id": add_error_or_keep(row_df["counter_id"], pct_error),
        "visitor_count": add_error_or_keep(row_df["visitor_count"], pct_error),
        "unit": add_error_or_keep(row_df["unit"], pct_error),
    }


def add_error_or_keep(
    data: str | int | float, pct_err: float = 0.01
) -> str | int | None:
    """randomly add an error to the data given
    if the random number is under err_pct (typically 5%)"""

    rd_num = np.random.random()
    rd_num2 = np.random.random()
    if rd_num < pct_err:
        if isinstance(data, str):
            if rd_num2 < 0.3:
                return None
            if rd_num2 < 0.6:
                return data.replace("_", " ")
            return replace_random_letter(data)
        if isinstance(data, (int, float)):
            if rd_num2 < 0.3:
                return None
            if rd_num2 < 0.6:
                return 100 * (data + 2)
    return data


def replace_random_letter(s):
    """randomly replaces a letter by another one"""
    if not s:  # Check if the string is empty
        return s
    # Pick a random index in the string
    random_index = np.random.randint(0, len(s) - 1)
    # Pick a random letter (lowercase or uppercase)

    new_letter = np.random.choice(list(string.ascii_letters))
    # Replace the letter at the chosen index
    new_string = s[:random_index] + new_letter + s[random_index + 1 :]

    return new_string


if __name__ == "__main__":
    # local db settings
    PATH_DB = "/home/michael/ProjetPerso/Banking_Agency_Traffic/api/data_app/db/agencies.duckdb"
    TABLE = "agencies"

    # API settings
    # BASE_URL = "https://simulated-banking-agency-traffic-counters.onrender.com"
    BASE_URL = "http://127.0.0.1:8000"
    GET_ROUTE = "/get_visitor_count"

    # Create the api object
    render_api = Api(BASE_URL, GET_ROUTE)

    # Doing request from CLI, one at a time (1 date_time, 1 agency, 1 counter_id) (single row)
    if len(sys.argv) >= 2:
        perform_single_date_request(render_api)

    else:
        # prepare the time_slice of the data
        YEAR = 2024
        MONTH = 1

        for month_i in range(2,12+1):
            MONTH = month_i
            LAST_DAY_OF_THE_MONTH = calendar.monthrange(YEAR, MONTH)[1]

            START_DATE = f"{YEAR}-{MONTH:02}-01"
            END_DATE =   f"{YEAR}-{MONTH:02}-{LAST_DAY_OF_THE_MONTH}"

            DATE_RANGE = pd.date_range(start=f'{START_DATE} 00:00',
                                       end=  f'{END_DATE} 23:00',
                                       freq="h")

            DATE_RANGE_STR = f"{START_DATE}-{END_DATE}"
            DATE_RANGE_STR = DATE_RANGE_STR.replace(' ','_')

            # use local database to load agency_name and corresponding counter_num
            agency_df = load_agency_name_counter_num_from_db(PATH_DB, TABLE)

            # initiate the df
            event_df = pd.DataFrame(
                columns=[
                    "date_time",
                    "agency_name",
                    "counter_id",
                    "visitor_count",
                    "unit",
                ]
            )
            # find all agency_names and their number of counter
            # loop on it in the API
            for date_str in DATE_RANGE:
                # put date_str to expected format in the API
                date_str = date_str.strftime("%Y-%m-%d_%H:%M")

                for agency_name, counter_num in agency_df[
                    ["agency_name", "counter_number"]
                ].values.tolist():

                    for counter_id in range(counter_num):

                        # request api and get JSON response
                        json_response = render_api.request_api(
                            date_str, agency_name, counter_id
                        )
                        new_row = add_random_error_to_row(json_response)
                        new_row = pd.DataFrame([new_row])
                        # Load cumulatively JSON data into a pandas DataFrame
                        try:
                            event_df = pd.concat([event_df, new_row], ignore_index=True)
                        except ValueError as v:
                            print(v)
                            print(json_response)

            # Save DataFrame to CSV
            event_df.to_csv(
                "data/raw/events_all_agencies_counters_" + DATE_RANGE_STR + ".csv",
                index=False,
            )
