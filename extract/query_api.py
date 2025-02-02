"""
Module usable in CLI
to query the api directly on render.com here:
(example of request)
https://simulated-banking-agency-traffic-counters.onrender.com/get_visitor_count?
date_time=2025-05-29_09:00&agency_name=Lyon_1&counter_id=0
Loop on multiple dates and hours to create analytical reports

CLI usage :
python3 query_api.py
"""

# import calendar
import re
import string
import sys
from datetime import datetime

import duckdb
import numpy as np
import pandas as pd
import requests

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
def get_this_date_agency_counter_count(
    target_api, date_string, agency_nam, counter_id
) -> pd.DataFrame | None:
    """
    perform a single request using CLI parameters
    writes the line obtained in 'data.csv' to disk
    TESTING purpose
    """
    try:
        # request api and obtain JSON response
        json_response = target_api.request_api(date_string, agency_nam, counter_id)
        # Load JSON data into a pandas DataFrame
        return pd.DataFrame([json_response])
    except ValueError as e:
        print(e)
        # print(json_response)
        return None


def validate_cli_parameters(sys_argv, m=2, n=4):
    """Check parameters validity for CLI"""
    # check passed arguments
    if not m <= len(sys_argv) <= n:
        print("Usage: python3 query_api.py <date_string> <agency_name> <counter_id>")
        sys.exit(1)  # stops with an error code


def get_cli_parameters(sys_argv):
    """return the 3 parameters : date, agency_name, counter_id
    from CLI as a tuple"""

    date_string = sys_argv[1]

    # get the facultative argument agency_name
    if len(sys_argv) >= 3:
        agency_name_str = sys_argv[2]
    else:
        agency_name_str = None

    # get the facultative argument counter_id
    if len(sys_argv) == 4:
        counter_id_int = int(sys_argv[3])
    else:
        counter_id_int = -1
    return date_string, agency_name_str, counter_id_int


# ----------------------------------------------------------------------------------------------


def validate_date_format(date_string: str):
    """
    Validate that date_string corresponds to format 'YYYY-MM-DD HH:MM'.
    """
    print(f"Received date string: '{date_string}'")

    pattern = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$"
    if not re.match(pattern, date_string):
        raise ValueError(
            f"Invalid date format: {date_string}. Expected format: YYYY-mm-dd HH:MM"
        )
    # check that the date is valid
    try:
        datetime.strptime(date_string, "%Y-%m-%d %H:%M")
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
    date_string = row_df["date_time"]
    date_time = datetime.strptime(date_string, "%Y-%m-%d %H:%M")

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
        "date_time": f'{date_time.strftime("%Y-%m-%d_%H")}',  # remove useless minutes
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


# def get_df_for_all_agencies_in_date_range(
#         agencies_df: pd.DataFrame, dates:pd.date_range()
#         ) -> pd.DataFrame:
#
#     event_df = initiate_event_df()
#
#     # find all agency_names and their number of counter
#     for date in dates:
#         # put date_string to expected format in the API
#         date_string = date.strftime("%Y-%m-%d %H:%M")
#
#         for agency_name, counter_num in (agencies_df[["agency_name", "counter_number"]]
#                                             .values.tolist()):
#             for counter_id in range(counter_num):
#                 return get_single_date_response(event_df, agency_name, counter_id, date_string)


# def get_single_date_response(render_api,count_df, agency_nam, counter_id, date_string):
#     json_response, new_df = get_api_response(render_api,agency_nam, counter_id, date_string)
#     try:
#         count_df = pd.concat([count_df, new_df], ignore_index=True)
#         return event_df
#     except ValueError as v:
#         print(v)
#         print(json_response)


def initiate_event_df():
    """if df does not exist, create it empty"""
    return pd.DataFrame(
        columns=[
            "date_time",
            "agency_name",
            "counter_id",
            "visitor_count",
            "unit",
        ]
    )


def get_api_response(
    api, agency_nam: str, counter_id: int, date_string: str
) -> (dict | str, dict):
    """request api and get JSON response"""
    json_response = api.request_api(date_string, agency_nam, counter_id)
    new_line = add_random_error_to_row(json_response)
    new_line = pd.DataFrame([new_line])
    # Load cumulatively JSON data into a pandas DataFrame
    return json_response, new_line


# def get_date_range(year, month)->(pd.date_range(), str):
#     """
#     :param month: get the date range within a month
#     :param year: get the date range within a year
#     :return:
#     """
#     last_day = calendar.monthrange(year, month)[1]
#     start_day = f"{year}-{month:02}-01"
#     and_day = f"{year}-{month:02}-{last_day}"
#     date_range = pd.date_range(
#         start=f"{start_day} 00:00", end=f"{and_day} 23:00", freq="h"
#     )
#     date_range_string = f"{start_day}-{and_day}"
#     date_range_string = date_range_string.replace(" ", "_")
#     return date_range, date_range_string


def clean_date(date_string: str):
    """replace the semicolon by an underscore for the date_time in the filename"""
    return date_string.replace(":", "-")


if __name__ == "__main__":

    # local db settings
    PROJECT_PATH = "/home/michael/ProjetPerso/Banking_Agency_Traffic/"
    PATH_DB = PROJECT_PATH + "api/data_app/db/agencies.duckdb"
    TABLE = "agencies"

    # API settings
    # BASE_URL = "https://simulated-banking-agency-traffic-counters.onrender.com"
    BASE_URL = (
        "http://127.0.0.1:8000"  # needs API to be running (or use render directly)
    )
    GET_ROUTE = "/get_visitor_count"

    # Create the api object
    render_api = Api(BASE_URL, GET_ROUTE)

    # Request from CLI: 1 date_time, (+1 agency, (+1 counter_id)) (single row)
    if len(sys.argv) >= 2:
        # 1 date_time : requests all agencies sensors
        validate_cli_parameters(sys.argv)
        date_str, agency_name, id_counter = get_cli_parameters(sys.argv)
        validate_date_format(date_str)

        if len(sys.argv) == 2:
            # loop over all agencies and sensors
            # use local database to load agency_name and corresponding counter_num
            agency_df = load_agency_name_counter_num_from_db(PATH_DB, TABLE)

            # init event_df
            event_df = initiate_event_df()

            # loop over all agencies and sensors
            for agency_name, counter_num in agency_df[
                ["agency_name", "counter_number"]
            ].values.tolist():
                for id_counter in range(counter_num):
                    new_row = get_this_date_agency_counter_count(
                        render_api, date_str, agency_name, id_counter
                    )
                    try:
                        event_df = pd.concat([event_df, new_row], ignore_index=True)
                    except ValueError as v:
                        print(v)

            # Save event_df to CSV
            cleaned_date_string = clean_date(date_str)
            event_df.to_csv(
                PROJECT_PATH
                + "data/raw/cli/"
                + "event_df_all_agencies_"
                + cleaned_date_string
                + ".csv",
                index=False,
            )
        else:
            pass

    # else:
    #     # prepare the time_slice of the data
    #     YEAR = 2024
    #
    #     # use local database to load agency_name and corresponding counter_num
    #     agency_df = load_agency_name_counter_num_from_db(PATH_DB, TABLE)
    #
    #     for month_i in range(1, 12 + 1):
    #         date_range, date_range_str = get_date_range(YEAR, month_i)
    #
    #         # get the df for all agencies within the date_range
    #         event_df = get_df_for_all_agencies_in_date_range(agency_df, date_range)
    #
    #         # Save DataFrame to CSV
    #         event_df.to_csv(
    #             "data/raw/back_fill/events_all_agencies_counters_" + date_range_str + ".csv",
    #             index=False,
    #         )
