"""
Module usable in CLI
to query the api directly on render.com here:
(example of request)
https://simulated-banking-agency-traffic-counters.onrender.com/get_visitor_count?
date_time=2025-05-29_09:00&agency_name=Lyon_1&counter_id=0
Loop on multiple dates and hours to create analytical reports

CLI usage :
python3 extract.py
"""

import calendar
import logging
import os

# import calendar
import re
import string
import subprocess
import sys
from datetime import datetime

import duckdb
import numpy as np
import pandas as pd
import requests
from date_randomseed.random_seed import RandomSeed

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
)
# ----------------------------------------------------------------------------------------------
#                       API class
# ----------------------------------------------------------------------------------------------


class Api:  # pylint: disable=R0903
    """
    api class used to request the api directly on render.com or localy
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
            # logging.info(f"requesting {url}")
            response = requests.get(url, timeout=5)

            # check if the request was successful
            if response.status_code == 200:
                return response.json()

            return response.text
        except requests.exceptions.RequestException as e:
            logging.error("An error occurred: %s", e)
            return str(e)


# ----------------------------------------------------------------------------------------------
#                       SINGLE DATE REQUEST FROM CLI PARAMETERS
# ----------------------------------------------------------------------------------------------
def get_this_date_agency_counter_count(
    target_api, date_tim, agency_nam, counter_id
) -> pd.DataFrame | None:
    """
    perform a single request to the API (one date_time, one agency, one counter)
    """
    try:
        date_str_web = date_tim.strftime("%Y-%m-%d %H:%M")
        # request api and obtain JSON response
        json_response = target_api.request_api(date_str_web, agency_nam, counter_id)
        # Load JSON data into a pandas DataFrame
        return pd.DataFrame([json_response])
    except ValueError as e:
        logging.error(e)
        # print(json_response)
        return None


def validate_cli_parameters(sys_argv, m=2, n=4):
    """Check parameters validity for CLI"""
    # check passed arguments
    if not m <= len(sys_argv) <= n:
        logging.warning(
            "Missing parameter. Usage: python3 extract.py <date_string> <agency_name> <counter_id>"
        )
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


def get_date_format(date_string: str) -> (datetime, str):
    """
    Detect if date_string corresponds to format :
    -'YYYY-MM-DD HH:MM'.
    -'YYYY-MM-DD'.
    -'YYYY-MM'.
    -raise error else
    :returns:date_clean, label ('hour', 'day', 'month')
    """
    logging.info("Received date string: '%s'", date_string)

    date_time_pattern = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$"
    date_pattern = r"^\d{4}-\d{2}-\d{2}$"
    month_pattern = r"^\d{4}-\d{2}$"
    year_pattern = r"^\d{4}$"

    if re.match(date_time_pattern, date_string):
        # check that the date is valid
        try:
            date_clean = datetime.strptime(date_string, "%Y-%m-%d %H:%M")
            return date_clean, "hour"
        except ValueError as e:
            raise ValueError(f"Invalid date content: {date_string}. {e}") from e

    elif re.match(date_pattern, date_string):
        # check that the date is valid
        try:
            date_clean = datetime.strptime(date_string, "%Y-%m-%d").date()
            return date_clean, "day"

        except ValueError as e:
            raise ValueError(f"Invalid date content: {date_string}. {e}") from e

    elif re.match(month_pattern, date_string):
        # check that the date is valid
        try:
            date_clean = datetime.strptime(date_string, "%Y-%m")
            return date_clean, "month"
        except ValueError as e:
            raise ValueError(f"Invalid date content: {date_string}. {e}") from e

    elif re.match(year_pattern, date_string):
        # check that the date is valid
        try:
            date_clean = datetime.strptime(date_string, "%Y")
            return date_clean, "year"
        except ValueError as e:
            raise ValueError(f"Invalid date content: {date_string}. {e}") from e

    else:
        raise ValueError(
            f"Invalid date format: {date_string}. Expected formats: /n"
            f"YYYY-mm-dd HH:MM /n"
            f"YYYY-mm-dd /n"
            f"YYYY-mm"
        )


def load_agency_name_counter_num_from_db(path, table_name):
    """Load from agencies table in db : (agency_name, counter_number)"""
    # load AgenciesDetails.duckdb database and look in AgenciesDetails table
    # Check if the database exists
    if not os.path.exists(path):
        # Si elle n'existe pas, initialiser la base de données
        # If connection fails, initialize the database by running the init script
        logging.warning("Database not found at %s. Initializing database.", path)
        subprocess.run(["python", "data/data_base/init_agencies_db.py"], check=True)

    conn = duckdb.connect(path)

    data_f = conn.execute(
        f"""
        SELECT agency_name, counter_number
        FROM {table_name}
        ORDER BY agency_name, counter_number
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

    # use another seed for error
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
) -> str | int | float | None:
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
) -> tuple[dict | str, dict]:
    """request api and get JSON response"""
    json_response = api.request_api(date_string, agency_nam, counter_id)
    new_line = add_random_error_to_row(json_response)
    new_line = pd.DataFrame([new_line])
    # Load cumulatively JSON data into a pandas DataFrame
    return json_response, new_line


def get_date_range(date_tim, date_kind) -> tuple[pd.DatetimeIndex, str]:
    """
    :param date_tim: get the date range within a month
    :param date_kind: kind of date we have 'hour', 'day', 'month'
    :return: date_period, date_range_string
    """
    logging.info("Received date_time %s", date_tim)
    logging.info("Received date_kind %s", date_kind)

    if date_kind == "hour":
        date_period = pd.date_range(start=date_tim, periods=1, freq="h")
        date_range_string = f"{date_tim}"
        date_range_string = date_range_string.replace(" ", "_")

    elif date_kind == "day":
        date_period = pd.date_range(
            start=f"{date_tim} 00:00", end=f"{date_tim} 23:00", freq="h"
        )
        date_range_string = f"{date_tim}"
        date_range_string = date_range_string.replace(" ", "_")

    elif date_kind == "month":
        last_day = calendar.monthrange(date_tim.year, date_tim.month)[1]
        start_day = f"{date_tim.year}-{date_tim.month:02}-01"
        end_day = f"{date_tim.year}-{date_tim.month:02}-{last_day}"

        date_period = pd.date_range(
            start=f"{start_day} 00:00", end=f"{end_day} 23:00", freq="h"
        )
        # date_range_string = f"{start_day}-{end_day}" #another way to name the file
        date_range_string = date_tim.strftime("%Y-%m")
        date_range_string = date_range_string.replace(" ", "_")

    elif date_kind == "year":
        start_day = f"{date_tim.year}-01-01"
        end_day = f"{date_tim.year}-12-31"

        date_period = pd.date_range(
            start=f"{start_day} 00:00", end=f"{end_day} 23:00", freq="h"
        )
        # date_range_string = f"{start_day}-{end_day}" #another way to name the file
        date_range_string = date_tim.strftime("%Y")
        # date_range_string = date_range_string.replace(" ", "_")

    else:
        raise ValueError(
            f"Invalid date kind: {date_kind}. Expected 'hour', 'day' or 'month'"
        )

    return date_period, date_range_string


def clean_date(date_string: str):
    """replace the semicolon by an underscore for the date_time in the filename"""
    return date_string.replace(":", "-")


if __name__ == "__main__":
    logging.info("Script started")
    # logging.warning("This is a warning")
    # logging.error("An error occurred")

    # local db settings
    PROJECT_PATH = ""
    PATH_DB = PROJECT_PATH + "data/data_base/agencies.duckdb"
    TABLE = "agencies"

    # API settings
    # BASE_URL = "https://simulated-banking-agency-traffic-counters.onrender.com"
    BASE_URL = (
        "http://127.0.0.1:8000"  # needs API to be running (or use render directly)
    )
    GET_ROUTE = "/get_visitor_count"

    # Create the api object
    render_api = Api(BASE_URL, GET_ROUTE)

    # minimum : 1 date_time
    validate_cli_parameters(sys.argv)

    # Request from CLI: 1 date_time, (+1 agency, (+1 counter_id)) (single row)
    date_str, agency_name, id_counter = get_cli_parameters(sys.argv)
    date_str, date_type = get_date_format(date_str)  #'hour', 'day', 'month' or 'year'

    date_range, date_range_str = get_date_range(date_str, date_type)

    if agency_name is None:
        # loop over all agencies and sensors
        # use local database to load agency_name and corresponding counter_num
        agency_df = load_agency_name_counter_num_from_db(PATH_DB, TABLE)
        agency_counter_num_list = agency_df[
            ["agency_name", "counter_number"]
        ].values.tolist()

        event_df = initiate_event_df()

        # loop over all agencies and sensors
        for date_i in date_range:
            if date_i.hour == 0:
                logging.info("Treating date: %s", date_i.date())
            for agency_name, counter_num in agency_counter_num_list:
                for id_counter in range(counter_num):
                    new_row = get_this_date_agency_counter_count(
                        render_api, date_i, agency_name, id_counter
                    )
                    try:
                        event_df = pd.concat([event_df, new_row], ignore_index=True)
                    except ValueError as v:
                        logging.error(v)

        # Save event_df to CSV
        CLEANED_DATE_STRING = clean_date(date_range_str)
        FILE_NAME = f"event_df_all_agencies_{CLEANED_DATE_STRING}.csv"
        PATH_NAME = f"{PROJECT_PATH}data/raw/"
        logging.info("DataFrame created -> saving %s in %s", FILE_NAME, PATH_NAME)
        event_df.to_csv(
            PATH_NAME + FILE_NAME,
            index=False,
        )
        logging.info("File successfully saved")

    else:  # we have a specific agency_name and maybe a specific counter name
        event_df = initiate_event_df()

        for date_i in date_range:
            new_row = get_this_date_agency_counter_count(
                render_api, date_i, agency_name, id_counter
            )
            try:
                event_df = pd.concat([event_df, new_row], ignore_index=True)
            except ValueError as v:
                logging.error(v)

        CLEANED_DATE_STRING = clean_date(date_range_str)
        AGENCY_STR = agency_name.replace(" ", "_")
        COUNTER_STR = f"_counter_{id_counter}" if id_counter >= 0 else "_all_counters"

        FILE_NAME = f"event_df_{AGENCY_STR}{COUNTER_STR}_{CLEANED_DATE_STRING}.csv"
        PATH_NAME = f"{PROJECT_PATH}data/raw/"
        logging.info("DataFrame created -> saving %s in %s", FILE_NAME, PATH_NAME)
        event_df.to_csv(PATH_NAME + FILE_NAME, index=False)
        logging.info("File successfully saved")
