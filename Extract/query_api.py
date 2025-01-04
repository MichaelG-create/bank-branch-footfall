"""
Module usable in CLI
to query the api directly on render.com here:
(example of request)
https://simulated-banking-agency-traffic-counters.onrender.com/get_visitor_count?
date_time=2025-05-29_09:00&agency_name=Lyon_1&counter_id=0
Loop on multiple dates and hours to create analytical reports
"""

import re
import sys
from datetime import datetime

import duckdb
import requests

import pandas as pd


class Api:
    """
    api class used to request the api directly on render.com
    methods : request_api
    (sends GET request and get JSON response back)
    """

    def __init__(self, base_url: str, get_route: str):
        self.base_url = base_url
        self.get_route = get_route

    def request_api(
        self, date_string: str, agency_name: str, counter_id: int = -1
    ) -> dict | str:
        """
        Sends a GET request to the api and print the response
        :return: json response | str error text
        """
        try:
            url = (
                f"{self.base_url}{self.get_route}?"
                f"date_time={date_string}&"
                f"agency_name={agency_name}&"
                f"counter_id={counter_id}"
            )
            print(f"requesting {url}")
            response = requests.get(url, timeout=5)

            # print(f'dir(response): {dir(response)}')

            # check if the request was successful
            if response.status_code == 200:
                return response.json()
            else:
                return response.text
        except requests.exceptions.RequestException as e:
            print(f"An errror occured: {e}")


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


def validate_cli_parameters():
    """Check parameters validity for CLI"""
    # check passed arguments
    if not (3 <= len(sys.argv) <= 4):
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


def load_agency_name_counter_num_from_db():
    # load AgenciesDetails.duckdb database and look in AgenciesDetails table
    conn = duckdb.connect("../api/data_app/db/AgencyDetails.duckdb")
    data_f = conn.execute(
        """
        SELECT agency_name, NumCounter 
        FROM AgencyDetails
    """
    ).fetchdf()

    conn.close()

    return data_f


if __name__ == "__main__":
    # api settings
    BASE_URL = "https://simulated-banking-agency-traffic-counters.onrender.com"
    GET_ROUTE = "/get_visitor_count"

    # create the api object
    renderAPI = Api(BASE_URL, GET_ROUTE)

    # doing request one at a time 1 date_time, 1 agency, 1 counter_id (single row)
    if len(sys.argv) >= 2:
        validate_cli_parameters()
        date_str, agency_name, counter_id = get_cli_parameters()
        validate_date_format(date_str)

        # request api and obtain JSON response
        json_response = renderAPI.request_api(date_str, agency_name, counter_id)

        # Load JSON data into a pandas DataFrame
        try:
            df = pd.DataFrame([json_response])
            print(df)
            # Save DataFrame to CSV
            df.to_csv("data.csv", index=False)
        except ValueError as e:
            print(e)
            print(json_response)

    else:
        # use local database to load agency_name and corresponding counter_num
        df = load_agency_name_counter_num_from_db()

        # prepare the time_slice of the data
        start_date = "2024-12-02 08:00"
        end_date   = "2024-12-02 09:00"

        date_range = pd.date_range(start= start_date,
                                   end= end_date,
                                   freq= "H")

        date_range_str = start_date+"-"+end_date

        # find all agency_names and counter_num they have
        # loop on it in the API
        for agency_name, counter_num in df[["agency_name", "NumCounter"]].values.tolist():
            for counter_id in range(counter_num):
                for date_str in date_range:
                    # put date_str to expected format in the API
                    date_str = date_str.strftime("%Y-%m-%d_%H:%M")

                    # request api and obtain JSON response
                    json_response = renderAPI.request_api(
                        date_str, agency_name, counter_id
                    )

                    # Load JSON data into a pandas DataFrame
                    try:
                        df = pd.DataFrame([json_response])
                        print(df)
                        # Save DataFrame to CSV
                        df.to_csv("data_"+date_range_str+".csv", index=False)
                    except ValueError as e:
                        print(e)
                        print(json_response)
