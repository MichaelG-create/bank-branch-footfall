"""
Module usable in CLI
to query the API directly on render.com here:
(example of request)
https://du-end2end-project.onrender.com/get_visitor_count?date_time=2025-05-29_09:00
Loop on multiple dates and hours to create analytical reports
"""

import re
import sys
from datetime import datetime

import requests


class RenderAPI:  # pylint: disable=R0903
    """
    API class used to request the API directly on render.com
    methods : request_api (sends GET request and get JSON response back)
    """

    def __init__(self, base_url: str, get_route: str):
        self.base_url = base_url
        self.get_route = get_route

    def request_api(self, date_string: str):
        """
        Sends a GET request to the API and print the response
        :return: None
        """
        try:
            url = f"{self.base_url}{self.get_route}?date_time={date_string}"
            response = requests.get(url, timeout=5)

            # check if the request was successful
            if response.status_code == 200:
                print(response.json())
            else:
                print(response.text)
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

    # Vérifie que la date est valide
    try:
        datetime.strptime(date_string, "%Y-%m-%d_%H:%M")
    except ValueError as e:
        raise ValueError(f"Invalid date content: {date_string}. {e}") from e


if __name__ == "__main__":
    # check passed arguments
    if len(sys.argv) != 2:
        print("Usage: python query_api.py <date_string>")
        sys.exit(1)  # stops with an error code

    # get the argument
    date_str = sys.argv[1]

    # validates the format
    validate_date_format(date_str)

    # API settings
    BASE_URL = "https://du-end2end-project.onrender.com"
    GET_ROUTE = "/get_visitor_count"

    # create the API object
    renderAPI = RenderAPI(BASE_URL, GET_ROUTE)

    # request API and print JSON response
    renderAPI.request_api(date_str)
