"""
Module to test module agency.py
- object Agency
    - method create_counters
    - method get_counter_traffic
    - method get_all_counter_traffic
    - method create_fractional_range (secondary)
    - similar agencies don't have the same count on same date_time and counter_id
"""

import unittest
from datetime import datetime

from API.data_app.agency import Agency


class TestAgency(unittest.TestCase):
    """
    tests:
    ------
    -sum (get_counter_traffic by counter) = get_all_counter_traffic
    """

    def setUp(self):
        """
        executed before every test : create Lyon_1 agency
        :return:
        """
        self.Big_agency_1 = Agency('Lyon_1','big', 'Metropolis',500,3)
        self.Big_agency_2 = Agency('Grenoble_1','big', 'Metropolis',500,3)
        self.Small_agency_1 = Agency('Aix_les_bains_1','small', 'Mid_sized_city',50,1)

    def test_weekdays_opened(self):
        """
            Test worked days at 9:00 : not -1 (opened)
            for all counter_id
            for Lyon_1 agency
            week : monday 11/9/2023 to friday 15/9/2023 (16 excluded)
            HOUR : 9 AM
        :return:
        """
        for test_day in range(11, 16):
            with self.subTest(i=test_day):
                for counter_id in range(self.Big_agency_1.counter_num):
                    visit_count = self.Big_agency_1.get_counter_traffic(
                        datetime(2023, 9, test_day, 9),
                        counter_id
                    )
                    self.assertFalse(visit_count == -1)

