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

    def test_hours_opened(self):
        """
            Test a worked day for hours in [9:12, 13:17] : not -1 (opened)
            weekday : wednesday 11/12/2024
            hours : 9 to 17 (without 12)
        :return:
        """
        for test_hour in [9, 10, 11, 13, 14, 15, 16, 17]:
            with self.subTest(i=test_hour):
                for counter_id in range(self.Big_agency_1.counter_num):
                    visit_count = self.Big_agency_1.get_counter_traffic(
                        datetime(2024, 12, 11, test_hour),
                        counter_id
                    )
                    self.assertFalse(visit_count == -1)

    def test_weekend_closed(self):
        """
            Test a saturday day for different hours : not -1 (opened)
            weekend : saturday 21/12/2024 and sunday 10/11/2024
            hours : 9 to 17 (without 12)
        :return:
        """
        # sunday test
        for test_hour in [0, 9, 12, 15, 22]:
            with self.subTest(i=test_hour):
                for counter_id in range(self.Big_agency_1.counter_num):
                    visit_count = self.Big_agency_1.get_counter_traffic(
                        datetime(2024, 12, 21, test_hour),
                        counter_id
                    )
                    self.assertTrue(visit_count == -1)

        # saturday test
        for test_hour in [0, 9, 12, 15, 22]:
            with self.subTest(i=test_hour):
                for counter_id in range(self.Big_agency_1.counter_num):
                    visit_count = self.Big_agency_1.get_counter_traffic(
                        datetime(2024, 11, 10, test_hour),
                        counter_id
                    )
                    self.assertTrue(visit_count == -1)

    def test_badly_counting(self):
        # 10/12/2024 10:00 bad counting == 7
        # (120 = avg_min, bad_behavior_rate = 0.005 , malfunction_rate = 0.002)
        """
            Test a certain day for a certain hour : 7 visitors instead of more (opened)
            day : saturday 10/12/2024
            hour : 10:00
        :return:
        """
        print(f'counter_id: {0}')
        visit_count = self.Big_agency_1.get_counter_traffic(
                            datetime(2026, 3, 18, 9),
                            counter_id=0)
        print(f'visit_count = {visit_count}')
        print(' ')
        self.assertTrue(visit_count == 10)

        print(f'counter_id: {1}')
        visit_count = self.Big_agency_1.get_counter_traffic(
                            datetime(2025, 10, 24, 9),
                            counter_id=1)
        print(f'visit_count = {visit_count}')
        print(' ')
        self.assertTrue(visit_count == 14)

        print(f'counter_id: {2}')
        visit_count = self.Big_agency_1.get_counter_traffic(
                            datetime(2025, 1, 17, 9),
                            counter_id=2)
        print(f'visit_count = {visit_count}')
        print(' ')
        self.assertTrue(visit_count == 7)


    def test_broken_counter(self):
        # 29/05/2025 10:00 dead counting == -10
        # malfunction_rate = 0.002)
        """
            Test a certain day for a certain hour : -10 visitors (broken)
            day : saturday 10/12/2024
            hour : 10:00
        :return:
        """
        for counter_id in range(self.Big_agency_1.counter_num):
            visit_count = self.Big_agency_1.get_counter_traffic(
                    datetime(2023, 9, 12, 9),
                    counter_id=1)
            self.assertTrue(visit_count == -10)

    def test_different_count_with_similar_agencies(self):
        """
        test daily_average_visitor_count
        :return:
        """
        visit_count_1 = self.Big_agency_1.get_counter_traffic(
                datetime(2025, 5, 29, 10),
                counter_id=0)
        visit_count_2 = self.Big_agency_2.get_counter_traffic(
                datetime(2025, 5, 29, 10),
                counter_id=0)

        self.assertTrue(visit_count_1 != visit_count_2)



if __name__ == "__main__":
    unittest.main()
