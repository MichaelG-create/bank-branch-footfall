"""
module to test module counter.py
- object VisitorCounter
    - method get_visitor_count
    - method is_working (indirectly)
    - method is_badly_working (indirectly)
"""

import unittest
from datetime import datetime

from src.API.data_app.counter import VisitorCounter


class TestVisitorCounter(unittest.TestCase):
    """
    tests:
    ------
    -worked days for hours in [9:12, 13:17] : not -1 (opened)
    -worked days for hours in [0:8, 12, 18:23] : -1 (closed)
    -not worked_days for all hours: -1 (closed)
    -badly_working a certain date (detected in logs)
    -broken a certain date (detected in logs)

    -average of mondays close to average 120 (+- std = 15%) (for 100 days)
    -average of tuesdays close to average 120*1.25 (+- std = 15%) (for 100 days)
    -average of wednesdays close to average 120*1.30 (+- std = 15%) (for 100 days)
    -average of thursdays close to average 120*1.6 (+- std = 15%) (for 100 days)
    -average of fridays close to average 120*1.8 (+- std = 15%) (for 100 days)
    """

    def setUp(self):
        """
        executed before every test
        :return:
        """
        self.visitors = VisitorCounter()

    def test_weekdays_opened(self):
        """
            Test worked days at 9:00 : not -1 (opened)
            week : monday 11/9/2023 to friday 15/9/2023 (16 excluded)
            HOUR : 9 AM
        :return:
        """
        for test_day in range(11, 16):
            with self.subTest(i=test_day):
                visit_count = self.visitors.get_visit_count(
                    datetime(2023, 9, test_day, 9)
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
                visit_count = self.visitors.get_visit_count(
                    datetime(2024, 12, 11, test_hour)
                )
                self.assertFalse(visit_count == -1)

    def test_weekend_closed(self):
        """
            Test a saturday day for different hours : not -1 (opened)
            weekend : saturday 21/12/2024 and sunday 10/11/2024
            hours : 9 to 17 (without 12)
        :return:
        """
        for test_hour in [0, 9, 12, 15, 22]:
            with self.subTest(i=test_hour):
                visit_count = self.visitors.get_visit_count(
                    datetime(2024, 12, 21, test_hour)
                )
                self.assertTrue(visit_count == -1)

        for test_hour in [0, 9, 12, 15, 22]:
            with self.subTest(i=test_hour):
                visit_count = self.visitors.get_visit_count(
                    datetime(2024, 11, 10, test_hour)
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
        visit_count = self.visitors.get_visit_count(datetime(2024, 12, 10, 10))
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
        visit_count = self.visitors.get_visit_count(datetime(2025, 5, 29, 10))
        self.assertTrue(visit_count == -10)

    def test_average(self):
        """
        test daily_average_visitor_count
        :return:
        """


if __name__ == "__main__":
    unittest.main()
