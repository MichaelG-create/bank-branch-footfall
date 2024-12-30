"""
sensor module
Contains VisitorCounter a class to generate visitor count :
- at a given date and hour
according to :
- an avg_visitor_count (typically 120 visitors per day for the minimum visits day)
- a std_dev_visitor_count (typically 15% of avg_visitor_count )
"""

from datetime import datetime

import numpy as np


class VisitorCounter:
    """
    simulates a visitor counter
    uses numpy random normal number generator
    avg_visitor_count : min avg_visit_counter per day of the week
    std_pct : percentage of std_dev to use with avg_visitor_count in
    """

    def __init__(self, avg_visitor_count: int = 120, std_pct: float = 0.15) -> None:
        """
        simulates a visitor counter
        uses random number generator to do so
        avg_visitor_count :
        """
        self.avg_visitor_count = avg_visitor_count
        self.std_pct = std_pct

    def get_visit_count(self, dt: datetime):
        """
        Simulates a reproducible visitor count in a bank (typically city center)
        for a given :
            - day
            - hour
        :return: visit_count
        """
        hour_visits = None

        try:
            np.random.seed = 42  # for reproducibility

            random_visit_count = np.random.normal(
                self.avg_visitor_count, self.std_pct * self.avg_visitor_count
            )

            day_avg_visits = self.get_day_visits(random_visit_count, dt)

            hour_visits = self.get_hour_visits(day_avg_visits, dt)

        except ValueError as e:
            print(e)

        return hour_visits

    @staticmethod
    def get_hour_visits(day_avg_visits, dt):
        """
        Rules for the hours:
        --------------------
        00:00-09:00 : traffic_fraction = 0
        09:00-10:00 : traffic_fraction = 20 % * day_avg_visits
        10:00-11:00 : traffic_fraction = 25 % * day_avg_visits
        11:00-12:00 : traffic_fraction = 25 % * day_avg_visits
        12:00-13:00 : traffic_fraction = 0
        13:00-14:00 : traffic_fraction = 10 % * day_avg_visits
        14:00-15:00 : traffic_fraction = 10 % * day_avg_visits
        15:00-16:00 : traffic_fraction =  2 % * day_avg_visits
        16:00-17:00 : traffic_fraction =  3 % * day_avg_visits
        17:00-18:00 : traffic_fraction =  5 % * day_avg_visits
        18:00-00:00 : traffic_fraction = 0

        :param day_avg_visits: visits of the current day
        :param dt: datetime
        :return:
        """
        if dt.hour == 9:
            hour_visits = int(day_avg_visits * 0.2)
        elif dt.hour == 10:
            hour_visits = int(day_avg_visits * 0.25)
        elif dt.hour == 11:
            hour_visits = int(day_avg_visits * 0.25)
        # elif dt.hour == 12: hour_visits = 0
        elif dt.hour == 13:
            hour_visits = int(day_avg_visits * 0.1)
        elif dt.hour == 14:
            hour_visits = int(day_avg_visits * 0.1)
        elif dt.hour == 15:
            hour_visits = int(day_avg_visits * 0.02)
        elif dt.hour == 16:
            hour_visits = int(day_avg_visits * 0.03)
        elif dt.hour == 17:
            hour_visits = int(day_avg_visits * 0.05)
        else:
            hour_visits = 0
        return hour_visits

    @staticmethod
    def get_day_visits(random_visit_count, dt):
        """
        Rules for the days:
        -------------------
        avg_visitor_count : 120
        std_visitor_count : 15 % (for each avg_visitor_count for each day)
        - monday : day_avg_visits = avg_visits * 100%
        - tuesday : day_avg_visits = avg_visits * 125 %
        - wednesday : day_avg_visits = avg_visits * 130 %
        - thursday : day_avg_visits = avg_visits * 160 %
        - friday : day_avg_visits = avg_visits * 180 %
        - saturday : day_avg_visits = 0
        - sunday : day_avg_visits = 0

        :param dt:
        :param random_visit_count:
        :return:
        """
        if dt.weekday() == 0:
            day_avg_visits = random_visit_count * 1.0
        elif dt.weekday() == 1:
            day_avg_visits = random_visit_count * 1.25
        elif dt.weekday() == 2:
            day_avg_visits = random_visit_count * 1.3
        elif dt.weekday() == 3:
            day_avg_visits = random_visit_count * 1.6
        elif dt.weekday() == 4:
            day_avg_visits = random_visit_count * 1.8
        elif dt.weekday() == 5:
            day_avg_visits = 0
        elif dt.weekday() == 6:
            day_avg_visits = 0
        else:
            raise ValueError("weekday not between 0 and 6!")
        return day_avg_visits


if __name__ == "__main__":

    Counter = VisitorCounter()
    date = datetime(2024, 12, 31, 9, 30)
    visitors = Counter.get_visit_count(date)
    print(
        f"This day {date.day}/{date.month}/{date.year}, "
        f'on {date.strftime("%A")} '
        f"at {date.hour} o" + "'" + "clock, "
        f"got {visitors} visitors"
    )
