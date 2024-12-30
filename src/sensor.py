"""
sensor module
Contains VisitorCounter a class to generate visitor count :
- at a given date and hour
according to :
- an avg_visitor_count (typically 120 visitors per day for the minimum visits day)
- a std_dev_visitor_count (typically 15% of avg_visitor_count )
"""

from datetime import datetime, timedelta

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

        # for the random generations -> concat y,m,d (no hours, we want the same result whatever the hours)
        formated_date = int(dt.strftime("%Y%m%d")) # %H if we want to have the bad event happening on an hour basis

        try:
            # if the counter works
            if self.is_working(formated_date):

                # generate random_visit_count (day base)
                np.random.seed(formated_date)  # for reproducibility
                random_visit_count = np.random.normal(
                    self.avg_visitor_count, self.std_pct * self.avg_visitor_count
                )
                # modulate visits with week_day
                day_avg_visits = self.get_day_visits(random_visit_count, dt)

                # if we're closed, no need to modulate hour
                if day_avg_visits == -1:
                    hour_visits = -1
                else:
                    # modulate visits with hour of the day
                    hour_visits = self.get_hour_visits(day_avg_visits, dt)

                # works bad : only 20% traffic counted
                if self.is_badly_counting(formated_date):
                    print('bad counting')
                    hour_visits = int(0.2*hour_visits)
            # defective counter : sends -1
            else:
                hour_visits = -10
                # print('dead counter')

        except ValueError as e:
            print(e)

        return hour_visits

    @staticmethod
    def is_badly_counting(seed_date: int) -> bool:
        """
        random number to see if counter works badly (count only a fraction of the traffic)
        :param seed_date:
        :return: True or False
        """
        np.random.seed(seed_date) # reproducibility
        bad_behavior_rate = 0.005 # greater than 0.0015 test already passed
        return np.random.random() <= bad_behavior_rate

    @staticmethod
    def is_working(seed_date: int) -> bool:
        """
        random number to see if counter works normally this peculiar day
        :param seed_date:
        :return: True or False
        """
        np.random.seed(seed_date) # reproducibility
        malfunction_rate = 0.002
        return np.random.random() > malfunction_rate

    @staticmethod
    def get_hour_visits(day_avg_visits, dt):
        """
        Rules for the hours:
        --------------------
        00:00-09:00 : traffic_fraction = -1
        09:00-10:00 : traffic_fraction = 20 % * day_avg_visits
        10:00-11:00 : traffic_fraction = 25 % * day_avg_visits
        11:00-12:00 : traffic_fraction = 25 % * day_avg_visits
        12:00-13:00 : traffic_fraction = -1
        13:00-14:00 : traffic_fraction = 10 % * day_avg_visits
        14:00-15:00 : traffic_fraction = 10 % * day_avg_visits
        15:00-16:00 : traffic_fraction =  2 % * day_avg_visits
        16:00-17:00 : traffic_fraction =  3 % * day_avg_visits
        17:00-18:00 : traffic_fraction =  5 % * day_avg_visits
        18:00-00:00 : traffic_fraction = -1

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
            hour_visits = -1
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
            day_avg_visits = -1
        elif dt.weekday() == 6:
            day_avg_visits = -1
        else:
            raise ValueError("weekday not between 0 and 6!")
        return day_avg_visits


if __name__ == "__main__":

    Counter = VisitorCounter()

    # Testing : printing days and hours for a full month
    # 7 000 hour testing
    increment = timedelta(hours=1)
    date = datetime(2024, 12, 1, 0, 0)

    for i in range(7000):
        visitors = Counter.get_visit_count(date)
        print(
            f"This day {date.day}/{date.month}/{date.year}, "
            f'on {date.strftime("%A")} '
            f"at {date.hour} o" + "'" + "clock, "
            f"got {visitors} visitors"
        )
        date += increment

