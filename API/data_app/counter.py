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

from API.data_app.random_seed import RandomSeed


class VisitorCounter:
    """
    simulates a visitor counter
    uses numpy random normal number generator
    avg_visitor_count : min avg_visit_counter per day of the week
    std_pct : percentage of std_dev to use with avg_visitor_count in
    """

    def __init__(self,
                 avg_visitor_count: int = 120,
                 std_pct: float = 0.15) -> None:
        """
        simulates a visitor counter
        uses random number generator to do so
        avg_visitor_count :
        """
        self.avg_visitor_count = avg_visitor_count
        self.std_pct = std_pct

    def get_visit_count(self, dt: datetime, random_seed:int=-1):
        """
        Simulates a reproducible visitor count in a bank (typically city center)
        for a given :
            - day
            - hour
        needs a random_seed object, generated from various parameters
        (datetime, Agency_name, counter_id)
        :return: visit_count
        """
        hour_visits = None

        # if no random_seed given use date
        if random_seed == -1 :
            # concat y,m,d (not hours, want same result whatever the hours e.g : broken behavior)
            random_seed = int(dt.strftime("%Y%m%d"))

        try:
            # if the counter works
            if self.is_working(random_seed):

                # generate random_visit_count (day base)
                np.random.seed(random_seed)  # for reproducibility
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
                    if (not hour_visits == -1
                            and self.is_badly_counting(random_seed)):
                        print(f"Normally counting : {hour_visits}")
                        hour_visits = int(0.2 * hour_visits)
                        print(f"Bad counting : {hour_visits}")

            # defective counter : sends -1
            else:
                hour_visits = -10
                print('dead counter')

        except ValueError as e:
            print(e)

        return hour_visits

    @staticmethod
    def is_badly_counting(random_seed: int) -> bool:
        """
        random number to see if counter works badly (counts only a fraction of the traffic)
        :param random_seed:
        :return: True or False
        """
        np.random.seed(random_seed)  # reproducibility
        bad_behavior_rate = 0.015  # greater than 0.005 test already passed
        return np.random.random() <= bad_behavior_rate

    @staticmethod
    def is_working(random_seed: int) -> bool:
        """
        random number to see if counter works normally this peculiar day
        :param random_seed:
        :return: True or False
        """
        np.random.seed(random_seed)  # reproducibility
        malfunction_rate = 0.005
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
        # List of multipliers for each hour from 0 to 23
        hour_multipliers = [
            0, 0, 0, 0, 0, 0, 0, 0, 0,  # 0-8
            0.2, 0.25, 0.25, 0,  # 9-12
            0.1, 0.1, 0.02, 0.03, 0.05,  # 13-17
            0, 0, 0, 0, 0, 0  # 18-23
        ]

        # Default to -1 if hour is not in the range with multipliers
        multiplier = hour_multipliers[dt.hour] if 0 <= dt.hour < len(hour_multipliers) else -1
        return int(day_avg_visits * multiplier) if multiplier > 0 else -1 # we're closed

    @staticmethod
    def get_day_visits(random_visit_count, dt):
        """
        Calculate average daily visits based on weekday multipliers.
        Rules :
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

        :param random_visit_count: Average visit count for calculations
        :param dt: A datetime object
        :return: Calculated visits or -1 for invalid days
        """
        # List of multipliers for weekdays (Monday to Sunday)
        weekday_multipliers = [1.0, 1.25, 1.3, 1.6, 1.8, 0, 0]

        # Get the multiplier for the current weekday
        multiplier = weekday_multipliers[dt.weekday()] if 0 <= dt.weekday() <= 6 else None

        if multiplier is None:
            raise ValueError("weekday not between 0 and 6!")

        # Return calculated visits or -1 for non-visiting days (multiplier 0)
        return random_visit_count * multiplier if multiplier > 0 else -1


if __name__ == "__main__":

    counter = VisitorCounter()

    # Testing : printing days and hours for a full month
    # 7 000 hour testing
    increment = timedelta(hours=1)
    date_time = datetime(2024, 12, 1, 0, 0)

    for i in range(7000):
        visitors = counter.get_visit_count(date_time)
        print(
            f"This day {date_time.strftime("%A")} {date_time.day}/{date_time.month}/{date_time.year}, "
            f"at {date_time.hour} got {visitors} visitors"
        )
        date_time += increment
