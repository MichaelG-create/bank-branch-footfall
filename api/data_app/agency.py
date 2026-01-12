# pylint: disable=duplicate-code
"""
Module implementing the Agency class used in the api
"""

import logging
from datetime import datetime, timedelta

from bank_footfall.date_randomseed.random_seed import RandomSeed

from api.data_app.counter import VisitorCounter

logging.info("successfuly loaded from date_randomseed.random_seed import RandomSeed")


# ----------------------------------------------------------------
#         Define the agency class
# ----------------------------------------------------------------
class Agency:
    """
    An agency contains :
    - the name of the agency (e.g. 'Lyon_1', 'Lyon_2', etc.)
    - their size ('small', 'medium', 'big'),
    - their location type ('Countryside', 'Mid_sized_city', 'Metropolis')
    - a base_traffic
    - a list of 'counter_num' VisitorCounters
    An agency has :
    - method to get the number of visitors for a given counter
    - method to get the number of visitors for all counters
    """

    def __init__(
        self,
        name: str,
        size: str,
        location_type: str,
        base_traffic: int,
        counter_num: int = 1,
    ):  # pylint: disable=R0913, R0917
        self.name = name
        self.size = size
        self.location_type = location_type
        self.base_traffic = base_traffic
        self.counter_num = counter_num

        self.counter_list = self.create_counters()

    def create_counters(self) -> list[VisitorCounter]:
        """
        returns a list of counters matching the base_traffic
        :param self:
        :return: list of VisitorCounter, each having its own base traffic
        """
        list_of_counter = []
        traffic_fraction_list = self.create_fractional_range(self.counter_num)
        # logging.info(f"{traffic_fraction_list}")
        # logging.info(f"self.name : {self.name}")
        # logging.info(f"self.size : {self.size}")
        # logging.info(f"self.location type : {self.location_type}")
        # logging.info(f"self.base_traffic : {self.base_traffic}")
        # logging.info(f"self.counter_num : {self.counter_num}")

        for j in range(self.counter_num):
            traffic_fraction = int(self.base_traffic * traffic_fraction_list[j])
            # logging.info(f"self.base_traffic = {self.base_traffic}")
            # logging.info(f"traffic_fraction_list[{i}] = {traffic_fraction_list[i]}")
            # logging.info(f"traffic_fraction: {traffic_fraction}")
            list_of_counter.append(VisitorCounter(traffic_fraction))

        return list_of_counter

    @staticmethod
    def create_fractional_range(n: int) -> list[float]:
        """
        creates a list of float
        - total sum = 1
        - first half sum = 0.8
        - second half sum = 0.2
        e.g.:
        logging.info(create_fractional_range(1))  # [1.0]
        logging.info(create_fractional_range(2))  # [0.8, 0.2]
        logging.info(create_fractional_range(3))  # [0.4, 0.4, 0.2]
        logging.info(create_fractional_range(4))  # [0.4, 0.4, 0.1, 0.1]
        logging.info(create_fractional_range(5))  # [0.27, 0.27, 0.27, 0.1, 0.09]

        :return:
        """
        if n == 1:
            return [1.0]

        # Calculate how many elements should be in the first and second half
        first_half_size = (n + 1) // 2  # If odd, the first half gets one more element
        second_half_size = n - first_half_size

        # Distribute 0.8 to the first half, 0.2 to the second half
        first_half_value = 0.8 / first_half_size
        second_half_value = 0.2 / second_half_size if second_half_size > 0 else 0

        # Create the list by combining both parts
        result = [first_half_value] * first_half_size + [
            second_half_value
        ] * second_half_size

        # Round all values to two decimal places
        result = [round(x, 2) for x in result]

        # Correct the total sum to exactly 1 by adjusting the last element
        discrepancy = round(1 - sum(result), 2)
        result[-1] = round(result[-1] + discrepancy, 2)
        # Round the last element to avoid floating-point issues

        return result

    def get_counter_traffic(
        self, time_date: datetime, counter_id: int = 0
    ) -> int | None:
        """
        returns the number of visitors
        for a given counter
        in a given agency
        at a certain time_date
        WARNING : have to modulate the result with :
        - counter_id
        - agency_name
        Else 2 counter having
        - the same time_date :
        - the same fraction of traffic
        - the same kind of agency (size, location_type)
        will have the same traffic !
        :param time_date:
        :param counter_id:
        :return:
        """
        try:
            # initiate the random_seed based on time_date, self.name, counter_id
            random_seed = RandomSeed(time_date).generate_seed(self.name, counter_id)

            traffic = self.counter_list[counter_id].get_visit_count(
                time_date, random_seed=random_seed
            )
            logging.info(
                "%s, %s, c_id: %d, seed: %d, traffic is %s",
                time_date,
                self.name,
                counter_id,
                random_seed,
                traffic,
            )

        except KeyError:
            logging.error(
                "counter_id: %d does not exist, max counter_id for the store %s is %d",
                counter_id,
                self.name,
                self.counter_num - 1,
            )

        return traffic

    def get_all_counter_traffic(self, time_date: datetime) -> int | None:
        """
        returns total number of visitors for all counters at the given time_date
        :param time_date:
        :return:
        """
        traffic = 0
        try:
            for c_i, _ in enumerate(self.counter_list):
                traffic += self.get_counter_traffic(time_date, counter_id=c_i)
            return traffic
        except KeyError as e:
            logging.error("No VisitorCounter found at all for the store %s", self.name)
            logging.error(e)
            return None


if __name__ == "__main__":
    big_agency_1 = Agency("Lyon_1", "big", "Metropolis", 500, 3)

    # Testing : printing days and hours for a full month
    # 7 000 hour testing
    increment = timedelta(hours=1)
    date_time = datetime(2024, 12, 1, 0, 0)

    for i in range(37000):
        for count_id in range(3):
            visitors = big_agency_1.get_counter_traffic(date_time, counter_id=count_id)
            logging.info(
                "This day %s %d/%d/%d, at %d got %d visitors in agency %s on counter_id %d",
                date_time.strftime("%A"),
                date_time.day,
                date_time.month,
                date_time.year,
                date_time.hour,
                visitors,
                big_agency_1.name,
                count_id,
            )
        date_time += increment
