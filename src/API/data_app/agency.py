"""
Module implementing the Agency class used in the API
"""
from datetime import datetime

from src.API.data_app.sensor import VisitorCounter

# ----------------------------------------------------------------
#         Define the agency class
# ----------------------------------------------------------------
class Agency:
    """
    An agency contains :
    - the name of the agency (e.g. 'Lyon_1', 'Lyon_2', etc.)
    - their size ('small', 'medium', 'big'),
    - their location type ('Countryside', 'Mid_sized_city', 'Metropolis')
    - a traffic_base
    - a list of 'counter_num' VisitorCounters
    An agency has :
    - method to get the number of visitors for a given counter
    - method to get the number of visitors for all counters
    """
    def __init__(self,
                 name: str,
                 size:str,
                 location_type:str,
                 traffic_base:int,
                 counter_num:int=1
        ):
        self.name = name
        self.size = size
        self.location_type = location_type
        self.traffic_base = traffic_base
        self.counter_num = counter_num

        self.counter_list = self.create_counters()

    def create_counters(self) -> list[VisitorCounter]:
        """
        returns a list of counters matching the traffic_base
        :param self:
        :return: list of VisitorCounter, each having its own base traffic
        """
        list_of_counter=[]
        traffic_fraction_list = self.create_fractional_range(self.counter_num)

        for i in range(self.counter_num):
            traffic_fraction = int(self.traffic_base * traffic_fraction_list[i])
            list_of_counter.append(VisitorCounter(traffic_fraction))

        return list_of_counter

    @staticmethod
    def create_fractional_range(n:int)-> list[float]:
        """
        creates a range of float
        - total sum = 1
        - first half sum = 0.8
        - second half sum = 0.2
        e.g.:
        print(create_fractional_range(1))  # [1.0]
        print(create_fractional_range(2))  # [0.8, 0.2]
        print(create_fractional_range(3))  # [0.4, 0.4, 0.2]
        print(create_fractional_range(4))  # [0.4, 0.4, 0.1, 0.1]
        print(create_fractional_range(5))  # [0.27, 0.27, 0.27, 0.1, 0.09]

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
        result = [first_half_value] * first_half_size + [second_half_value] * second_half_size

        # Round all values to two decimal places
        result = [round(x, 2) for x in result]

        # Correct the total sum to exactly 1 by adjusting the last element
        discrepancy = round(1 - sum(result), 2)
        result[-1] = round(result[-1] + discrepancy,
                           2)  # Round the last element to avoid floating-point issues

        return result

    def get_counter_traffic(self, date_time:datetime, counter_id:int)-> int | None:
        """
        returns the number of visitors for a given counter
        :param date_time:
        :param counter_id:
        :return:
        """
        try:
            return self.counter_list[counter_id].get_visit_count(date_time)
        except KeyError:
            print(f'counter_id : {counter_id} does not exist, '
                  f'max counter_id for the store {self.name} is {self.counter_num-1}')

    def get_all_counter_traffic(self, date_time:datetime)-> int | None:
        """
        returns total number of visitors for all counters at the given date_time
        :param date_time:
        :return:
        """
        traffic = 0
        try:
            for counter in self.counter_list:
                traffic += counter.get_visit_count(date_time)
            return traffic
        except KeyError as e:
            print(f'No VisitorCounter found at all for the store {self.name}')
            print(e)
