"""
Module implementing the Agency class used in the API
"""
import hashlib
from datetime import datetime

import numpy as np

from src.API.data_app.counter import VisitorCounter

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
    def __init__(self,
                 name: str,
                 size:str,
                 location_type:str,
                 base_traffic:int,
                 counter_num:int=1
        ):
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
        list_of_counter=[]
        traffic_fraction_list = self.create_fractional_range(self.counter_num)
        print(f'{traffic_fraction_list}')
        print(f'self.name : {self.name}')
        print(f'self.size : {self.size}')
        print(f'self.location type : {self.location_type}')
        print(f'self.base_traffic : {self.base_traffic}')
        print(f'self.counter_num : {self.counter_num}')

        for i in range(self.counter_num):
            traffic_fraction = int(self.base_traffic * traffic_fraction_list[i])
            print(f'self.base_traffic = {self.base_traffic}')
            print(f'traffic_fraction_list[{i}] = {traffic_fraction_list[i]}')
            print(f'traffic_fraction: {traffic_fraction}')
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
        returns the number of visitors
        for a given counter
        in a given agency
        at a certain date_time
        WARNING : have to modulate the result with :
        - counter_id
        - agency_name
        Else 2 counter having
        - the same date_time :
        - the same fraction of traffic
        - the same kind of agency (size, location_type)
        will have the same traffic !
        :param date_time:
        :param counter_id:
        :return:
        """
        try:
            return self.modulate_traffic_with_agency_and_counter_id(date_time,counter_id)
        except KeyError:
            print(f'counter_id : {counter_id} does not exist, '
                  f'max counter_id for the store {self.name} is {self.counter_num-1}')

    def modulate_traffic_with_agency_and_counter_id(self, date_time:datetime, counter_id:int)-> int | None:
        # Concatenate parameters (including the string) into a single string
        params_string = f"{date_time}{counter_id}{self.name}"

        # Hash the concatenated string using SHA256
        hash_object = hashlib.sha256(params_string.encode())
        hash_value = int(hash_object.hexdigest(), 16)  # Convert hex to an integer

        # Reduce the hash value to fit within the valid seed range
        valid_seed = hash_value % (2 ** 32)  # Ensure the seed is between 0 and 2**32 - 1

        # Set the seed for numpy.random
        np.random.seed(valid_seed)

        modulation_rate = np.random.normal(1, 0.10) # modulate of +/- 10%
        counter_traffic = self.counter_list[counter_id].get_visit_count(date_time)

        return int(modulation_rate * counter_traffic)

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
