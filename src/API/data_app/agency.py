"""
Module implementing the Agency class used in the API
"""
import duckdb

from src.API.data_app.sensor import VisitorCounter

# ----------------------------------------------------------------
#         Define the agency class
# ----------------------------------------------------------------
class Agency:
    """
    An agency contains :
    - one counter
    - a traffic depending on :
        - their size ('small', 'medium', 'big'),
        - their location type ('Countryside', 'Mid_sized_city', 'Metropolis')
        - the name of the agency (e.g. 'Lyon_1', 'Lyon_2', etc.)
    """
    def __init__(self,
                 name: str,
                 size:str,
                 location_type:str,
                 avg_visitor_count:int,
                 sensor_number:int=1
        ):
        self.name = name
        self.sensor_number = sensor_number
        self.size = size
        self.location_type = location_type
        self.avg_visitor_count =avg_visitor_count
        self.counter = self.create_counters()

        def create_counters(self) -> list[VisitorCounter]:
            """
            returns a list of sensors matching the base_visit_count
            depending on the location_type and size of the agency
            :param self:
            :return:
            """
            list_of_counter=[]
            for i in range(self.sensor_number):
                list_of_counter.append(VisitorCounter(self.avg_visitor_count))

            return list_of_counter



