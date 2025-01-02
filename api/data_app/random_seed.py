"""
Module to generate a random seed to ensure reproducibility
for get_visitor_count
on a basis of:
_date
_agency_name
_counter_id
_whatever other parameters we may imagine
"""

import hashlib
from datetime import datetime


class RandomSeed:
    """
    Class to generate a random seed to ensure reproducibility
    """

    def __init__(self, date_time: datetime = ""):
        self.base_seed = int(date_time.strftime("%Y%m%d"))

    def generate_seed(self, *args):
        """
        Combine base seed with additional parameters to generate a unique seed.
        """
        combined = str(self.base_seed) + "".join(map(str, args))

        # Hash the concatenated string using SHA256
        hash_object = hashlib.sha256(combined.encode())
        hash_value = int(hash_object.hexdigest(), 16)  # Convert hex to an integer

        # Reduce the hash value to fit within the valid seed range (for np.random.seed())
        valid_seed = hash_value % (2**32)  # Ensure the seed is between 0 and 2**32 - 1

        return valid_seed
