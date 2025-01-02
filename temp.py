import hashlib

import numpy as np

date_time = 20240930
counter_id = 1
name= 'Lyon_1'

params_string = f"{date_time}{counter_id}{name}"

# Hash the concatenated string using SHA256
hash_object = hashlib.sha256(params_string.encode())
hash_value = int(hash_object.hexdigest(), 16)  # Convert hex to an integer

# Reduce the hash value to fit within the valid seed range
valid_seed = hash_value % (2**32)  # Ensure the seed is between 0 and 2**32 - 1

# Set the seed for numpy.random
np.random.seed(valid_seed)

print(np.random.normal(1, 0.15))
