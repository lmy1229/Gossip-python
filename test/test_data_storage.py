from cassandra.engine.storage import DataStorage

import random
import string

def random_str(length):
    selection = string.ascii_letters + string.digits
    return ''.join([random.choice(selection) for i in range(length)])

def main():
    config = {
        'datafile_dir': 'data/test/',
        'max_indices_in_memory': 10,
        'max_data_per_sstable': 1000 # 10M
    }
    ds = DataStorage(**config)

    # put test
    keys = set()
    for i in range(1000):
        key = random_str(2)
        data = random_str(10)
        keys.add(key)
        # ds.put(key, data)

    # get test
    keys = list(keys)
    for i in range(20):
        key = random.choice(keys)
        print(key + " - " + str(ds.get(key)))

    for i in range(20):
        key = random_str(2)
        print(key + " - " + str(ds.get(key)))