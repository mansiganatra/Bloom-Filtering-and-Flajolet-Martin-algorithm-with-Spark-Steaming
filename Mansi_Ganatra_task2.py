from pyspark import SparkContext, StorageLevel
from pyspark.streaming import StreamingContext
import sys
import json
import csv
import itertools
from time import time
import math
import binascii
from datetime import datetime
from statistics import mean, median
import random

if len(sys.argv) != 3:
    print("Usage: ./bin/spark-submit Mansi_Ganatra_task1.py <port_number> <output_file_path>")
    exit(-1)
else:
    port_number = int(sys.argv[1])
    output_file_path = sys.argv[2]


# def generate_hash_codes(city):
#
#     hashes = []
#     for i in range(1, 10):
#         current_hash_code = ((i * city) + (5 * i * 13)) % 200
#         hashes.append(current_hash_code)
#
#     return hashes

def apply_flajolet_martin(current_stream):

    global cities_so_far
    global n_cities
    global output_fp
    global n_hashes
    global m_expected

    current_cities_in_stream = current_stream.collect()
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    actual_n_cities = len(set(current_cities_in_stream))
    current_expected_2r = 0
    max_trailing_zeros = -math.inf
    all_hashes = []
    for i in range(1, n_hashes+1):
        for city in current_cities_in_stream:
            # n_cities += 1
            city_hash = int(binascii.hexlify(city.encode('utf8')),16)
            current_hash_code = (((random_prime_a[i] * city_hash) + random_prime_b[i]) % 1398318269) % m_expected
            current_hash_code_binary = bin(current_hash_code)[2:]
            current_hash_code_binary_without_trailing_zeros = current_hash_code_binary.rstrip("0")
            n_trailing_zeros = len(current_hash_code_binary) - len(current_hash_code_binary_without_trailing_zeros)

            if n_trailing_zeros > max_trailing_zeros:
                max_trailing_zeros = n_trailing_zeros

        all_hashes.append((2 ** max_trailing_zeros))
        max_trailing_zeros = -math.inf


    grouped_averages = []
    current_index = 0
    for group_range in range(3,n_hashes,3):
       group_hashes = all_hashes[current_index:group_range]
       current_index = group_range
       grouped_averages.append(mean(group_hashes))

    current_expected_2r = median(grouped_averages)

    # output_fp.write('\n'.join('{},{}'.format(str(current_timestamp), str(false_positive_rate))))
    output_fp.write(str(current_timestamp) + "," + str(actual_n_cities) + "," + str(current_expected_2r) + "\n")
    output_fp.flush()

    return


batch_size = 5
window_length = 30
sliding_interval = 10
n_hashes = 15
# number of distinct elements
m_expected = 2 ** n_hashes
seed = 5
random.seed(seed)

random_prime_a = [1187,1193,1201,1213,1217,1223,18371,18379,18397,
                  18401,18413,18427,16963,16979,16981,16987,16993,17011,
                  17573,17579,15737,15739,15791]

random_prime_b = [11633,11657,11677,11681,11689,11699,11701,17477,
                  17483,17489,17491,17497,17509,17519,17539,17551,
                  17569,15749,15761,15767,15773,1578]

# random.shuffle(random_prime_a)
# random.shuffle(random_prime_b)


# Create a local StreamingContext with two working thread and batch interval of 1 second
SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
sc = SparkContext("local[*]", "task1")
ssc = StreamingContext(sc, batch_size)

output_fp = open(output_file_path, "a+")
output_fp.write("Time,Ground Truth,Estimation")
output_fp.write("\n")

# Create a DStream that will connect to hostname:port, like localhost:9999
business_rdd = ssc.socketTextStream("localhost", port_number)

finalRdd = business_rdd.map(json.loads)\
    .map(lambda entry: entry['city'])\
    .window(windowDuration=window_length, slideDuration=sliding_interval)\
    .foreachRDD(apply_flajolet_martin)

ssc.start()
ssc.awaitTermination()
