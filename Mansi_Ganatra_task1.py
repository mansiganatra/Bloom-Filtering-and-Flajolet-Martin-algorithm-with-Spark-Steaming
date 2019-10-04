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

if len(sys.argv) != 3:
    print("Usage: ./bin/spark-submit Mansi_Ganatra_task1.py <port_number> <output_file_path>")
    exit(-1)
else:
    port_number = int(sys.argv[1])
    output_file_path = sys.argv[2]


def generate_hash_codes(city):

    global n_hashes
    hashes = []
    for i in range(1, n_hashes):
        current_hash_code = ((i * city) + (5 * i * 13)) % 200
        hashes.append(current_hash_code)

    return hashes


def apply_bloom_filter(current_stream):

    global cities_so_far
    global n_false_positive
    global n_cities
    global output_fp

    current_cities_in_stream = current_stream.collect()

    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for city in current_cities_in_stream:
        n_cities += 1
        city_hash = int(binascii.hexlify(city.encode('utf8')),16)
        hashes = generate_hash_codes(city_hash)

        is_new_entry = False
        for hashcode in hashes:
            if bloom_filter_bit_array[hashcode] == 0:
                bloom_filter_bit_array[hashcode] = 1
                is_new_entry = True

        if not is_new_entry and city not in cities_so_far:
            n_false_positive += 1

        cities_so_far.add(city)

    false_positive_rate = float(n_false_positive)/n_cities

    # output_fp.write('\n'.join('{},{}'.format(str(current_timestamp), str(false_positive_rate))))
    output_fp.write(str(current_timestamp) + "," + str(false_positive_rate) + "\n")
    output_fp.flush()

    return


# Create a local StreamingContext with two working thread and batch interval of 1 second
SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
sc = SparkContext("local[*]", "task1")
ssc = StreamingContext(sc, 10)
bloom_filter_bit_array = [0] * 200
n_cities = 0
n_false_positive = 0
n_hashes = 10
cities_so_far = set()

output_fp = open(output_file_path, "a+")
output_fp.write("Time,FPR")
output_fp.write("\n")

# Create a DStream that will connect to hostname:port, like localhost:9999
business_rdd = ssc.socketTextStream("localhost", port_number)

finalRdd = business_rdd.map(json.loads)\
    .map(lambda entry: entry['city'])\
    .foreachRDD(apply_bloom_filter)

ssc.start()
ssc.awaitTermination()