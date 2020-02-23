# coding=utf8
import math
import sys
import time

import pyspark
from operator import add

from pyspark import RDD
import json

import matplotlib.pyplot as plt


def q1(trips_lines: RDD):
    """
    How many unique taxis are present in the data?
    """
    taxi_id_lines = trips_lines.map(lambda x: x['taxi_id'])
    taxi_id_lines = taxi_id_lines.distinct()
    print("There are %d unique taxis.\n" % taxi_id_lines.count())


def q2(trips_lines: RDD):
    """
    How many trips did taxi 46 complete?
    """
    taxi_id_lines = trips_lines.filter(lambda x: x['taxi_id'] == '46')
    print("Taxi 46 completed %d trips.\n" % taxi_id_lines.count())


def q3(trips_lines: RDD):
    """
    Which taxi participated in the most trips?
    """
    taxi_id_cnt_lines = trips_lines.map(lambda x: (x['taxi_id'], 1))
    taxi_id_cnt_lines = taxi_id_cnt_lines.reduceByKey(add)
    res = taxi_id_cnt_lines.max(key=lambda x: x[1])
    print("Taxi %s participated in the most trips.\n" % res[0])


def get_distance(start_latitude, start_longitude, end_latitude, end_longitude):
    """
    Spherical Earth projected to a plane
    """
    delta_latitude = (start_latitude - end_latitude) * 2 * math.pi / 360
    delta_longitude = (start_longitude - end_longitude) * 2 * math.pi / 360
    mean_latitude = (start_latitude + end_latitude) / 2.0 * 2 * math.pi / 360
    R = 6371.009
    distance = R * math.sqrt(delta_latitude ** 2 + (math.cos(mean_latitude) * delta_longitude) ** 2)
    return distance


def q4(trips_lines: RDD):
    """
    What is the distance of the longest trip?
    :return:
    """
    dis_lines = trips_lines.map(lambda x: x['distance'])
    print("The distance of the longest trip is %.2f.\n" % dis_lines.max())


def q5(trips_lines: RDD):
    """
    Which three taxis completed the longest trips on average?
    :param trips_lines:
    :return:
    """
    taxi_id_distance_lines = trips_lines.map(lambda x: (x['taxi_id'], x['distance']))
    taxi_id_distance_lines = taxi_id_distance_lines.groupByKey()
    taxi_id_distance_lines = taxi_id_distance_lines.mapValues(list).mapValues(
        lambda distances: sum(distances) / len(distances))
    print(taxi_id_distance_lines.top(3, key=lambda x: x[1]))


def q6(trips_lines: RDD):
    """
    On what date did the most trips start?
    """
    date_cnt_lines = trips_lines.map(lambda x: (x['start_date'], 1))
    date_cnt_lines = date_cnt_lines.reduceByKey(add)
    res = date_cnt_lines.max(key=lambda x: x[1])
    print("\nOn date: %s the most trips start.\n" % res[0])


def format_data(iterator):
    for line in iterator:
        taxi_id, start_date, latitude1, longitude1, end_date, latitude2, longitude2 = line.split()

        start_date = time.localtime(float(start_date))
        start_date = time.strftime("%Y%m%d", start_date)

        end_date = time.localtime(float(end_date))
        end_date = time.strftime("%Y%m%d", end_date)

        line = {'taxi_id': taxi_id, 'start_date': start_date, 'start_latitude': float(latitude1),
                'start_longitude': float(longitude1), 'end_date': end_date, 'end_latitude': float(latitude2),
                'end_longitude': float(longitude2)}
        line['distance'] = get_distance(line['start_latitude'], line['start_longitude'], line['end_latitude'],
                                        line['end_longitude'])
        yield line


def compute_trip_dis_cost():

    input_file = sys.argv[1]
    sc = pyspark.SparkContext()
    start_time = time.time()
    trips_lines = sc.textFile(input_file)
    trips_lines = trips_lines.map(lambda x: x.split())
    trips_lines = trips_lines.map(lambda x: get_distance(float(x[2]), float(x[3]), float(x[5]), float(x[6])))
    print(trips_lines.max())
    end_time = time.time()
    print("Compute trips distance elapsed %.2fs using Python.\n" % (end_time - start_time))
    sc.stop()


def plot_distri(trips_lines: RDD):
    print("Filtered %d records out that are greater than 160km.\n" % trips_lines.filter(lambda x: x['distance'] > 160).count())
    total = trips_lines.count()
    trips_lines = trips_lines.filter(lambda x: x['distance'] < 160)
    distri_lines = trips_lines.map(lambda x: (int(x['distance']), 1))
    distri_lines = distri_lines.reduceByKey(add)
    distri_lines = distri_lines.mapValues(lambda x: x * 1.0 / total)
    distri_lines = sorted(distri_lines.collect(), key=lambda x: x[0])

    distances = [x[0] for x in distri_lines]
    dis_cnt = [x[1] for x in distri_lines]

    plt.bar(range(len(dis_cnt)), height=dis_cnt)
    # plt.xticks(range(len(dis_cnt)), distances)
    plt.xlabel("Trip Distance (those less than 160km)")
    plt.ylabel("Frequency")
    plt.show()
    plt.show()


def main():
    """
    input data format:
    <taxi-id> <start date (POSIX) > <start pos (lat)> <start pos (long)>  <end date (POSIX)> <end pos (lat)> <end pos (long)>
    """
    input_file = sys.argv[1]
    sc = pyspark.SparkContext()
    trips_lines = sc.textFile(input_file)
    trips_lines = trips_lines.mapPartitions(format_data)
    trips_lines.persist(pyspark.StorageLevel.MEMORY_AND_DISK)

    q1(trips_lines)
    q2(trips_lines)
    q3(trips_lines)
    q4(trips_lines)
    q5(trips_lines)
    q6(trips_lines)

    plot_distri(trips_lines)
    sc.stop()


def test():
    from geopy.distance import geodesic
    lines = format_data(sys.stdin)
    for line in lines:
        print(json.dumps(line, indent=4))
        print(geodesic((line['start_latitude'], line['start_longitude']),
                       (line['end_latitude'], line['end_longitude'])).km)
    sys.exit(0)


if __name__ == "__main__":
    # test()
    compute_trip_dis_cost()
    main()
