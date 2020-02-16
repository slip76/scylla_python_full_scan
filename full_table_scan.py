""" Based on https://www.scylladb.com/2017/02/13/efficient-full-table-scans-with-scylla-1-6/
and https://chriskiehl.com/article/parallelism-in-one-line """

import argparse
import contextvars
import os
# import time
from cassandra.cluster import Cluster
from multiprocessing import Pool


verbosity = contextvars.ContextVar('verbosity')


def get_n():
    """ Calculate "Parallel queries" """
    cluster = Cluster(['127.0.0.1', ], port=9042)
    _ = cluster.connect()
    nodes_in_cluster = len(cluster.metadata.all_hosts())
    # TODO (asmirnov): set cores_in_node value based on "num_procs" described here:
    # https://docs.datastax.com/en/opscenter/6.1/api/docs/cluster_info.html#response-node
    cores_in_node = 2
    if verbosity.get():
        print("DEBUG: Nodes in cluster:", nodes_in_cluster)
        print("DEBUG: Cores in node:", cores_in_node)
    n = nodes_in_cluster * cores_in_node * 3
    # Calculate larger number M:
    return n


def get_subranges(numberofpartitions):
    """ Based on https://stackoverflow.com/questions/33878939/get-indices-of-roughly-equal-sized-chunks """
    # -9223372036854775807 ≤ token(id) ≤ 9223372036854775807
    totalsize = (2 * (2 ** 63 - 1)) + 1
    # Compute the chunk size (integer division)
    chunksize = totalsize // numberofpartitions
    # How many chunks need an extra 1 added to the size?
    remainder = totalsize - chunksize * numberofpartitions
    a = -9223372036854775807
    for i in range(numberofpartitions):
        b = a + chunksize + (i < remainder)
        # Yield the inclusive-inclusive range
        yield (a, b - 1)
        a = b


# def handle_results(response, timestamp):
#     res_cnt = 0
#     for _ in response:
#         res_cnt += 1
#     if verbosity.get():
#         print("DEBUG (%s, %s): Got %s results" % (os.getpid(), timestamp, res_cnt))


def get_data(subrange):
    cluster = Cluster(['127.0.0.1', ], port=9042)
    session = cluster.connect('test_keyspace')
    query = """SELECT token(id), id, ck, v1, v2 FROM example WHERE token(id) >= ? AND token(id) <= ?"""
    prepared_select = session.prepare(query)
    if verbosity.get():
        print("DEBUG (%s): %s %s" % (os.getpid(), query, (subrange[0], subrange[1])))

    resultset = session.execute(prepared_select, (subrange[0], subrange[1]))
    # future = session.execute_async(prepared_select, (subrange[0], subrange[1]))
    # future.add_callback(handle_results, time.time())
    res_cnt = 0
    for _ in resultset:
        res_cnt += 1
    # It's easier to convert into list but it's memory-consuming in case of large resultset:
    # res_cnt = len(list(resultset))
    if verbosity.get():
        print("DEBUG (%s): Got %s results" % (os.getpid(), res_cnt))

    return res_cnt


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--num-queries", help="Number of parallel queries to execute",
                        type=int, dest="num_q", required=False)
    parser.add_argument("-d", "--verbosity", help="increase output verbosity", action="store_true")
    options = parser.parse_args()
    verbosity.set(options.verbosity)

    if options.num_q:
        n = options.num_q
    else:
        n = get_n()

    # Client-side parallelism:
    pool = Pool()  # If you leave it blank, it will default to the number of Cores in your machine.

    # Server-side parallelism:
    results = pool.map(get_data, get_subranges(n * 100))
    # close the pool and wait for the work to finish
    pool.close()
    pool.join()

    print("Scanned %s rows" % (sum(results), ))
