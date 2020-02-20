#!/usr/bin/env python3
""" Based on https://www.scylladb.com/2017/02/13/efficient-full-table-scans-with-scylla-1-6/
    and https://www.datastax.com/blog/2015/06/datastax-python-driver-multiprocessing-example-improved-bulk-data-throughput """

import argparse
import itertools
import time
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import tuple_factory
from multiprocessing import Pool


def get_n():
    """ Calculate "Parallel queries" """
    cluster = Cluster(['127.0.0.1', ], port=9042)
    _ = cluster.connect()
    nodes_in_cluster = len(cluster.metadata.all_hosts())
    # TODO (asmirnov): set cores_in_node value based on "num_procs" described here:
    # https://docs.datastax.com/en/opscenter/6.1/api/docs/cluster_info.html#response-node
    cores_in_node = 2
    n = nodes_in_cluster * cores_in_node * 3

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


class QueryManager(object):
    concurrency = 100

    def __init__(self, cluster, process_count=None):
        self.pool = Pool(processes=process_count, initializer=self._setup, initargs=(cluster,))

    @classmethod
    def _setup(cls, cluster):
        cls.session = cluster.connect('test_keyspace2')
        cls.session.row_factory = tuple_factory
        cls.prepared = cls.session.prepare(
            'SELECT token(id), id, ck, v1, v2 FROM example WHERE token(id) >= ? AND token(id) <= ?')

    def close_pool(self):
        self.pool.close()
        self.pool.join()

    def get_results(self, params):
        params = list(params)
        results = self.pool.map(_multiprocess_get, (params[n:n + self.concurrency] for n in range(0, len(params), self.concurrency)))
        return list(itertools.chain(*results))

    @classmethod
    def _results_from_concurrent(cls, params):
        return [len(results) for results in execute_concurrent_with_args(cls.session, cls.prepared, params)]


def _multiprocess_get(params):
    return QueryManager._results_from_concurrent(params)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--num-iterations", help="Number of iterations",
                        type=int, dest="iterations", required=True)
    parser.add_argument("-p", "--num-processes", help="Number of processes",
                        type=int, dest="processes", required=False)
    options = parser.parse_args()

    cluster = Cluster(['127.0.0.1', ], port=9042)
    qm = QueryManager(cluster, options.processes)

    start = time.time()
    rows = qm.get_results(get_subranges(options.iterations))
    delta = time.time() - start
    print("%d queries in %.2f seconds (%.2f/s)" % (options.iterations, delta, options.iterations / delta))
