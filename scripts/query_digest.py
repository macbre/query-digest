from __future__ import print_function

import logging

from tabulate import tabulate

from digest.map_reduce import map_reduce
from digest.math import median
from digest.queries import get_sql_queries, normalize_query_log_entry, filter_query


def queries_reduce(_, values):
    """
    :type _ str
    :type values tuple[dict]
    :rtype dict
    """
    ret = values[0].copy()

    ret['count'] = len(values)

    # calculate times stats
    times = [value.get('time') for value in values]

    ret['time_sum'] = sum(times)
    ret['time_median'] = median(times)

    # rows stats
    rows = [value.get('rows') for value in values]

    ret['rows_sum'] = sum(rows)
    ret['rows_median'] = median(rows)

    # get rid of item specific fields
    del ret['rows']
    del ret['time']

    return ret


def main(path='/extensions/wikia/Wall'):
    logger = logging.getLogger('query_digest')
    logger.info('Digesting queries for "{}" path'.format(path))

    queries = tuple(map(normalize_query_log_entry, get_sql_queries(path)))
    queries = tuple(filter(filter_query, queries))

    logger.info('Processing {} queries...'.format(len(queries)))

    results = map_reduce(
        queries,
        map_func=lambda item: '{}-{}'.format(item.get('method'), item.get('source_host')),
        reduce_func=queries_reduce
    )

    logger.info('Got {} kinds of queries'.format(len(results)))

    # sort the results ordered by "time_sum" descending
    results_ordered = sorted(results, key=lambda (_, item): item['time_sum'], reverse=True)
    data = [entry for (_, entry) in results_ordered]

    # @see https://pypi.python.org/pypi/tabulate
    print(tabulate(data, headers='keys', tablefmt='grid'))
    print('Note: times are in [ms], queries are normalized')
