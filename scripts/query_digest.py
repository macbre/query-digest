# pylint: disable=too-many-locals,too-many-branches,too-many-statements,redefined-builtin
"""
This is a dynamic code analysis tool that processes SQL queries logs
and reports those made by given feature or using given table

Usage:
  query_digest [ --file=<file> ] [ --path=<path> ] [ --table=<table> ] [ --service=<service> ]
    [ --database=<database> ] [ --csv ] [ --data-flow ] [ --simple ] [ --sql-log ] [ --last-24h ]

Example:
  query_digest --file=/var/log/queries.log

  query_digest --path=extensions/wikia/Wall
  query_digest --path=extensions/wikia/Wall --csv
  query_digest --path=extensions/wikia/Wall --last-24h

  query_digest --table=wall_notification
  query_digest --table=wall_notification --csv
  query_digest --table=image_view --data-flow

  query_digest --service=liftigniter-metadata
  query_digest --service=liftigniter-metadata --csv

  query_digest --database=statsdb --simple
  query_digest --database=statsdb --sql-log

  query_digest --table=wall_notification --simple - simple output type (list queries only)
"""
from __future__ import print_function
import logging

from functools import reduce
from operator import itemgetter

from csv import DictWriter
from sys import stdout

import docopt
from tabulate import tabulate

from digest.dataflow import data_flow_format_entry
from digest.map_reduce import map_reduce
from digest.math import median
from digest.queries import \
    get_sql_queries_by_path, get_sql_queries_by_table, get_backend_queries_by_table,\
    get_sql_queries_by_service, get_sql_queries_by_database, get_backend_queries_by_database, \
    filter_query


def queries_reduce(_, values, sequence_len):
    """
    :type _ str
    :type values tuple[dict]
    :type sequence_len int
    :rtype dict
    """
    ret = values[0].copy()

    ret['count'] = len(values)
    ret['percentage'] = '{:.2f}%'.format(100. * ret['count'] / sequence_len)

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

    # count all queries that were made using master node
    if ret.get('from_master') is not None:
        master_queries = reduce(
            lambda acc, i: acc+1 if i.get('from_master') is True else acc,
            values,
            0  # initializer
        )
        ret['from_master'] = master_queries > 0
    else:
        ret['from_master'] = None

    # FIXME: from_master will show cases when DB_MASTER is selected because of load-balancer weights
    del ret['from_master']

    return ret


def main(arguments=None, output=stdout):
    """
    :type arguments dict
    :type output file
    """
    logger = logging.getLogger('query_digest')

    # handle command line options
    if arguments is None:
        arguments = docopt.docopt(__doc__)

    logger.info("Got the following arguments: %s", arguments)

    file = arguments.get('--file')
    path = arguments.get('--path')
    service = arguments.get('--service')
    table = arguments.get('--table')
    database = arguments.get('--database')

    output_csv = arguments.get('--csv') is True
    simple_output = arguments.get('--simple') is True
    data_flow_output = arguments.get('--data-flow') is True
    sql_log_output = arguments.get('--sql-log') is True

    period = 86400 if arguments.get('--last-24h') is True else 3600

    # period = 60  # 10 minutes # DEBUG

    if file is not None:
        logger.info('Digesting queries from "%s" file', file)
    elif path is not None:
        logger.info('Digesting queries for "%s" path', path)
    elif service is not None:
        logger.info('Digesting queries made by "%s" Pandora service', service)
    elif table is not None:
        logger.info('Digesting queries affecting "%s" table', table)
    elif database is not None:
        logger.info('Digesting queries affecting "%s" database', database)
    else:
        raise Exception('Either --path or --table needs to be provided')

    # run the reporter
    if path is not None:
        queries = get_sql_queries_by_path(path, period=period)
        report_header = '"{}" path'.format(path)
    elif service is not None:
        queries = get_sql_queries_by_service(service, period=period)
        report_header = '"{}" service'.format(service)
    elif database is not None:
        queries = get_sql_queries_by_database(database, period=period) + \
                  get_backend_queries_by_database(database, period=period)
        report_header = '"{}" database'.format(database)
    else:
        queries = get_sql_queries_by_table(table, period=period) + \
                  get_backend_queries_by_table(table, period=period)
        report_header = '"{}" table'.format(table)

    queries = tuple(filter(filter_query, queries))

    if not queries:
        raise Exception('No queries found for "{}" path'.format(path))

    logger.info('Processing %d queries from the last %d hour(s)...', len(queries), period / 3600)

    # this returns (method_name, entry_data) tuples
    results = map_reduce(
        queries,
        map_func=lambda item: '{}-{}'.format(item.get('method'), item.get('source_host')),
        reduce_func=queries_reduce
    )
    data = [entry for (_, entry) in results]

    logger.info('Got %d kinds of queries', len(data))

    # sort the results ordered by "time_sum" descending
    data = sorted(data, key=itemgetter('time_sum'), reverse=True)
    # print(data)

    report_header = 'Query digest for {}, found {} queries'.format(report_header, len(queries))

    # --csv
    if output_csv:
        writer = DictWriter(f=output, fieldnames=data[0].keys())

        output.write('# {}\n'.format(report_header))
        writer.writeheader()
        writer.writerows(data)
    # --simple
    elif simple_output:
        print(report_header)
        output.writelines([
            '{method} {percentage} [{source_host}] db:{dbname} | {query}\n'.format(**entry)
            for entry in data
        ])
    # --data-flow
    elif data_flow_output:
        max_queries = max(item.get('count') for item in data)

        output.write('# {}\n'.format(report_header))

        for item in data:
            output.writelines(data_flow_format_entry(item, max_queries))
    # --sql-log
    elif sql_log_output:
        print('-- {}'.format(report_header))
        output.writelines([
            '/* {} */ {}\n'.format(
                entry.get('method'),
                entry.get('original_query', '').replace("\n", ' ').encode('utf-8')
            ) for entry in data
        ])
    else:
        # @see https://pypi.python.org/pypi/tabulate
        print(report_header)
        print(tabulate(data, headers='keys', tablefmt='grid'))
        print('Note: times are in [ms], queries are normalized')
