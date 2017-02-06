import logging

from collections import OrderedDict

from wikia.common.kibana import Kibana

from .helpers import generalize_sql


def get_sql_queries(path, limit=3000000):  # 3 mm
    """
    Get MediaWiki SQL queries made in the last 24h from a given code path

    Please note that SQL queries log is sampled at 1%

    :type path str
    :type limit int
    :rtype tuple
    """
    logger = logging.getLogger('get_sql_queries')
    source = Kibana(period=Kibana.DAY)
    # source = Kibana(period=3600)  # last hour

    query = 'appname: "mediawiki" AND @fields.datacenter: "sjc" AND @fields.environment: "prod" ' + \
            'AND @message: "^SQL" AND @exception.trace: "{}"'.format(path)

    logger.info('Query: "{}"'.format(query))

    matches = source.query_by_string(query, limit)

    return tuple(matches)


def normalize_query_log_entry(entry):
    """
    Normalizes given query log entry and keeps only needed fields

    :type entry dict
    :return: dict
    """
    context = entry.get('@context', {})
    fields = entry.get('@fields', {})

    res = OrderedDict()

    res['query'] = generalize_sql(entry.get('@message').replace('SQL ', ''))
    res['method'] = context.get('method')
    res['dbname'] = 'local' if fields.get('wiki_dbname') == context.get('db_name') else context.get('db_name')

    res['source_host'] = entry.get('@source_host').split('-')[0]  # e.g. ap / cron / task

    res['rows'] = int(context.get('num_rows', 0))
    res['time'] = float(1000. * context.get('elapsed', 0))  # [ms]

    return res


def filter_query(entry):
    """
    Filter out transactions

    :type entry dict
    :rtype bool
    """
    query = entry['query']

    return 'BEGIN' not in query and 'COMMIT' not in query and 'SHOW SLAVE STATUS' not in query \
        and not query.startswith('Important table write')
