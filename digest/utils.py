from collections import OrderedDict

from wikia.common.kibana import Kibana

from .helpers import generalize_sql


def get_sql_queries(path, limit=100000):
    """
    Get MediaWiki SQL queries made in the last 24h from a given code path

    Please note that SQL queries log is sampled at 1%

    :type path str
    :type limit int
    :rtype tuple
    """
    source = Kibana(period=Kibana.DAY)
    # source = Kibana(period=3600)  # last hour

    matches = source.query_by_string(
        query='appname: "mediawiki" AND @message: "^SQL" AND @exception.trace: "{}"'.format(path),
        limit=500000
    )

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
