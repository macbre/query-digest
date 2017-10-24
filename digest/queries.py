import logging
import re

from collections import OrderedDict
from hashlib import md5

from .helpers import generalize_sql
from .kibana import Kibana


def get_log_entries(query, period, limit, index_prefix='logstash-other'):
    """
    Get log entries from elasticsearch that match given query

    :type query str
    :type period int
    :type limit int
    :type index_prefix str
    :rtype tuple
    """
    logger = logging.getLogger('get_log_entries')
    source = Kibana(period=period, index_prefix=index_prefix)

    logger.info('Query: \'{}\' for the last {} hour(s)'.format(query, period / 3600))

    return source.query_by_string(query, limit)


def get_sql_queries_by_path(path, limit=500000, period=3600):
    """
    Get MediaWiki SQL queries made in the last hour from a given code path

    Please note that SQL queries log is sampled at 5%

    :type path str
    :type limit int
    :type period int
    :rtype tuple
    """
    query = '@fields.datacenter: "sjc" AND @fields.environment: "prod" AND @exception.trace: "{}"'.format(path)

    entries = get_log_entries(query, period, limit, index_prefix='logstash-mediawiki-sql')

    return tuple(map(normalize_mediawiki_query_log_entry, entries))


def get_sql_queries_by_table(table, limit=500000, period=3600):
    """
    Get MediaWiki SQL queries made in the last hour affecting given table

    Please note that SQL queries log is sampled at 5%

    :type table str
    :type limit int
    :type period int
    :rtype tuple
    """
    query = '@fields.datacenter: "sjc" AND @fields.environment: "prod" AND @message: "{}"'.format(table)

    entries = get_log_entries(query, period, limit, index_prefix='logstash-mediawiki-sql')

    return tuple(map(normalize_mediawiki_query_log_entry, entries))


def get_backend_queries_by_table(table, limit=500000, period=3600):
    """
    Get Perl backend SQL queries made in the last hour affecting given table

    Please note that this SQL queries log is not sampled!

    :type table str
    :type limit int
    :type period int
    :rtype tuple
    """
    query = 'program:"backend" AND @context.statement: * AND @context.statement: "{}"'.format(table)

    entries = get_log_entries(query, period, limit, index_prefix='logstash-backend-sql')

    return tuple(map(normalize_backend_query_log_entry, entries))


def get_sql_queries_by_database(database, limit=500000, period=3600):
    """
    Get MediaWiki SQL queries made in the last hour affecting given database

    Please note that SQL queries log is sampled at 5%

    :type database str
    :type limit int
    :type period int
    :rtype tuple
    """
    query = '@fields.datacenter: "sjc" AND @fields.environment: "prod" AND @context.db_name:"{}"'.format(database)

    entries = get_log_entries(query, period, limit, index_prefix='logstash-mediawiki-sql')

    return tuple(map(normalize_mediawiki_query_log_entry, entries))


def get_backend_queries_by_database(database, limit=500000, period=3600):
    """
    Get Perl backend SQL queries made in the last hour affecting given database

    Please note that this SQL queries log is not sampled!

    :type database str
    :type limit int
    :type period int
    :rtype tuple
    """
    query = 'program:"backend" AND @context.statement: * AND @context.db_name:"{}"'.format(database)

    entries = get_log_entries(query, period, limit, index_prefix='logstash-backend-sql')

    return tuple(map(normalize_backend_query_log_entry, entries))


def get_sql_queries_by_service(service, limit=500000, period=3600):
    """
    Get Pandora SQL queries made by a given service

    Please note that SQL queries log is sampled at 1%

    :type service str
    :type limit int
    :type period int
    :rtype tuple
    """
    query = 'appname:"{}" AND logger_name:"query-log-sampler" AND env: "prod"'.format(service)

    entries = get_log_entries(
        query=query,
        period=period,
        limit=limit
    )

    return tuple(map(normalize_pandora_query_log_entry, entries))


def normalize_mediawiki_query_log_entry(entry):
    """
    Normalizes given MediaWiki query log entry and keeps only needed fields

    :type entry dict
    :return: dict
    """
    context = entry.get('@context', {})
    fields = entry.get('@fields', {})

    res = OrderedDict()

    res['query'] = generalize_sql(entry.get('@message').replace('SQL ', ''))

    # e.g. WikiFactory::loadVariableFromDB (from DesignSystemGlobalNavigationModel:isWikiaOrgCommunity)
    res['method'] = re.sub(r'\s\(([^\)]+)\)', '', context.get('method'))

    res['dbname'] = 'local' if fields.get('wiki_dbname') == context.get('db_name') else context.get('db_name')
    res['from_master'] = context.get('server_role', 'slave') == 'master'

    res['source_host'] = entry.get('@source_host').split('-')[0]  # e.g. ap / cron / task

    res['rows'] = int(context.get('num_rows', 0))
    res['time'] = float(1000. * context.get('elapsed', 0))  # [ms]

    return res


def normalize_backend_query_log_entry(entry):
    """
    Normalizes given backend query log entry and keeps only needed fields

    :type entry dict
    :return: dict
    """
    context = entry.get('@context', {})

    res = OrderedDict()

    res['query'] = generalize_sql(entry.get('@message').replace('SQL ', ''))
    res['method'] = context.get('method')  # e.g. "DB.pm line 171 via phalanx_stats.pl line 158"
    res['dbname'] = context.get('db_name')  # e.g. "specials"
    res['from_master'] = context.get('server_role', 'slave') == 'master'

    res['source_host'] = entry.get('@source_host').split('-')[0]  # e.g. job / task

    res['rows'] = int(context.get('num_rows', 0))
    res['time'] = float(1000. * context.get('elapsed', 0))  # [ms]

    return res


def normalize_pandora_query_log_entry(entry):
    """
    Normalizes given Pandora query log entry and keeps only needed fields

    :type entry dict
    :return: dict
    """
    res = OrderedDict()

    res['query'] = generalize_sql(entry.get('rawMessage'))
    res['method'] = entry.get('thread_name')  # TODO: implement in Pandora
    res['dbname'] = entry.get('appname')  # TODO: implement in Pandora

    res['source_host'] = entry.get('@source_host').split('-')[0]  # e.g. mesos

    res['rows'] = int(entry.get('numRows', 0))
    res['time'] = float(entry.get('execTimeMs', 0))  # [ms]

    # use a short md5 hash of normalized SQL method to generate the method name
    res['method'] = md5(res['query']).hexdigest()[0:8]

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
