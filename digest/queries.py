"""
Fetch SQL queries from elasticsearch
"""
import logging
import re

from collections import OrderedDict
from hashlib import md5
from elasticsearch_query import ElasticsearchQuery
from sql_metadata import generalize_sql, remove_comments_from_sql

from digest.errors import QueryDigestReadError

QUERIES_LIMIT = 50000
LOGS_ES_HOST = 'logs-prod.es.service.sjc.consul'


def get_sql_queries_by_file(file_path):
    """
    Get normalized log entries from provided file

    :type file_path str
    :rtype tuple
    """
    try:
        with open(file_path, 'rt') as handler:
            lines = handler.readlines()
    except Exception as ex:
        raise QueryDigestReadError(ex)

    def wrap_query(sql):
        """
        :type sql str
        :rtype: str
        """
        comment = re.match(r'/\*([^*]+)\*/', sql)
        if comment:
            comment = str(comment.group(1)).strip()

        normalized_sql = generalize_sql(sql.strip())
        sql_hash = md5(normalized_sql.encode('utf8')).hexdigest()[0:8]

        return {
            'query': normalized_sql,
            # use comment extracted from SQL or
            # a short md5 hash of normalized SQL
            'method': comment or sql_hash,
            'source_host': sql_hash,
        }

    return [
        wrap_query(line) for line in lines
        # filter out lines with SQL commands (-- foo) and empty ones
        if not line.startswith('--') and line != '\n'
    ]


def get_log_entries(query, period, fields, limit, index_prefix='logstash-other'):
    """
    Get log entries from elasticsearch that match given query

    :type query str
    :type period int
    :type fields list[str] or None
    :type limit int
    :type index_prefix str
    :rtype tuple
    """
    logger = logging.getLogger('get_log_entries')
    source = ElasticsearchQuery(es_host=LOGS_ES_HOST, period=period, index_prefix=index_prefix)

    logger.info('Query: \'%s\' for the last %d hour(s)', query, period / 3600)

    return source.query_by_string(query, fields, limit)


def get_sql_queries_by_path(path, limit=QUERIES_LIMIT, period=3600):
    """
    Get MediaWiki SQL queries made in the last hour from a given code path

    Please note that SQL queries log is sampled at 5%

    :type path str
    :type limit int
    :type period int
    :rtype tuple
    """
    query = '@fields.datacenter: "sjc" AND @fields.environment: "prod" ' \
            'AND @exception.trace: "{}"'.format(path)

    fields = [
        '@message',
        '@context.method',
        '@context.db_name',
        '@context.server_role',
        '@context.num_rows',
        '@context.elapsed',
        '@fields.wiki_dbname',
        '@source_host',
    ]

    entries = get_log_entries(query, period, fields, limit, index_prefix='logstash-mediawiki-sql')

    return tuple(map(normalize_mediawiki_entry, entries))


def get_sql_queries_by_table(table, limit=QUERIES_LIMIT, period=3600):
    """
    Get MediaWiki SQL queries made in the last hour affecting given table

    Please note that SQL queries log is sampled at 5%

    :type table str
    :type limit int
    :type period int
    :rtype tuple
    """
    query = '@fields.datacenter: "sjc" AND @fields.environment: "prod" ' \
            'AND @message: "{}"'.format(table)

    fields = [
        '@message',
        '@context.method',
        '@context.db_name',
        '@context.server_role',
        '@context.num_rows',
        '@context.elapsed',
        '@fields.wiki_dbname',
        '@source_host',
    ]

    entries = get_log_entries(query, period, fields, limit, index_prefix='logstash-mediawiki-sql')

    return tuple(map(normalize_mediawiki_entry, entries))


def get_backend_queries_by_table(table, limit=QUERIES_LIMIT, period=3600):
    """
    Get Perl backend SQL queries made in the last hour affecting given table

    Please note that this SQL queries log is not sampled!

    :type table str
    :type limit int
    :type period int
    :rtype tuple
    """
    query = 'program:"backend" AND @context.statement: * AND @context.statement: "{}"'.format(table)

    fields = [
        '@message',
        '@context.method',
        '@context.db_name',
        '@context.server_role',
        '@context.num_rows',
        '@context.elapsed',
        '@source_host',
    ]

    entries = get_log_entries(query, period, fields, limit, index_prefix='logstash-backend-sql')

    return tuple(map(normalize_backend_entry, entries))


def get_sql_queries_by_database(database, limit=QUERIES_LIMIT, period=3600):
    """
    Get MediaWiki SQL queries made in the last hour affecting given database

    Please note that SQL queries log is sampled at 5%

    :type database str
    :type limit int
    :type period int
    :rtype tuple
    """
    query = '@fields.datacenter: "sjc" AND @fields.environment: "prod"' \
            ' AND @context.db_name:"{}"'.format(database)

    fields = [
        '@message',
        '@context.method',
        '@context.db_name',
        '@context.server_role',
        '@context.num_rows',
        '@context.elapsed',
        '@fields.wiki_dbname',
        '@source_host',
    ]

    entries = get_log_entries(query, period, fields, limit, index_prefix='logstash-mediawiki-sql')

    return tuple(map(normalize_mediawiki_entry, entries))


def get_backend_queries_by_database(database, limit=QUERIES_LIMIT, period=3600):
    """
    Get Perl backend SQL queries made in the last hour affecting given database

    Please note that this SQL queries log is not sampled!

    :type database str
    :type limit int
    :type period int
    :rtype tuple
    """
    query = 'program:"backend" AND @context.statement: * AND @context.db_name:"{}"'.format(database)

    fields = [
        '@message',
        '@context.method',
        '@context.db_name',
        '@context.server_role',
        '@context.num_rows',
        '@context.elapsed',
        '@source_host',
    ]

    entries = get_log_entries(query, period, fields, limit, index_prefix='logstash-backend-sql')

    return tuple(map(normalize_backend_entry, entries))


def get_sql_queries_by_service(service, limit=25000, period=3600):
    """
    Get Pandora SQL queries made by a given service

    Please note that SQL queries log is sampled at 1%

    :type service str
    :type limit int
    :type period int
    :rtype tuple
    """
    query = 'logger_name:"query-log-sampler" AND env: "prod" AND raw_query: *'

    entries = get_log_entries(
        query=query,
        period=period,
        fields=[
            'raw_query',
            'container_name',
            'kubernetes.host',
            'rows_number',
            'execution_time',
        ],
        limit=limit,
        index_prefix='logstash-{}'.format(service)
    )

    return tuple(map(normalize_pandora_entry, entries))


def normalize_mediawiki_entry(entry):
    """
    Normalizes given MediaWiki query log entry and keeps only needed fields

    :type entry dict
    :return: dict
    """
    context = entry.get('@context', {})
    fields = entry.get('@fields', {})

    res = OrderedDict()

    res['original_query'] = remove_comments_from_sql(entry.get('@message'))
    res['query'] = generalize_sql(entry.get('@message'))

    # e.g. WikiFactory::loadVariableFromDB (from foo::bar)
    res['method'] = re.sub(r'\s\(([^)]+)\)', '', context.get('method'))

    res['dbname'] = 'local' if fields.get('wiki_dbname') == context.get('db_name') \
        else context.get('db_name')
    res['from_master'] = context.get('server_role', 'slave') == 'master'

    res['source_host'] = entry.get('@source_host', 'ap').split('-')[0]  # e.g. ap / cron / task

    res['rows'] = int(context.get('num_rows', 0))
    res['time'] = float(1000. * context.get('elapsed', 0))  # [ms]

    return res


def normalize_backend_entry(entry):
    """
    Normalizes given backend query log entry and keeps only needed fields

    :type entry dict
    :return: dict
    """
    context = entry.get('@context', {})

    res = OrderedDict()

    res['original_query'] = remove_comments_from_sql(entry.get('@message'))
    res['query'] = generalize_sql(entry.get('@message'))
    res['method'] = context.get('method')  # e.g. "DB.pm line 171 via phalanx_stats.pl line 158"
    res['dbname'] = context.get('db_name')  # e.g. "specials"
    res['from_master'] = context.get('server_role', 'slave') == 'master'

    res['source_host'] = entry.get('@source_host').split('-')[0]  # e.g. job / task

    res['rows'] = int(context.get('num_rows', 0))
    res['time'] = float(1000. * context.get('elapsed', 0))  # [ms]

    return res


def normalize_pandora_entry(entry):
    """
    Normalizes given Pandora query log entry and keeps only needed fields

    logger_name: "query-log-sampler"

    :type entry dict
    :return: dict
    """
    res = OrderedDict()

    query = entry.get('raw_query')
    k8s = entry.get('kubernetes', {})

    res['original_query'] = remove_comments_from_sql(query)
    res['query'] = generalize_sql(query)
    # res['method'] = k8s.get('container_name')  # e.g liftigniter-metadata
    res['dbname'] = entry.get('container_name')  # TODO: implement in Pandora

    res['source_host'] = k8s.get('host').split('-')[0]  # e.g. k8s-worker-s3 -> k8

    res['rows'] = int(entry.get('rows_number', 0))
    res['time'] = float(entry.get('execution_time', 0))  # [ms]

    # use a short md5 hash of normalized SQL method to generate the method name
    res['method'] = md5(res['query'].encode('utf8')).hexdigest()[0:8]

    return res


def filter_query(entry):
    """
    Filter out transactions

    :type entry dict
    :rtype bool
    """
    query = entry['query']

    return 'BEGIN' not in query and 'COMMIT' not in query and not query.startswith('SHOW') \
        and not query.startswith('Important table write')
