import logging
import re

logger = logging.getLogger('dataflow')


def get_query_metadata(query):
    """
    TODO: use https://pypi.python.org/pypi/sqlparse

    :type query: string
    :rtype list
    :raises ValueError
    """
    kind = query.split(' ')[0].upper()  # SELECT, INSERT, UPDATE, ...

    # SET sql_big_selects=N
    if kind in ['BEGIN', 'COMMIT', 'SHOW', 'SET']:
        return kind, None

    try:
        # SELECT FROM, INSERT INTO, DELETE FROM
        matches = re.search(r'(FROM|INTO) ([`\w]+)', query, flags=re.IGNORECASE)
        return kind, (matches.group(2).strip('`'),)
    except:
        pass

    try:
        # UPDATE foo SET ...
        matches = re.search(r'([`\w]+) SET', query, flags=re.IGNORECASE) if kind == 'UPDATE' else None
        return kind, (matches.group(1).strip('`'),)
    except:
        pass

    raise ValueError('Could not get metadata for ' + query)


def data_flow_format_entry(entry, max_queries):
    """
    Generate an entry for data-flow TSV file:

    (source node)\t(edge label)\t(target node)\t(edge weight - optional)\t(metadata for tooltip)

    @see https://github.com/macbre/data-flow-graph

    :type entry: dict
    :type max_queries: int
    :rtype string
    """
    # OrderedDict([('query', 'SELECT wiki_id,page_id FROM `image_review` WHERE wiki_id = X'),
    # ('method', u'UpdateImageReview::getImageReviewFiles'),
    # ('dbname', u'dataware'),
    # ('source_host', u'cron'),
    # ('count', 172),
    # ('percentage', '45.38%'),
    # ('time_sum', 102.78344154357926),
    # ('time_median', 0.37848949432372997),
    # ('rows_sum', 5405),
    # ('rows_median', 8.0)]) 379
    # print(entry, max_queries)

    # get query metadata (kind and tables involved
    query = entry.get('query')

    try:
        (kind, tables) = get_query_metadata(query)
    except ValueError:
        logger.error('Unable to parse query metadata: ' + query, exc_info=True)
        return ''

    # do not include BEGIN, COMMIT and STATUS queries
    if tables is None:
        return ''

    # for SELECT queries of source is the database and the target is the code class
    source = '{db}:{table}'.format(db=entry.get('dbname'), table=tables[0])

    # parse caller name and extract class and method name
    method = entry.get('method')

    try:
        if ' via ' in method:
            # handle Perl method names: "DB.pm line 238 via fiximagereview.pl line 123"
            matches = re.search(r' via ([^\s]+) line (\d+)', method)

            target = 'backend:{}'.format(matches.group(1))  # backend:fiximagereview.pl
            edge = '{}:{} ({})'.format(matches.group(1), matches.group(2), kind)  # fiximagereview.pl:123 (SELECT)
        else:
            # PHP method names (Foo::get_bar / FavoriteWikisModel:getTopWikisFromDb)
            (target, edge) = method.rsplit('::' if '::' in method else ':', 1)
    except ValueError:
        logger.error('Unable to parse method name: ' + method, exc_info=True)
        return ''

    # for writing queries reverse the order
    if kind != 'SELECT':
        (source, target) = (target, source)

    return '{source}\t{edge}\t{target}\t{weight:.2f}\t{metadata}\n'.format(
        source=source,
        edge=edge,
        target=target,
        weight=1. * entry.get('count') / max_queries,
        metadata='{at}, median time: {time:.2f} ms, count: {count}'.format(
            at=entry.get('source_host'),  # cron, ap, ...
            time=entry.get('time_median') * 100.,
            count=entry.get('count') * 100  # multiply for 1% logs sampling
        )
    )
