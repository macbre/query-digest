import re


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
