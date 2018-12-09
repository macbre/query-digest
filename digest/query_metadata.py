"""
Given SQL query returns its type and tables involved
"""
import re

from sql_metadata import get_query_tables


def get_query_metadata(query):
    """
    :type query: string
    :rtype list
    :raises ValueError
    """
    kind = query.split(' ')[0].upper()  # SELECT, INSERT, UPDATE, ...

    # print(kind, query, get_query_tables(query))

    # SET sql_big_selects=N
    if kind in ['BEGIN', 'COMMIT', 'SHOW', 'SET']:
        return kind, None

    if kind == 'SELECT':
        return kind, tuple(get_query_tables(query))

    try:
        # INSERT INTO, DELETE FROM, INSERT OVERWRITE TABLE
        matches = re.search(r'(FROM|INTO|TABLE) ([`,.\w]+)', query, flags=re.IGNORECASE)

        # multi-table SELECTS
        # SELECT * FROM foo,bar,test
        tables = matches.group(2).split(',')

        # table names cleanup
        tables = [table.replace('`', '') for table in tables]

        return kind, tuple(tables)
    except AttributeError:
        pass

    try:
        # UPDATE foo SET ...
        matches = re.search(r'([`\w]+) SET', query, flags=re.IGNORECASE) \
            if kind == 'UPDATE' else None

        return kind, (matches.group(1).strip('`'),)
    except AttributeError:
        pass

    raise ValueError('Could not get metadata for ' + query)
