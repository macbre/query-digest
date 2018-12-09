from os.path import dirname, join
from digest.queries import filter_query, get_sql_queries_by_file

fixtures_dir = join(dirname(__file__), 'fixtures')


def test_filter_query():
    queries = (
        'SELECT foo FROM bar',
        'UPDATE foo SET bar = "123" WHERE id = 1',
    )

    for query in queries:
        assert filter_query({'query': query}), 'Query "%s" should not be filtered out' % query


def test_filter_out_query():
    queries = (
        'BEGIN',
        'COMMIT',
        'SHOW TABLES',
        'SHOW SLAVES STATUS',
        'Important table write: UPDATE foo SET bar = "123" WHERE id = 1',
    )

    for query in queries:
        assert filter_query({'query': query}) is False, 'Query "%s" should be filtered out' % query


def test_read_file():
    queries = get_sql_queries_by_file(file_path=fixtures_dir + '/queries.sql')

    print(queries)
    assert len(queries) == 3
    assert queries[0]['query'] == 'SELECT foo FROM bar WHERE foo = N;'
    assert queries[1]['query'] == 'SELECT foo FROM bar WHERE foo = N;'
    assert queries[2]['query'] == 'SELECT foo FROM bar ORDER BY foo LIMIT N;'

    assert queries[0]['method'] == '4d9ef9d7'
    assert queries[1]['method'] == '4d9ef9d7'
    assert queries[2]['method'] == 'get_items.sql'  # extracted from SQL query comment
