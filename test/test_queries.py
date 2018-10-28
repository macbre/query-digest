from digest.queries import filter_query


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
