from pytest import raises

from os.path import dirname, join
from io import StringIO

from digest.errors import QueryDigestCommandLineError, QueryDigestReadError
from scripts.query_digest import main

fixtures_dir = join(dirname(__file__), 'fixtures')


def test_required_args():
    with raises(QueryDigestCommandLineError) as ex:
        main(arguments=dict())

    assert str(ex).endswith('QueryDigestCommandLineError: Either --file, --path or --table needs to be provided')


def test_read_file_not_found():
    with raises(QueryDigestReadError) as ex:
        main(arguments={'--file': '/foo/var/not_existing.sql'})

    assert str(ex).endswith(
        "QueryDigestReadError: [Errno 2] "
        "No such file or directory: '/foo/var/not_existing.sql'"
    )


def test_read_file_table():
    out = StringIO()
    main(arguments={'--file': join(fixtures_dir, 'queries.sql')}, output=out)

    print(out.getvalue())
    assert 'test/fixtures/queries.sql" file, found 3 queries' in out.getvalue()
    assert 'SELECT foo FROM bar WHERE foo = N;' in out.getvalue()
    assert 'get_items.sql' in out.getvalue()


def test_read_file_data_flow():
    out = StringIO()
    main(arguments={'--file': join(fixtures_dir, 'queries.sql'), '--data-flow': True}, output=out)

    print(out.getvalue())
    assert 'test/fixtures/queries.sql" file, found 3 queries' in out.getvalue()
    assert 'db:bar\t4d9ef9d7\t4d9ef9d7\t1.00' in out.getvalue()
    assert 'db:bar\tget_items.sql\tget_items.sql\t0.50' in out.getvalue()


def test_read_hive_sql_data_flow():
    out = StringIO()
    main(arguments={'--file': join(fixtures_dir, 'hive.sql'), '--data-flow': True}, output=out)

    print(out.getvalue())
    assert 'test/fixtures/hive.sql" file, found 2 queries' in out.getvalue()
    assert 'db:rollup_wiki_beacon_pageviews\thive_01_select\thive_01_select\t1.00' in out.getvalue()
    assert 'statsdb:dimension_wikis\thive_01_select\thive_01_select\t1.00' in out.getvalue()
