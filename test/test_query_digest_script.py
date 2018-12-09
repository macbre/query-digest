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
