from pytest import raises

from digest.errors import QueryDigestCommandLineError
from scripts.query_digest import main


def test_required_args():
    with raises(QueryDigestCommandLineError) as ex:
        main(arguments=dict())

    assert str(ex).endswith('QueryDigestCommandLineError: Either --file, --path or --table needs to be provided')


def test_read_file_not_found():
    with raises(FileNotFoundError):
        main(arguments={'--file': '/foo/var/not_existing.sql'})
