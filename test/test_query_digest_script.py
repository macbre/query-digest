from pytest import raises

from scripts.query_digest import main


def test_required_args():
    with raises(Exception) as ex:
        main(arguments=dict())

    assert str(ex).endswith('Either --path or --table needs to be provided')
