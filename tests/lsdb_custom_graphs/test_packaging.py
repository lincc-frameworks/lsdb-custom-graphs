import lsdb_custom_graphs


def test_version():
    """Check to see that we can get the package version"""
    assert lsdb_custom_graphs.__version__ is not None
