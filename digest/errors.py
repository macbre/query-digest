"""
Exceptions for query_digest
"""


class QueryDigestError(Exception):
    """
    Base exception
    """
    pass


class QueryDigestCommandLineError(QueryDigestError):
    """
    This one is thrown by command line tool
    """
    pass


class QueryDigestReadError(QueryDigestError):
    """
    This one is thrown when reading from log source fails
    """
    pass
