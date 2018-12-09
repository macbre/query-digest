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
