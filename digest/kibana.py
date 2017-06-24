"""
Run queries against Kibana's elasticsearch
@see http://elasticsearch-py.readthedocs.org/en/master/
"""
import json
import logging
import time

from datetime import datetime
from dateutil import tz
from itertools import islice

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


class KibanaError(Exception):
    pass


class Kibana(object):
    # give 5 seconds for all log messages to reach logstash and be stored in elasticsearch
    SHORT_DELAY = 5

    # seconds in 24h used to get the es index for yesterday
    DAY = 86400

    ELASTICSEARCH_HOST = 'logs-prod.es.service.sjc.consul'  # ES5

    """ Interface for querying Kibana's storage """
    def __init__(self, since=None, period=900, es_host=None, read_timeout=10, index_prefix='logstash-other'):
        """
        :type since int
        :type period int
        :type es_host str
        :type read_timeout int
        :type index_prefix str

        :arg since: UNIX timestamp data should be fetched since
        :arg period: period (in seconds) before now() to be used when since is empty (defaults to last 15 minutes)
        :arg es_host: customize Elasticsearch host(s) that should be used for querying
        :arg read_timeout: customize Elasticsearch read timeout (defaults to 10 s)
        """
        self._es = Elasticsearch(hosts=es_host if es_host else self.ELASTICSEARCH_HOST, timeout=read_timeout)

        self._logger = logging.getLogger('kibana')

        # if no timestamp provided, fallback to now() in UTC
        now = int(time.time())

        if since is None:
            since = now - period
        else:
            since += 1
            self._logger.info("Using provided {0} timestamp as since ({1} seconds ago)".format(since, now - since))

        self._since = since
        self._to = now - self.SHORT_DELAY  # give logs some time to reach Logstash

        # Elasticsearch index to query
        # from today and yesterday
        self._index = ','.join([
            self.format_index(index_prefix, now-self.DAY),
            self.format_index(index_prefix, now),
        ])

        self._logger.info("Using {} indices".format(self._index))
        self._logger.info("Querying for messages from between {} and {}".
                          format(self.format_timestamp(self._since), self.format_timestamp(self._to)))

    @staticmethod
    def format_index(prefix, ts):
        """
        :type prefix str
        :type ts int
        :rtype: str
        """
        tz_info = tz.tzutc()

        # ex. logstash-other-2017.05.09
        return "{prefix}-{date}".format(prefix=prefix, date=datetime.fromtimestamp(ts, tz=tz_info).strftime('%Y.%m.%d'))

    @staticmethod
    def format_timestamp(ts):
        """
        Format the UTC timestamp for Elasticsearch
        eg. 2014-07-09T08:37:18.000Z

        @see https://docs.python.org/2/library/time.html#time.strftime
        """
        tz_info = tz.tzutc()
        return datetime.fromtimestamp(ts, tz=tz_info).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def _get_timestamp_filer(self):
        return {
            "range": {
                "@timestamp": {
                    "gte": self.format_timestamp(self._since),
                    "lte": self.format_timestamp(self._to)
                }
            }
        }

    def _search(self, query, limit=50000):
        """
        Perform the search and return raw rows

        :type query object
        :type limit int
        :rtype: list
        """
        body = {
            "query": {
                "bool": {
                    "must": [
                        query
                    ]
                }
            }
        }

        # add @timestamp range
        # @see http://stackoverflow.com/questions/40996266/elasticsearch-5-1-unknown-key-for-a-start-object-in-filters
        # @see https://discuss.elastic.co/t/elasticsearch-watcher-error-for-range-query/70347/2
        body['query']['bool']['must'].append(self._get_timestamp_filer())

        self._logger.debug("Running {} query (limit set to {:d})".format(json.dumps(body), body.get('size', 0)))

        # use Scroll API to be able to fetch more than 10k results and prevent "search_phase_execution_exception":
        # "Result window is too large, from + size must be less than or equal to: [10000] but was [500000].
        # See the scroll api for a more efficient way to request large data sets."
        #
        # @see http://elasticsearch-py.readthedocs.io/en/master/helpers.html#scan
        rows = scan(
            client=self._es,
            clear_scroll=False,  # True causes "403 Forbidden: You don't have access to this resource"
            index=self._index,
            query=body,
            sort=["_doc"],  # return the next batch of results from every shard that still has results to return.
            size=1000,  # batch size
        )

        # get only requested amount of entries and cast them to a list
        rows = islice(rows, 0, limit)
        rows = [entry['_source'] for entry in rows]  # get data

        self._logger.info("{:d} rows returned".format(len(rows)))
        return rows

    def get_rows(self, match, limit=10):
        """
        Returns raw rows that matches given query

        :arg match: query to be run against Kibana log messages (ex. {"@message": "Foo Bar DB queries"})
        :arg limit: the number of results (defaults to 10)
        """
        query = {
            "match": match,
        }

        return self._search(query, limit)

    def query_by_string(self, query, limit=10):
        """
        Returns raw rows that matches the given query string

        :arg query: query string to be run against Kibana log messages (ex. @message:"^PHP Fatal").
        :arg limit: the number of results (defaults to 10)
        """
        query = {
            "query_string": {
                "query": query,
            }
        }

        return self._search(query, limit)

    def get_to_timestamp(self):
        """ Return the upper time boundary to returned data """
        return self._to
