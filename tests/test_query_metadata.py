from __future__ import unicode_literals

import unittest
from digest.query_metadata import get_query_metadata


class TestQueryMetadata(unittest.TestCase):
    """
    Unit tests for get_query_metadata
    """
    def test_get_query_metadata(self):
        # SELECTs
        assert get_query_metadata(
            'SELECT * FROM `comments_index` WHERE comment_id = X LIMIT N') == \
            ('SELECT', ('comments_index',))

        assert get_query_metadata(
            'select * from `Comments_index` where comment_id = X limit N') == \
            ('SELECT', ('Comments_index',))

        assert get_query_metadata(
            'SELECT props FROM `page_wikia_props` WHERE page_id = X AND propname = X') == \
            ('SELECT', ('page_wikia_props',))

        # multi-table queries
        assert get_query_metadata(
            'SELECT props FROM `page_wikia_props`,`foo`,`bar` WHERE page_id = X AND propname = X') == \
            ('SELECT', ('page_wikia_props', 'foo', 'bar'))

        assert get_query_metadata(
            'SELECT rev_id,rev_page,rev_text_id,rev_timestamp,rev_comment,rev_user_text,rev_user,rev_minor_edit,rev_deleted,rev_len,rev_parent_id,rev_shaN,page_namespace,page_title,page_id,page_latest,user_name FROM `revision` INNER JOIN `page` ON ((page_id = rev_page)) LEFT JOIN `wikicities_cN`.`user` ON ((rev_user != N) AND (user_id = rev_user)) WHERE rev_id = X LIMIT N') == \
            ('SELECT', ('revision', 'page', 'wikicities_cN.user'))

        assert get_query_metadata(
            'SELECT comments_index.comment_id, count(*) as cnt, last_child_comment_id  FROM `wall_related_pages`,`comments_index`  WHERE page_id = N AND removed = N AND (wall_related_pages.comment_id = comments_index.comment_id)  GROUP BY comments_index.comment_id ORDER BY last_update desc LIMIT 2 ') == \
            ('SELECT', ('wall_related_pages', 'comments_index'))

        # UPDATEs, DELETEs, INSERTs, REPLACEs
        assert get_query_metadata(
            'UPDATE `page` SET page_touched = X WHERE page_id = X') == \
            ('UPDATE', ('page',))

        assert get_query_metadata(
            'INSERT INTO `wall_history` (parent_page_id,post_user_id) VALUES (X,X)') == \
            ('INSERT', ('wall_history',))

        assert get_query_metadata(
            'SELECT * FROM `comments_index` WHERE comment_id = X LIMIT N') == \
            ('SELECT', ('comments_index',))

        # BEGIN, COMMIT, ...
        assert get_query_metadata(
            'BEGIN') == \
            ('BEGIN', None)

        assert get_query_metadata(
            'COMMIT') == \
            ('COMMIT', None)

        assert get_query_metadata(
            'SHOW SLAVE STATUS') == \
            ('SHOW', None)

        # invalid queries
        self.assertRaises(ValueError, get_query_metadata, 'SELECT INSERT')
        self.assertRaises(ValueError, get_query_metadata, 'FOO BAR')
        self.assertRaises(ValueError, get_query_metadata, 'UPDATE BAR')
