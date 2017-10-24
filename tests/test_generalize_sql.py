# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import unittest
from digest.helpers import generalize_sql, remove_comments_from_sql


class TestGeneralizeSql(unittest.TestCase):
    """
    Unit tests for generalize_sql
    """
    def test_generalize_sql(self):
        assert generalize_sql(None) is None

        assert remove_comments_from_sql('SELECT /* Test */ foo FROM BAR') == 'SELECT foo FROM BAR'

        assert generalize_sql(
            "UPDATE  `category` SET cat_pages = cat_pages + 1,cat_files = cat_files + 1 WHERE cat_title = 'foo'") == \
            "UPDATE `category` SET cat_pages = cat_pages + N,cat_files = cat_files + N WHERE cat_title = X"

        assert generalize_sql(
            "SELECT  entity_key  FROM `wall_notification_queue`  WHERE (wiki_id = ) AND (event_date > '20150105141012')") == \
            "SELECT entity_key FROM `wall_notification_queue` WHERE (wiki_id = ) AND (event_date > X)"

        assert generalize_sql("UPDATE  `user` SET user_touched = '20150112143631' WHERE user_id = '25239755'") == \
            "UPDATE `user` SET user_touched = X WHERE user_id = X"

        assert generalize_sql(
            "SELECT /* CategoryDataService::getMostVisited 207.46.13.56 */  page_id,cl_to  FROM `page` INNER JOIN `categorylinks` ON ((cl_from = page_id))  WHERE cl_to = 'Characters' AND (page_namespace NOT IN(500,6,14))  ORDER BY page_title") == \
            "SELECT page_id,cl_to FROM `page` INNER JOIN `categorylinks` ON ((cl_from = page_id)) WHERE cl_to = X AND (page_namespace NOT IN (XYZ)) ORDER BY page_title"

        assert generalize_sql(
            "SELECT /* ArticleCommentList::getCommentList Dancin'NoViolen... */  page_id,page_title  FROM `page`  WHERE (page_title LIKE 'Dreams\_Come\_True/@comment-%' ) AND page_namespace = '1'  ORDER BY page_id DESC") == \
            "SELECT page_id,page_title FROM `page` WHERE (page_title LIKE X ) AND page_namespace = X ORDER BY page_id DESC"

        assert generalize_sql(
            "delete /* DatabaseBase::sourceFile( /usr/wikia/slot1/3690/src/maintenance/cleanupStarter.sql ) CreateWiki scri... */ from text where old_id not in (select rev_text_id from revision)") == \
            "delete from text where old_id not in (select rev_text_id from revision)"

        assert generalize_sql(
            "SELECT /* WallNotifications::getBackupData Craftindiedo */  id,is_read,is_reply,unique_id,entity_key,author_id,notifyeveryone  FROM `wall_notification`  WHERE user_id = '24944488' AND wiki_id = '1030786' AND unique_id IN ('880987','882618','708228','522330','662055','837815','792393','341504','600103','612640','667267','482428','600389','213400','620177','164442','659210','621286','609757','575865','567668','398132','549770','495396','344814','421448','400650','411028','341771','379461','332587','314176','284499','250207','231714')  AND is_hidden = '0'  ORDER BY id") == \
            "SELECT id,is_read,is_reply,unique_id,entity_key,author_id,notifyeveryone FROM `wall_notification` WHERE user_id = X AND wiki_id = X AND unique_id IN (XYZ) AND is_hidden = X ORDER BY id"

        # comments with * inside
        assert generalize_sql(
            "SELECT /* ArticleCommentList::getCommentList *Crashie* */  page_id,page_title  FROM `page`  WHERE (page_title LIKE 'Dainava/@comment-%' ) AND page_namespace = '1201'  ORDER BY page_id DESC") == \
            "SELECT page_id,page_title FROM `page` WHERE (page_title LIKE X ) AND page_namespace = X ORDER BY page_id DESC"

        # comments with * inside
        assert generalize_sql(
            "SELECT /* ListusersData::loadData Lart96 - 413bc6e5-b151-44fd-80bd-3baff733fb91 */  count(0) as cnt  FROM `events_local_users`  WHERE wiki_id = '7467' AND (user_name != '') AND user_is_closed = '0' AND ( single_group = 'poweruser' or  all_groups = ''  or  all_groups  LIKE '%bot'  or  all_groups  LIKE '%bot;%'  or  all_groups  LIKE '%bureaucrat'  or  all_groups  LIKE '%bureaucrat;%'  or  all_groups  LIKE '%sysop'  or  all_groups  LIKE '%sysop;%'  or  all_groups  LIKE '%authenticated'  or  all_groups  LIKE '%authenticated;%'  or  all_groups  LIKE '%bot-global'  or  all_groups  LIKE '%bot-global;%'  or  all_groups  LIKE '%content-reviewer'  or  all_groups  LIKE '%content-reviewer;%'  or  all_groups  LIKE '%council'  or  all_groups  LIKE '%council;%'  or  all_groups  LIKE '%fandom-editor'  or  all_groups  LIKE '%fandom-editor;%'  or  all_groups  LIKE '%helper'  or  all_groups  LIKE '%helper;%'  or  all_groups  LIKE '%restricted-login'  or  all_groups  LIKE '%restricted-login;%'  or  all_groups  LIKE '%restricted-login-exempt'  or  all_groups  LIKE '%restricted-login-exempt;%'  or  all_groups  LIKE '%reviewer'  or  all_groups  LIKE '%reviewer;%'  or  all_groups  LIKE '%staff'  or  all_groups  LIKE '%staff;%'  or  all_groups  LIKE '%translator'  or  all_groups  LIKE '%translator;%'  or  all_groups  LIKE '%util'  or  all_groups  LIKE '%util;%'  or  all_groups  LIKE '%vanguard'  or  all_groups  LIKE '%vanguard;%'  or  all_groups  LIKE '%voldev'  or  all_groups  LIKE '%voldev;%'  or  all_groups  LIKE '%vstf'  or  all_groups  LIKE '%vstf;%' ) AND ( edits >= 5)  LIMIT 1  ") == \
            "SELECT count(N) as cnt FROM `events_local_users` WHERE wiki_id = X AND (user_name != X) AND user_is_closed = X AND ( single_group = X or all_groups = X or all_groups LIKE X ... ) AND ( edits >= N) LIMIT N"

        # multiline query
        sql = """
        SELECT page_title
            FROM page
            WHERE page_namespace = '10'
            AND page_title COLLATE LATIN1_GENERAL_CI LIKE '%{{Cata%'
                """

        assert generalize_sql(sql) == \
            "SELECT page_title FROM page WHERE page_namespace = X AND page_title COLLATE LATINN_GENERAL_CI LIKE X"
