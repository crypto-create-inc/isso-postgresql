# -*- encoding: utf-8 -*-

import psycopg2
import logging
import operator
import os.path

from collections import defaultdict

logger = logging.getLogger("isso")

from isso.compat import buffer

from isso.db_psql.comments import Comments
from isso.db_psql.threads import Threads
from isso.db_psql.spam import Guard
from isso.db_psql.preferences import Preferences


class PSQL:
    """DB-dependend wrapper around PostgreSQL.

    Runs migration if `user_version` is older than `MAX_VERSION` and register
    a trigger for automated orphan removal.
    """

    MAX_VERSION = 3

    def __init__(self, path, conf):

        self.path = os.path.expanduser(path)
        self.conf = conf

        rv = self.execute([
            "SELECT tablename AS name FROM pg_catalog.pg_tables"
            "   WHERE tablename IN ('threads', 'comments', 'preferences')"]
        ).fetchone()

        self.preferences = Preferences(self)
        self.threads = Threads(self)
        self.comments = Comments(self)
        self.guard = Guard(self)

        if rv is None:
            self.set_version(PSQL.MAX_VERSION)
        else:
            self.migrate(to=PSQL.MAX_VERSION)

        self.execute([
            'CREATE or REPLACE FUNCTION remove_stale_threads_func() RETURNS trigger AS $remove_stale_threads_func$',
            'BEGIN',
            '   DELETE FROM threads WHERE id NOT IN (SELECT tid FROM comments);',
            '   RETURN NULL;',
            'END',
            '$remove_stale_threads_func$ LANGUAGE plpgsql'
        ])

        self.execute([
            'DROP TRIGGER IF EXISTS remove_stale_threads ON comments;',
            'CREATE TRIGGER remove_stale_threads',
            'AFTER DELETE ON comments',
            'EXECUTE PROCEDURE remove_stale_threads_func()'])

    def execute(self, sql, args=()):

        if isinstance(sql, (list, tuple)):
            sql = ' '.join(sql)

        sql = sql.replace('?', '%s')

        with psycopg2.connect(self.path) as con:
            cursor = con.cursor()
            cursor.execute(sql, args)
            return cursor

    @property
    def version(self):
        return self.execute("SELECT MAX(version) FROM versions").fetchone()[0]

    def set_version(self, version):
        self.execute('CREATE TABLE IF NOT EXISTS versions (version INTEGER)')
        self.execute("INSERT INTO versions VALUES (%i)" % version)

    def migrate(self, to):

        if self.version >= to:
            return

        logger.info("migrate database from version %i to %i", self.version, to)

        # re-initialize voters blob due a bug in the bloomfilter signature
        # which added older commenter's ip addresses to the current voters blob
        if self.version == 0:

            from isso.utils import Bloomfilter
            bf = buffer(Bloomfilter(iterable=["127.0.0.0"]).array)

            with psycopg2.connect(self.path) as con:
                con.cursor().execute('UPDATE comments SET voters=?', (bf, ))
                con.cursor().execute('INSERT INTO versions VALUES (1)')
                logger.info("%i rows changed", con.total_changes)

        # move [general] session-key to database
        if self.version == 1:

            with psycopg2.connect(self.path) as con:
                if self.conf.has_option("general", "session-key"):
                    con.cursor().execute('UPDATE preferences SET value=? WHERE key=?', (
                        self.conf.get("general", "session-key"), "session-key"))

                con.cursor().execute('INSERT INTO versions VALUES (2)')
                logger.info("%i rows changed", con.total_changes)

        # limit max. nesting level to 1
        if self.version == 2:

            first = lambda rv: list(map(operator.itemgetter(0), rv))

            with psycopg2.connect(self.path) as con:
                top = first(con.cursor().execute("SELECT id FROM comments WHERE parent IS NULL").fetchall())
                flattened = defaultdict(set)

                for id in top:

                    ids = [id, ]

                    while ids:
                        rv = first(con.cursor().execute("SELECT id FROM comments WHERE parent=?", (ids.pop(), )))
                        ids.extend(rv)
                        flattened[id].update(set(rv))

                for id in flattened.keys():
                    for n in flattened[id]:
                        con.cursor().execute("UPDATE comments SET parent=? WHERE id=?", (id, n))

                con.cursor().execute('INSERT INTO versions VALUES (3)')
                logger.info("%i rows changed", con.total_changes)
