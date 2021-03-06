#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2017 Snowflake Computing Inc. All right reserved.
#

import pytest

import snowflake.connector


@pytest.mark.skipif(True, reason="""
No commit or rollback is supported as of today until we support the implicit 
transaction.
""")
def test_transaction(conn_cnx, db_parameters):
    u"""
    Transaction API
    """
    with conn_cnx() as cnx:
        cnx.cursor().execute(u"create table {name} (c1 int)".format(
            name=db_parameters['name']))
        cnx.cursor().execute(u"insert into {name}(c1) "
                             u"values(1234),(3456)".format(
            name=db_parameters['name']))
        c = cnx.cursor()
        c.execute(u"select * from {name}".format(name=db_parameters['name']))
        total = 0
        for rec in c:
            total += rec[0]
        assert total == 4690, u'total integer'

        #
        cnx.cursor().execute(u"begin")
        cnx.cursor().execute(
            u"insert into {name}(c1) values(5678),(7890)".format(
                name=db_parameters['name']))
        c = cnx.cursor()
        c.execute(u"select * from {name}".format(name=db_parameters['name']))
        total = 0
        for rec in c:
            total += rec[0]
        assert total == 18258, u'total integer'
        cnx.rollback()

        c.execute(u"select * from {name}".format(name=db_parameters['name']))
        total = 0
        for rec in c:
            total += rec[0]
        assert total == 4690, u'total integer'

        #
        cnx.cursor().execute(u"begin")
        cnx.cursor().execute(
            u"insert into {name}(c1) values(2345),(6789)".format(
                name=db_parameters['name']))
        c = cnx.cursor()
        c.execute(u"select * from {name}".format(name=db_parameters['name']))
        total = 0
        for rec in c:
            total += rec[0]
        assert total == 13824, u'total integer'
        cnx.commit()
        cnx.rollback()
        c = cnx.cursor()
        c.execute(u"select * from {name}".format(name=db_parameters['name']))
        total = 0
        for rec in c:
            total += rec[0]
        assert total == 13824, u'total integer'


def test_connection_context_manager(request, db_parameters):
    db_config = {
        'protocol': db_parameters['protocol'],
        'account': db_parameters['account'],
        'user': db_parameters['user'],
        'password': db_parameters['password'],
        'host': db_parameters['host'],
        'port': db_parameters['port'],
        'database': db_parameters['database'],
        'schema': db_parameters['schema'],
        'timezone': 'UTC',
    }

    def fin():
        with snowflake.connector.connect(**db_config) as cnx:
            cnx.cursor().execute("""
DROP TABLE IF EXISTS {name}
""".format(name=db_parameters['name']))

    request.addfinalizer(fin)

    try:
        with snowflake.connector.connect(**db_config) as cnx:
            cnx.cursor().execute("""
ALTER SESSION SET AUTOCOMMIT_API_SUPPORTED=true
""".format(name=db_parameters['name']))
            cnx.autocommit(False)
            cnx.cursor().execute("""
CREATE TABLE {name} (cc1 int)
""".format(name=db_parameters['name']))
            cnx.cursor().execute("""
INSERT INTO {name} VALUES(1),(2),(3)
""".format(name=db_parameters['name']))
            ret = cnx.cursor().execute("""
SELECT SUM(cc1) FROM {name}
""".format(name=db_parameters['name'])).fetchone()
            assert ret[0] == 6
            cnx.commit()
            cnx.cursor().execute("""
INSERT INTO {name} VALUES(4),(5),(6)
""".format(name=db_parameters['name']))
            ret = cnx.cursor().execute("""
SELECT SUM(cc1) FROM {name}
""".format(name=db_parameters['name'])).fetchone()
            assert ret[0] == 21
            cnx.cursor().execute("""
SELECT WRONG SYNTAX QUERY
""".format(name=db_parameters['name']))
            raise Exception("Failed to cause the syntax error")
    except snowflake.connector.Error as e:
        # syntax error should be caught here
        # and the last change must have been rollbacked
        with snowflake.connector.connect(**db_config) as cnx:
            ret = cnx.cursor().execute("""
SELECT SUM(cc1) FROM {name}
""".format(name=db_parameters['name'])).fetchone()
            assert ret[0] == 6
