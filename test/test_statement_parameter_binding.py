#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2017 Snowflake Computing Inc. All right reserved.
#
from datetime import datetime

import pytz


def test_binding_security(conn_cnx):
    """
    Binding statement parameters
    """
    expected_qa_mode_datetime = datetime(1967, 6, 23, 7, 0, 0, 123000, pytz.UTC)

    with conn_cnx() as cnx:
        cnx.cursor().execute("alter session set timezone='UTC'")
        with cnx.cursor() as cur:
            cur.execute(
                "show databases like 'TESTDB_JM'")
            rec = cur.fetchone()
            assert rec[0] != expected_qa_mode_datetime

        with cnx.cursor() as cur:
            cur.execute(
                "show databases like 'TESTDB_JM'",
                _statement_params={
                    'QA_MODE': True,
                })
            rec = cur.fetchone()
            import pdb
            pdb.set_trace()
            assert rec[0] == expected_qa_mode_datetime

        with cnx.cursor() as cur:
            cur.execute(
                "show databases like 'TESTDB_JM'")
            rec = cur.fetchone()
            assert rec[0] != expected_qa_mode_datetime
