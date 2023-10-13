import config_py.config as config

#sample queries

#QA ENV SOurce and target queries.
if config.db_type=='mysql':
    if config.execution_env=='qa':
        sql_query_src="""select distinct * from test_db.demo7;"""  

        sql_query_trg="""select distinct * from test_db.demo7_replica;"""

    elif config.execution_env=='prod':
        sql_query_src="""select distinct * from test_db.demo7;"""  

        sql_query_trg="""select distinct * from test_db.demo7_replica;"""

elif config.db_type=='ssms':
    if config.execution_env=='qa':
        sql_query_src="""SELECT distinct * FROM [test_db].[dbo].[test2]"""  

        sql_query_trg="""SELECT distinct * FROM [test_db].[dbo].[test2]"""

    elif config.execution_env=='prod':
        sql_query_src="""SELECT distinct * FROM [test_db].[dbo].[test2]"""  

        sql_query_trg="""SELECT distinct * FROM [test_db].[dbo].[test2]"""

if config.db_type=='oracle':
    if config.execution_env=='qa':
        sql_query_src="""select distinct * from test_db.demo7;"""  

        sql_query_trg="""select distinct * from test_db.demo7_replica;"""

    elif config.execution_env=='prod':
        sql_query_src="""select distinct * from test_db.demo7;"""  

        sql_query_trg="""select distinct * from test_db.demo7_replica;"""
