import pyodbc
import pandas as pd
import numpy as np

#database connection to d12 env and df creation
def d12ewmbda_conn():
    server_name='d12ewmdba'
    database_name='AM_Billing'
    conn=pyodbc.connect('Driver={SQL Server};'
                        f'Server={server_name};'
                        f'Database={database_name};'
                        'Trusted_Connection=yes;')
    query ="""
            SELECT top 10 * FROM [AM_Billing].[dbo].[Account]
            """
    df=pd.read_sql_query(query,con=conn)
    print(len(df))
    print(df)

d12ewmbda_conn()