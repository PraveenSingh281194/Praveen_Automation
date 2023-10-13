import pandas as pd
import mysql.connector
import pyodbc
from UDF.db_conn import mysql_conn,mysql_db_credentials

def mysql_table_drop():
    print('please carefully check the database details before droping table !!!')

    table_name=input('Enter the table name that you want to drop : ')
            
    #connection to database
    user, passwd, host, port, database=mysql_db_credentials()
    database_d=mysql_conn(host=host,user=user,password=passwd,database=database)
     
    cursor_obj = database_d.cursor()
    try:
        drop_sql_query = f'DROP table {table_name};'
        cursor_obj.execute(drop_sql_query)
        print('Table succesfully droped')
    except Exception as e:
        print(e)
        raise e
        