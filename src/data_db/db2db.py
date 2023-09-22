import pandas as pd
import mysql.connector
import pyodbc
import config_py.config as config
from UDF.db_conn import mysql_conn,mysql_db_credentials
from sqlalchemy import create_engine

def db2db_function():
    print("""******* It Seems you want to validate a DB with a DB  *******""")
    print("""I am assuming both tables are in same database""")

    source_db_name = input("Please Enter the Source Table Name  :  ")
    target_db_name = input("\n Please Specify the Table you want the data to be Validated from : ")
    
    user, passwd, host, port, database=mysql_db_credentials()
    database_d=mysql_conn(host=host,user=user,password=passwd,database=database)

    engine = create_engine("mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}".format(user, passwd, host, port, database))
    database_d.close()

    source_data = pd.read_sql_query("select * from {};".format(source_db_name),con=engine)
    target_data = pd.read_sql_query("select * from {};".format(target_db_name),con=engine)
        
    if len(source_data.index)== len(target_data.index):
        print("1. Both Tables have Equal Number of Data")

    else:
        print("1 . Rows mismatch : Source File has {0} rows and Target File has {1} rows".format(len(source_data),len(target_data)))
    df_diff = pd.concat([source_data,target_data]).drop_duplicates(keep=False)
    report_gen(df,'src//reports//GFG.xlsx')
    print('\n')
    print(df_diff)

