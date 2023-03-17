import pandas as pd
import numpy as np
import pyodbc

#Newway2022@a
#anuj.kogata@assetmark.com


def advisor_benefit_conn():
    server_name='sqlewmdigitaltest.database.windows.net'
    database_name='AdvisorBenefit'
    conn=pyodbc.connect(Driver='{ODBC Driver 17 for SQL Server}',
                    Server=server_name,
                    Port='1433',
                    Database=database_name,
                    AUTHENTICATION='ActiveDirectoryPassword',
                    User ='anuj.kogata@assetmark.com',
                    Password='Newway2022@a',
                    MARS_Connection='Yes')
    query ="""SELECT  * FROM [dbo].[AdvisorAsset] as tra """
    df=pd.read_sql_query(query,con=conn)
    return df
    
#advisor_benefit_conn()



def edh_connection(path,business_date=''):
    conn = pyodbc.connect(Driver='{ODBC Driver 17 for SQL Server}',
                    Server='tcp:aznct01edhsynwrkspc1-ondemand.sql.azuresynapse.net',
                    Port='1433',
                    Database='EDH',
                    AUTHENTICATION='ActiveDirectoryPassword',
                    User ='Praveen.Singh@assetmark.com',
                    Password='Welcome208435!',
                    MARS_Connection='Yes' 
                    )
    query =f"""
            select * from OPENROWSET
            (BULK '{path}', DATA_SOURCE = 'EDH_DataLake',FORMAT='PARQUET',FIRSTROW = 1) 
            as tra {business_date}
            """
    df=pd.read_sql_query(query,con=conn)
    return df
    

#edh_connection()

    