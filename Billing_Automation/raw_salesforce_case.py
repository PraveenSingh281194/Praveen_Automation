import pandas as pd
import pyodbc
def advisor_benefit_conn():
    server_name='tcp:aznct01edhsynwrkspc1-ondemand.sql.azuresynapse.net'
    #database_name='AdvisorBenefit'
    conn=pyodbc.connect(Driver='{ODBC Driver 17 for SQL Server}',
                    Server=server_name,
                    Port='1433',
                    Database='EDH',
                    AUTHENTICATION='ActiveDirectoryPassword',
                    User ='Praveen.Singh@assetmark.com',
                    Password='Welcome208435!',
                    MARS_Connection='Yes')
    query ="""select top 10 *  
FROM
OPENROWSET (
BULK '/00raw/Salesforce/Case/*/*/*/',
DATA_SOURCE = 'EDH_DataLake', FORMAT = 'PARQUET',
FIRSTROW = 1 
) AS x"""
    df=pd.read_sql_query(query,con=conn)
    return df


df=advisor_benefit_conn()
print(df)