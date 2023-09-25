import pyodbc,csv
import pandas as pd,numpy as np
import config_py.config as config
from UDF.db_conn import ssms_connection
from UDF.db_conn import mysql_db_credentials
from UDF.report import report_gen
from UDF.email import email_Send
from sqlalchemy import create_engine
from UDF.logger import logging

def csv_to_ssms_db():
    append=True
    # file_path=input('Enter the csv file path that you want to load in table : ')
    # table_name=input('Enter the table name that you want to create or append : ')
    file_path=r'C:\Users\praveen.singh2\Documents\Py_utilities\src\Compare_files\employees.csv'
    table_name='test2'
    server,database,user,pwd=mysql_db_credentials()
    conn=ssms_connection(server=server,database=database,user=user,password=pwd)
    cursor = conn.cursor()

    f=open(file_path, "r")
    columns = f.readline().strip().split(",")
    reader=csv.reader(f)
    
    sqlQueryCreate= 'CREATE TABLE'+ f' {table_name}' + "("
    for column in columns:
        sqlQueryCreate += column + " VARCHAR(64),"
    sqlQueryCreate = sqlQueryCreate[:-2]
    sqlQueryCreate += '))'
    
    try:
        if append==False:
            cursor.execute(sqlQueryCreate)
            conn.commit()
            
        df_file = pd.read_csv(file_path)
        df_file=df_file.astype(str).replace('nan',np.nan)
        for row in reader:
            insert_query=f"""INSERT INTO {table_name} ({', '.join(columns)}) VALUES"""
            insert_query=insert_query + '{0}' 
            insert_query=insert_query.format(tuple(row))
            cursor.execute(insert_query)
        conn.commit()
        
        # #engine=create_engine('mssql+pymssql://{0}:{1}@{2}/{3}'.format(user, pwd, server, database))
        # df.to_sql(name=f'{table_name}', con=engine, if_exists = 'append', index=False)
        print('Data Loaded succesfully in table')
        # validation_input=input("""Please confirm if you want to proceed with data validation of loaded data
        # 1 : yes
        # 2 : No 
        # Enter your choice: """)
        validation_input='1'
        if validation_input=='1':
            query=f"""SELECT * FROM [{database}].[dbo].[{table_name}]"""
            df_table=pd.read_sql_query(query,con=conn)
            df_table.drop_duplicates(keep='first',inplace=True)
            df_table.columns=df_file.columns
            df_table.sort_values(by=['EMPLOYEE_ID','FIRST_NAME','LAST_NAME'],inplace=True)
            df_file.sort_values(by=['EMPLOYEE_ID','FIRST_NAME','LAST_NAME'],inplace=True)
            df_diff = pd.concat([df_file,df_table])
            df_diff.drop_duplicates(keep=False,ignore_index=True,inplace=True)
            report_gen(df_diff,'src//reports//data_difference.csv',file_type='csv')
            #email_Send(to_reciepient=['praveen.singh2@incedoinc.com'])
            print('mismatches exists in the datasets') if len(df_diff.index)>0 else print('No mismatches in 2 datasets')
            logging.info('Data inserted and Validaed succesfully.')
        else:
            logging.info('data loaded into table succesfully') 
        
    except Exception as E:
        print(E)
        raise E




