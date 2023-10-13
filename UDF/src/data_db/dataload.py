import pandas as pd
import mysql.connector
import pyodbc
import config_py.config as config
from UDF.db_conn import mysql_conn
from UDF.db_conn import mysql_db_credentials
from UDF.report import report_gen
from UDF.email import email_Send
from sqlalchemy import create_engine
from UDF.logger import logging

def csv_to_mysql_table():
    append=True     #if append =True, then new table will not be created
                    #Set append =False , if you want to create a new table in mysql db

    print('You are about to load the data in mysql table')
    print('please carefully check the database details befoe loading data !!!')

    file_path=input('Enter the csv file path that you want to load in table : ')
    table_name=input('Enter the table name that you want to create or append : ')
            
    #connection to database
    user, passwd, host, port, database=mysql_db_credentials()
    database_d=mysql_conn(host=host,user=user,password=passwd,database=database)
     
    cursor_obj = database_d.cursor()
    fileInput = open(file_path, "r")

    #Extract first line of file
    firstLine = fileInput.readline().strip()
    columns = firstLine.split(",")
    
    #defining schema of table(column names, datatypes)
    #all datatype will be of type varchar only for now.
    sqlQueryCreate = 'CREATE TABLE'+ f' {database}.{table_name}' + "("
    for column in columns:
        sqlQueryCreate += column + " VARCHAR(64),"
    sqlQueryCreate = sqlQueryCreate[:-2]
    sqlQueryCreate += '))'
    
    try:
        #main data load logic
        if append==False:
            cursor_obj.execute(sqlQueryCreate)
        df = pd.read_csv(file_path)
        engine = create_engine("mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}".format(user, passwd, host, port, database))
        df.to_sql(name=f'{table_name}', con=engine, if_exists = 'append', index=False)  
        database_d.close()
        print('data loaded succesfully from csv')
        #report generation in reports folder
        report_gen(df,'src//reports//data.csv',file_type='csv')
        logging.info('Report generated in reports folder.')
        #email send to outlook reciepient, provide reciepient email id in a list.
        #email_Send(to_reciepient=['praveen.singh2@incedoinc.com'],cc_reciepient=['praveen.singh2@incedoinc.com'])  #please provide full path of file attachment to support mail send.
        
    except Exception as e:
        print(e)
        logging.info(f'error occured : {e}')
        raise e





