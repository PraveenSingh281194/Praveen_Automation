#Main logic resides here which is not visisble to the user
import sys
import mysql.connector
import config_py.config as config
from UDF.db_conn import mysql_conn
from UDF.db_conn import mysql_db_credentials

def calltoNullValidation():
    # Establish the connection
    table_name= input("Please give your table on which you want to perform the Null Check validation : ")
    
    user, passwd, host, port, database=mysql_db_credentials()
    conn=mysql_conn(host=host,user=user,password=passwd,database=database)

    cursor = conn.cursor()
    # Fetch columns for the table
    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    columns = [col[0] for col in cursor.fetchall()]
    print("Validating the table for null values is begun..!")
    null_columns = []

    # Check each column for NULL values
    for col in columns:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL")
        null_count = cursor.fetchone()[0]
        if null_count > 0:
            null_columns.append((col, null_count))
    print("""
           **************************************************************
                             Please find below for results
           **************************************************************""")
    if null_columns:
      for col, count in null_columns:
         print(f"Column '{col}' has {count} null values.")
    else:
      print(f"No null values found in the '{table_name}' table.")
    
    cursor.close()
    conn.close()
    if not conn.close():
     print("""
           **************************************************************
                             connection is closed
           **************************************************************""")

