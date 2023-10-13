import mysql.connector
import config_py.config as config
from UDF.db_conn import mysql_conn
from UDF.db_conn import mysql_db_credentials
def callToCountValidation():
   
    #The below def will perform the count action on each table that was passed on to it and returns count
    def get_table_count(conn, table_name):
       cursor = conn.cursor()
    
       # Fetch row count for the table
       cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
       count = cursor.fetchone()[0]
       cursor.close()
       return count

    def validate_table_counts(table1, table2):
         user, passwd, host, port, database=mysql_db_credentials()
         conn=mysql_conn(host=host,user=user,password=passwd,database=database)
 
         count_table1 = get_table_count(conn, table1)
         count_table2 = get_table_count(conn, table2)

         conn.close()
         if not conn.close():
          print("""
                **************************************************************
                                  connection is closed
                **************************************************************""")
         if count_table1 == count_table2:
             print(f"Both {table1} and {table2} have the same row count: {count_table1}.")
         else:
             print(f"Both {table1} and {table2} do not have the same row count")
             print(f"Row count for {table1}: {count_table1}")
             print(f"Row count for {table2}: {count_table2}")


    table1=input("Please enter the First table name : ")
    table2=input("Please enter the secound table name : ")

    validate_table_counts(table1, table2)
