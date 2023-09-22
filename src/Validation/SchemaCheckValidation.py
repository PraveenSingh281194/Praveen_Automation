import mysql.connector
import config_py.config as config
from UDF.db_conn import mysql_conn
from UDF.db_conn import mysql_db_credentials
def callToSchemaValidation():

    def get_table_schema(conn, table_name):
        cursor = conn.cursor()
    
        # Fetch schema details for the table
        cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        schema = cursor.fetchall()
    
        cursor.close()
    
        return schema

    def validate_table_schemas(table1, table2):
        #establishing connection with database
        user, passwd, host, port, database=mysql_db_credentials()
        conn=mysql_conn(host=host,user=user,password=passwd,database=database)

        schema_table1 = get_table_schema(conn, table1)
        schema_table2 = get_table_schema(conn, table2)

        conn.close()
        if not conn.close():
            print("""
           **************************************************************
                             connection is closed ..!
           **************************************************************""")
        if schema_table1 == schema_table2:
            print(f"""\n
                  ***********************************************************************************
                                   The schema of {table1} and {table2} are identical..!
                  ***********************************************************************************""")
        else:
            print(f"""\n
                  ***********************************************************************************
                                   The schema of {table1} and {table2} are not Identical..!
                  ***********************************************************************************""")
            
            print(f"""\n
                ***********************************************************************************
                ***********************************************************************************
                                           Details of Schema for {table1}
                ***********************************************************************************
                ***********************************************************************************""")
            for column in schema_table1:
                print(column)
        
            print(f"""\n
                ***********************************************************************************
                ***********************************************************************************
                                           Details of Schema for {table2}
                ***********************************************************************************
                ***********************************************************************************""")
            for column in schema_table2:
                print(column)

    table1 = input("PLease provide the First Table name that you would like to check schema : ")
    table2 = input("PLease provide the Secound Table name that you would like to check schema : ")

    validate_table_schemas(table1, table2)
