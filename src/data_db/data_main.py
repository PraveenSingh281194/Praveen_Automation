import pandas as pd
from data_db.file2file import file2file_function
from data_db.file2db import file2db_function
from data_db.db2db import db2db_function
from data_db.dataload import csv_to_mysql_table
from data_db.table_drop import mysql_table_drop
from data_db.csv_to_ssms import csv_to_ssms_db


welcome_message = """
********************  Welcome to the comparison Utility **************
"""
print(welcome_message)

type_of_validation  = input("""********* Please Select the Type of Validation that you want to do ******
1. File to File 
2. File to DB
3. DB to DB
4. dataload
5. table_drop
6. ssms

Enter the Sequence : """)

def __validation(type_of_validation):

    if int(type_of_validation) == 1:
        file2file_function()

    elif int(type_of_validation) == 2:

        file2db_function()

    elif int(type_of_validation) == 3:

        db2db_function()

    elif int(type_of_validation) == 4:

        csv_to_mysql_table()
    elif int(type_of_validation) == 5:

        mysql_table_drop()
    elif int(type_of_validation) == 6:
        csv_to_ssms_db()



__validation(type_of_validation)