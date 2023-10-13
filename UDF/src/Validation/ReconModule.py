import mysql.connector
from itertools import zip_longest
import config_py.config as config
from UDF.db_conn import mysql_conn
from UDF.db_conn import mysql_db_credentials

def calltoReconvalidation():
    Welcome="""
    *            You have opted for recon validation of two tables              *
    """
    print(Welcome)
    table1= input("\nPlease provide the first table for validating : ")
    table2= input("\nPlease provide the secound table for validating : ")
    limitor= input("\nPlease mention the limitor to display the records : ")

    user, passwd, host, port, database=mysql_db_credentials()
    conn=mysql_conn(host=host,user=user,password=passwd,database=database)
    
    cursor = conn.cursor()

       # Check row counts for both tables
    cursor.execute(f"SELECT COUNT(*) FROM {table1}")
    count1 = cursor.fetchone()[0]

    cursor.execute(f"SELECT COUNT(*) FROM {table2}")
    count2 = cursor.fetchone()[0]
    
    print("""\n
          
        Validating the counts between two tables...!
          
        """)

    if count1 != count2:
        print(f"\nRow count mismatch: {table1} has {count1} rows and {table2} has {count2} rows.")
    else:
        print(f"\nBoth tables have equal number of records : {count1} rows")
    
       # Compare data (assuming the tables have the same structure and order)

    cursor.execute(f"SELECT * FROM {table1}")
    data1 = cursor.fetchall()
    cursor.execute(f"SELECT * FROM {table1} limit {limitor}")
    data11 = cursor.fetchall()

    cursor.execute(f"SELECT * FROM {table2}")
    data2 = cursor.fetchall()
    cursor.execute(f"SELECT * FROM {table2} limit {limitor}")
    data22 = cursor.fetchall()
    
    mismatches = []
    print("""\n
          
        Validating the row data between two tables...!
          
        """)

    for row1, row2 in zip_longest(data1, data2):
        if row1 != row2:
            mismatches.append((row1, row2))

    if mismatches:
        print(f"""\n
            Data mismatches found between {table1} and {table2}:
            """)
        for row1, row2 in mismatches:
            print(f"{table1}: {row1} vs {table2}: {row2}")
        print(f"""\n
              
              Below are {limitor} records of first table : 
              
              """)
        for row11 in data11:
            print(f"{table1}: {row11}")
        print(f"""\n
              
            Below are {limitor} records of Secound table : 
              
              """)
        for row22 in data22:
            print(f"{table2}: {row22}")
    else:
        print(f"Data in {table1} and {table2} are identical.")

    cursor.close()
    conn.close()
    if not conn.close():
        print("""
              **************************************************************
                                connection is closed
              **************************************************************""")
