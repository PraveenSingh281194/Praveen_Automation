import mysql.connector,pyodbc
import config_py.config as config
import config_py.config_db as config_db
import base64,pandas as pd,numpy as np
import oracledb
from sqlalchemy import create_engine

db_details=config_db.db_details()

def _decode(encoded_value):
    try:
        import base64
        try:
            decoded_value = base64.b64decode(encoded_value).decode('ascii',errors='ignore')
            return decoded_value
        except TypeError as e:
            raise TypeError("Attempted to decode {value} once. Illegal Value. ".format(value=encoded_value))
    except ImportError:
        raise ImportError("Base64 import failed")

db_details['password']=_decode(db_details['password'])      #password decode
if 'server' in db_details.keys():
    db_details['server']=_decode(db_details['server']) 

if 'dsn' in db_details.keys():
    db_details['dsn']=_decode(db_details['dsn'])

# print(db_details)
# a=db_details

# def switch_db_conn(case):
#     switch_dict = {        
#         'mysql': mysql_sql_to_df_conn,
#         'ssms': ssms_sql_to_df_connection,
#                     }        
#     return switch_dict.get(case, 'Provided case does not exist')

def oracle_sql_to_df_connection(sql_query):
    try:
        user,password,dsn,database=db_details['username'],db_details['password'],db_details['dsn'],db_details['db_name']
        con = oracledb.connect(user=user, password=password, dsn=dsn)
        if con.is_healthy():
            print("Healthy connection!")
            print("Database version:", con.version)
        else:
            print("Unusable connection. Please check the database and network settings.")
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            from pandas import DataFrame
            df = DataFrame(cursor.fetchall())
            df.columns = [x[0] for x in cursor.description]
            print("I got %d lines " % len(df))
        return df
    except Exception as e:
        raise e

def mysql_sql_to_df_conn(sql_query):
    # Establish the connection
    try:
        host,user,password,database,port=db_details['host'],db_details['username'],db_details['password'],db_details['db_name'],db_details['port']
        conn = mysql.connector.connect(host=host,user=user,password=password,database=database)
        if conn.is_connected():
            print(""" connection to database is estalished ..! """)
        engine = create_engine("mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database))
        df = pd.read_sql_query(sql_query,con=engine)
        return df
    except Exception as e:
        raise e

def ssms_sql_to_df_connection(sql_query):
    try:
        user,password,database,server=db_details['username'],db_details['password'],db_details['db_name'],db_details['server']
        conn= pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};" f"SERVER={server};"
        f"DATABASE={database};" f"UID={user};" f"PWD={password};")
        df=pd.read_sql_query(sql_query,con=conn)
        return df
    except Exception as e:
        raise e


def db_credentials():
    host=config.db_variables[config.db_type]["host"]
    user=config.db_variables[config.db_type]["username"]
    password=config.db_variables[config.db_type]["password"]
    database=config.db_variables[config.db_type]["db_name"]
    port=config.db_variables[config.db_type]["port"]
    server=config.db_variables[config.db_type]["server"]
    dsn=config.db_variables[config.db_type]["dsn"]
    password=_decode(password)              #coz json won't accept slashes in string, so pass encrypted dsn , servr details if any in json and then decode it.
    dsn=_decode(dsn)
    if server!='':
        server=_decode(server)
        return user,password,dsn,database,server
    else:
        return user,password,dsn,database,host,port,




def _encode(sample_string):
    import base64
    sample_string_bytes = sample_string.encode("ascii")
  
    base64_bytes = base64.b64encode(sample_string_bytes)
    base64_pwd = base64_bytes.decode("ascii")
    return base64_pwd

# database_type=switch_db_conn(config.db_type)