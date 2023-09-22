import mysql.connector,pyodbc
import config_py.config as config
import base64
def mysql_conn(host,user,password,database):
    # Establish the connection
    try:
        conn = mysql.connector.connect(host=host,user=user,password=password,database=database)
        if conn.is_connected():
            print(""" connection to database is estalished ..! """)
        return conn
    except Exception as e:
        raise e

def ssms_connection(server,database,user,password):
    try:
        conn= pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};" f"SERVER={server};"
        f"DATABASE={database};" f"UID={user};" f"PWD={password};")
        # if conn.is_connected:
        #     print(""" connection to database is estalished ..! """)
        return conn
    except Exception as e:
        raise e


def mysql_db_credentials():
    host=config.db_variables[config.db_type]["host"]
    user=config.db_variables[config.db_type]["username"]
    password=config.db_variables[config.db_type]["password"]
    database=config.db_variables[config.db_type]["db_name"]
    port=config.db_variables[config.db_type]["port"]
    server=config.db_variables[config.db_type]["server"]
    password=_decode(password)
    if server!='':
        server=_decode(server)
        return server,database,user,password
    else:
        return user,password,host,port,database


def _decode(encoded_value):
    try:
        import base64
        try:
            decoded_value = base64.b64decode(encoded_value).decode('ascii')
            return decoded_value
        except TypeError as e:
            raise TypeError("Attempted to decode {value} once. Illegal Value. ".format(value=encoded_value))
    except ImportError:
        raise ImportError("Base64 import failed")

def _encode(sample_string):
    sample_string_bytes = sample_string.encode("ascii")
  
    base64_bytes = base64.b64encode(sample_string_bytes)
    base64_pwd = base64_bytes.decode("ascii")

    return base64_pwd

    