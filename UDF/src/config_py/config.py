import pandas as pd
import json
import base64

with open(r'src\config_json\input.json','r') as context:
    data=context.read()
    properties=json.loads(data)

    #if we want the user to provide the input for database at runtime.
    # db_choice=input("""Enter the database name that you would be working upon
    # 1. Mysql
    # 2. SSMS
    # 3. oracle
    # 4. Postgres
    # Enter the sequence.
    # """)

    # either above way or we can hardcode db_choice input here 
    #db_choice=='1' # we can also also keep this as a parameter in json file.
    
    #db_choice=properties["db"]["db_type"]  # reading from json directly
    db_choice='2'
    if db_choice=='1':
        db_type='mysql'
    elif db_choice=='2':
        db_type='ssms'
    elif db_choice=='3':
        db_type='oracle'
    elif db_choice=='4':
        db_type='postgres'

    db_variables = {
        db_type:{
           "host":properties["db"][db_type]["host"],
           "db_type":properties["db"][db_type]["db_type"],
           "username":properties["db"][db_type]["credentials"]["username"],
           "password":properties["db"][db_type]["credentials"]["password"],
           "db_name":properties["db"][db_type]["db_name"],
           "port":properties["db"][db_type]["port"],
           "server":properties["db"][db_type]["server"]
                }
    }

