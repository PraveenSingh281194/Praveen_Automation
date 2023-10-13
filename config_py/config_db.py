import pandas as pd
import json
import base64
import os,datetime
import config_py.config as config

#config dependent attributes
execution_env=config.execution_env
db_type=config.db_type

# function to switch to multiple database for fetching database details
def switch_db(case):
    switch_dict = {        
        'mysql': my_sql_credentials,
        'ssms': ssms_credentials,
        'oracle':oracle_credentials,
                    }        
    return switch_dict.get(case, 'Provided database type does not exist')


def my_sql_credentials():
    with open(r'config_json\config_db.json','r') as context:
        data=context.read()
        properties=json.loads(data) 
        db_variables = {
            db_type:{
            "host":properties["db"][db_type][execution_env]["host"],
            "db_type":properties["db"][db_type][execution_env]["db_type"],
            "username":properties["db"][db_type][execution_env]["credentials"]["username"],
            "password":properties["db"][db_type][execution_env]["credentials"]["password"],
            "db_name":properties["db"][db_type][execution_env]["db_name"],
            "port":properties["db"][db_type][execution_env]["port"],
            "server":properties["db"][db_type][execution_env]["server"]
                    }
                        }
    db_details={"username":db_variables[db_type]["username"],"password":db_variables[db_type]["password"],"db_name":db_variables[db_type]["db_name"],
    "host":db_variables[db_type]["host"],"port":db_variables[db_type]["port"]}
    return db_details

def ssms_credentials():
    with open(r'config_json\config_db.json','r') as context:
        data=context.read()
        properties=json.loads(data)
        db_variables = {
            db_type:{
            "host":properties["db"][db_type][execution_env]["host"],
            "db_type":properties["db"][db_type][execution_env]["db_type"],
            "username":properties["db"][db_type][execution_env]["credentials"]["username"],
            "password":properties["db"][db_type][execution_env]["credentials"]["password"],
            "db_name":properties["db"][db_type][execution_env]["db_name"],
            "port":properties["db"][db_type][execution_env]["port"],
            "server":properties["db"][db_type][execution_env]["server"]
                    }
                        }
    db_details={"username":db_variables[db_type]["username"],"password":db_variables[db_type]["password"],"db_name":db_variables[db_type]["db_name"],
    "server":db_variables[db_type]["server"]}
    return db_details

def oracle_credentials():
    with open(r'config_json\config_db.json','r') as context:
        data=context.read()
        properties=json.loads(data)
        db_variables = {
            db_type:{
            "host":properties["db"][db_type][execution_env]["host"],
            "dsn":properties["db"][db_type][execution_env]["dsn"],
            "username":properties["db"][db_type][execution_env]["credentials"]["username"],
            "password":properties["db"][db_type][execution_env]["credentials"]["password"],
            "db_name":properties["db"][db_type][execution_env]["db_name"],
            "port":properties["db"][db_type][execution_env]["port"],
            "server":properties["db"][db_type][execution_env]["server"]
                    }
                        }
    db_details={"username":db_variables[db_type]["username"],"password":db_variables[db_type]["password"],"db_name":db_variables[db_type]["db_name"],
    "dsn":db_variables[db_type]["dsn"]}
    return db_details


db_details=switch_db(db_type)
