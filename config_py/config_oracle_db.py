import pandas as pd
import json
import base64
import os,datetime


with open(r'config_json\\config_db_oracle.json','r') as context:
    data=context.read()
    properties=json.loads(data)
    db_type='oracle'                    #have to remove the hardcoding of this later
    db_variables = {
        db_type:{
           "host":properties["db"][db_type]["host"],
           "dsn":properties["db"][db_type]["dsn"],
           "username":properties["db"][db_type]["credentials"]["username"],
           "password":properties["db"][db_type]["credentials"]["password"],
           "db_name":properties["db"][db_type]["db_name"],
           "port":properties["db"][db_type]["port"],
           "server":properties["db"][db_type]["server"]
                }
    }

