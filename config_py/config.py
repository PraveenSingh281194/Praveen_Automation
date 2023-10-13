import pandas as pd
import json
import base64
import os,datetime

#report directory creation
test_result_folder='test_results_' + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
directory=f"reports\\{test_result_folder}"

if not os.path.exists(directory):
        os.makedirs(directory)

execution_manner='sequential'           #script execution manner

#-------------------------------------------------------------------------
#Test Parameters to be changed by user
#-------------------------------------------------------------------------
test_script_dir = "Test_scripts"  # "Test_scripts\\functional_scripts"  # change this path to the directory where your test execution scripts are stored.
execution_env='qa'                 # env to execute the script, eg. 'qa' , 'prod'
db_type='oracle'                         # db type for execution, eg. Mysql,ssms.

#pass the specific script names that you want to execute in attribute 'test_execute_list'. Only pass if you don't want to run the whole suite.
test_execute_list=['script2']