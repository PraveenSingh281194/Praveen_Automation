import subprocess,time
import os,datetime
import config_py.config as config

def _module_name(string):   
    import importlib.util
    module_name = string
    # Create a module spec object
    module_spec = importlib.util.spec_from_file_location(module_name, f"{config.test_script_dir}\\{string}")
    # Create a module object
    module = importlib.util.module_from_spec(module_spec)
    # Load the module
    module_spec.loader.exec_module(module)

def run():  
    start_time=time.time()
    #root_dir = "Test_scripts" #Test scripts directory
    test_script_dir=config.test_script_dir
    if len(config.test_execute_list)==0:
        test_scripts_list=[item for item in os.listdir(test_script_dir) if os.path.isfile(os.path.join(test_script_dir, item))]  # Filter items and only keep files (strip out directories)
    else:
        test_scripts_list=config.test_execute_list
    test_execution_list=[]
    for script in test_scripts_list:
        script=script + '.py' if not script.endswith('.py') else script
        test_execution_list.append(script)
    if config.execution_manner=='sequential':
        for script_name in test_execution_list:
            print('script getting executed is : ',script_name)
            _module_name(script_name)
    print(f'Total Time taken: ', time.time()-start_time)



