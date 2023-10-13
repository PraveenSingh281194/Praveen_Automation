import subprocess,time
from UDF.logger import logging

#code execution starts from here
if __name__ == "__main__":
    start_time=time.time()
    print('!!!Welcome to Py_utilities!!!')
    # user_input=input("""Please select the type of operation you would like to execute, 
    # '1' for data load
    # '2' for Validation
    # Enter the Sequence : """)
    user_input='1'
    if user_input=='1':
        try:
            print('Code execution starts')
            from data_db import data_main
            logging.info('Data load part completed, please proceed with validation')
            #from Validation import Agenda
        except Exception as E:
            print(E)
            raise E 
    elif user_input=='2':
        try:
            print('Code execution starts')
            from Validation import Agenda
        except Exception as E:
            print(E)
            raise E
    print(f'Total Time taken: ', time.time()-start_time)  
    
