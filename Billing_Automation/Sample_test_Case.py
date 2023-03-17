import pandas as pd
import pyodbc
import detect_delimiter
from detect_delimiter import detect
import os,csv,openpyxl,time
from datetime import date

# reporting dictionary to update the report
reporting_dictionary=dict()

path ='M:\IT\Praveen\Dummy_Files\R220220808.CLI'
report_path='M:\IT\Praveen\Dummy_Files\dummy_report.xlsx'
file_attributes=['Col1','Col2','Col3','Col4','Col5']        #hardcoded for the timebeing

#reading a file contents
with open(path) as myfile:
    if os.path.exists(path) and os.stat(path).st_size!=0:           #if file exists
        df=pd.read_csv(path)
        firstline = myfile.readline()
        delimiter = detect(firstline)
        header=list(df)
        has_headings=True if header==file_attributes else False 
        footer=myfile.readlines()[-1].split(',')
        myfile.close()
    else:
        delimiter=''
        has_headings = False
        print('File is empty')

#dataframe directly from file 
df_file=pd.read_csv(path)
actual_file_name='R220220808' 
expected_file_name='R2' + ''.join(str(date.today()).split('-'))
#result and reason list to save the scenarios result and reason if it fails.
result_list=[]
reason_list=[]

# result list values append based on scenarios
scenario_list=[actual_file_name,delimiter,has_headings,footer[0],int(footer[1]),footer[2].split(' ')[0],footer[0]]
scenarios_values=[expected_file_name,',',True,'EOF',int(len(df_file)-1),str(date.today()),'EOF']

for a,b in zip(scenario_list,scenarios_values):
    result_list.append('Pass') if a==b else result_list.append('Fail')

for i in result_list:
    reason_list.append('')  if i=='Pass'  else reason_list.append('Fail')
reporting_dictionary.update({'Scenario_name':['File_name_prefix','Delimiter','Has Header','Footer_first_column','Footer_second_column','Footer_third_column','Has_Footer'],'Scenario_value':[actual_file_name,delimiter,has_headings,footer[0],footer[1],footer[2].split(' ')[0],footer[0]],'Result':result_list,'Reason':reason_list})


#creating a reporting dataframe and report generation
reporting_df=pd.DataFrame(reporting_dictionary)
reporting_df.to_excel(report_path)




