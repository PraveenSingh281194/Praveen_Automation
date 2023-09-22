import openpyxl
import pandas as pd

def report_gen(reporting_df,report_path,file_type='excel'):
    if file_type!='csv':
        reporting_df.to_excel(report_path,index=False)
    else:
        reporting_df.to_csv(report_path,index=False)

    print('report generated in folder')
