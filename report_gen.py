import openpyxl
import pandas as pd

def report_gen(reporting_df,report_path='',file_type='csv'):
    #report_path is optional, if nor provided then report would be generated with name 'report' with filetype extension.
    try:
        if report_path=='':
                report_path=f'reports//report.{file_type}'
        if file_type!='csv':
            reporting_df.to_excel(report_path,index=False)
        else:
            reporting_df.to_csv(report_path,index=False)
        print('report generated in folder')

    except Exception as e:
        print(e)
        raise e