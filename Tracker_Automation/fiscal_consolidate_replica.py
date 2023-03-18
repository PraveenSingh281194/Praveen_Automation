# import sharepy
# from sharepy import connect
# from sharepy import SharePointSession
import pandas as pd
import numpy as np
from pathlib import Path 
import fiscal_consolidate_preprocessing
import configpy
#
# SPUrl = "https://assetmark-my.sharepoint.com"
# username ="Praveen.Singh@assetmark.com"
# password ="Welcome208435!"
#
# site = "https://assetmark-my.sharepoint.com/:x:/r/personal/anuj_kogata_assetmark_com1/Documents/QA%20Leave%20Tracker/QA%20Leave%20Tracker%202023.xlsx"


# # Create header for the http request
# my_headers = {
# 'accept' : 'application/json;odata=verbose',
# 'content-type' : 'application/json;odata=verbose',
# 'odata' : 'verbose',
# 'X-RequestForceAuthentication' : 'true'
# }
# month_name2='March'
# month_name1='Feb'
# fiscal_year='2023'
# tracker_path=configpy.tracker_path

#only uncomment below two lines in actual code
# s = sharepy.connect(configpy.SPUrl,configpy.username,configpy.password)
#response = s.getfile(configpy.site,headers = configpy.my_headers,filename = configpy.tracker_path) #check and change for exact path of tracker path in json , it shopuld be downloads folder for AM.


try:
    df1=pd.read_excel(configpy.tracker_path,sheet_name=configpy.month1)
    df2=pd.read_excel(configpy.tracker_path,sheet_name=configpy.month2)
except Exception as e:
    print(e)

# local_file_path_mar= str(Path.home() / "Downloads" /"mar_file.xlsx")
# local_file_path_feb= str(Path.home() / "Downloads" /"feb_file.xlsx")
# df_path1= str(Path.home() / "Downloads" /"Nov_df.xlsx")
# df_path2= str(Path.home() / "Downloads" /"Dec_df.xlsx")
merged_df_path= str(Path.home() / "Downloads"/"Consolidated_data.xlsx")
split_df_path= str(Path.home() / "Downloads"/"Split_data.xlsx")
final_df_path= str(Path.home() / "Downloads"/"fiscal_df.xlsx")


# response2 = s.getfile(site,headers = my_headers,filename = local_file_path_mar)
#convert excel into dataframe



#1st Month Loc
#2nd Month location




#creating processed dataframes
df1=fiscal_consolidate_preprocessing.preprocessing(df1, configpy.month1, configpy.fiscal_year, configpy.l3)
df2=fiscal_consolidate_preprocessing.preprocessing(df2, configpy.month2, configpy.fiscal_year, configpy.l4)

#business dates list for both months
list1,list2,list3=fiscal_consolidate_preprocessing.date_change()

fiscal_list1=[i.replace('_x', '') for i in list1]
fiscal_list2=[j.replace('_y', '') for j in list2]
df1=fiscal_consolidate_preprocessing.fiscal_absent_creation(df1, fiscal_list1, configpy.month1)
df2=fiscal_consolidate_preprocessing.fiscal_absent_creation(df2, fiscal_list2, configpy.month2)

#cal_absent_date=fiscal_consolidate_preprocessing.cal_absent_creation(df2,list3,month_name2)

#merging df using outer join to support both month's data
merged_df=df1.merge(df2,how='outer', on=['Emp_Id','Emp_name'])
merged_df['Fiscal_Absent_date_x']=fiscal_consolidate_preprocessing.dates_sorting(merged_df, 'Fiscal_Absent_date_x')
merged_df['Fiscal_Absent_date_y']=fiscal_consolidate_preprocessing.dates_sorting(merged_df, 'Fiscal_Absent_date_y')
merged_df.loc[merged_df.Month_y.isna(), ['Month_y']] = merged_df['Month_x']
merged_df.loc[merged_df.Year_y.isna(), ['Year_y']] = merged_df['Year_x']
merged_df['Fiscal_Absent_Date']=merged_df['Fiscal_Absent_date_x'].map(str) + ',' + merged_df['Fiscal_Absent_date_y'].map(str)
merged_df['Fiscal_Absent_Date']=merged_df['Fiscal_Absent_Date'].str.lstrip(',').replace('nan','')
merged_df['Fiscal_Absent_Date']=merged_df['Fiscal_Absent_Date'].str.rstrip(',')
merged_df.drop(['Month_x', 'Year_x', 'Location_x', 'Location_y', 'Fiscal_Absent_date_x', 'Fiscal_Absent_date_y'], axis=1, inplace=True)
for i in range(1,32):
    merged_df.rename(columns={f"BD_{i}_x": f"BD_{i}_Month1",f"BD_{i}_y": f"BD_{i}_Month2"},inplace=True)
merged_df.rename(columns={"Month_y": "Emp_last_entry_Month", "Year_y": "Emp_last_entry_Year"},inplace=True)

#creating fiscal weeks
# fiscal_week1=['BD_{}_Month1'.format(i) for i in range(19,29)]
# fiscal_week2=['BD_{}_Month2'.format(i) for i in range(1,26)]
fiscal_dates_1st_month = ['BD_{}_Month1'.format(i) for i in configpy.fiscal_dates_1st_month ] #configpy.fiscal_dates_1st_month
fiscal_dates_2nd_month = ['BD_{}_Month2'.format(i) for i in configpy.fiscal_dates_2nd_month ] #configpy.fiscal_dates_2nd_month
total_fiscal_days = [*fiscal_dates_1st_month, *fiscal_dates_2nd_month]

# month1=['BD_{}_Month1'.format(i) for i in range(1,29)]
# month2=['BD_{}_Month2'.format(i) for i in range(1,32)]
total_calendar_month = [*configpy.calendar_month]

#dates to slice for split df column
# fiscal_month_1=['BD_{}_Month1'.format(i) for i in range(19,29)]
# fiscal_month_2=['BD_{}_Month2'.format(i) for i in range(1,26)]
# slicing_dates=[*fiscal_month_1,*fiscal_month_2]
slicing_dates=['Emp_Id','Emp_name',*total_fiscal_days,'Emp_last_entry_Month','Emp_last_entry_Year','Fiscal_Absent_Date','Cal_Absent_date', 'Fiscal_effort(hr)','Calendar_Month_effort(hr)']
#cal_absent_date=[f'BD_{i}_Month2' for i in range(1,32)]   #Check for varying month dates here

merged_df=fiscal_consolidate_preprocessing.cal_absent_creation(merged_df,total_calendar_month,configpy.month2)
merged_df['Cal_Absent_date']=fiscal_consolidate_preprocessing.dates_sorting(merged_df,'Cal_Absent_date')
merged_df=fiscal_consolidate_preprocessing.fiscal_efforts(merged_df,total_fiscal_days,'Fiscal_effort(hr)', len(merged_df.columns))
merged_df=fiscal_consolidate_preprocessing.fiscal_efforts(merged_df,total_calendar_month,'Calendar_Month_effort(hr)',len(merged_df.columns))
merged_df.to_excel(merged_df_path,index=False)
split_df=merged_df[slicing_dates]
split_df.to_excel(split_df_path,index=False)
