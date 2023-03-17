import sharepy
from sharepy import connect
from sharepy import SharePointSession
import pandas as pd
import numpy as np
from pathlib import Path 
import fiscal_df_preprocessing 

SPUrl = "https://assetmark-my.sharepoint.com"
username ="Praveen.Singh@assetmark.com"
password ="Welcome208435!" 

site = "https://assetmark-my.sharepoint.com/:x:/r/personal/rishi_bhadoria_assetmark_com/Documents/Documents/AM_2022/QA%20Transition/QA_Leave_Tracker_2022.xlsx"
s = sharepy.connect(SPUrl,username,password)

# Create header for the http request
my_headers = {
'accept' : 'application/json;odata=verbose',
'content-type' : 'application/json;odata=verbose',
'odata' : 'verbose',
'X-RequestForceAuthentication' : 'true'
}

local_file_path= str(Path.home() / "Downloads" /"S_file.xlsx")
df_path1= str(Path.home() / "Downloads" /"Nov_df.xlsx")
df_path2= str(Path.home() / "Downloads" /"Dec_df.xlsx")
merged_df_path= str(Path.home() / "Downloads" /"merged_df.xlsx")
final_df_path= str(Path.home() / "Downloads" /"fiscal_df.xlsx")
response = s.getfile(site,headers = my_headers,filename = local_file_path)
#convert excel into dataframe
month_name1='NOV'
month_name2='DEC'
fiscal_year='2022'

try:
    df1=pd.read_excel(local_file_path,sheet_name=month_name1)
    df2=pd.read_excel(local_file_path,sheet_name=month_name2)    
except Exception as e:
    print(e)

#1st Month Loc
l3=['Gurgaon']*26
[l3.extend(l) for l in (['Chennai']*9,['Gurgaon']*3,['Mexico']*24)]


#2nd Month location
l4=['Gurgaon']*27
[l4.extend(i) for i in (['Chennai']*9,['Mexico']*22)]

#creating processed dataframes
df1=fiscal_df_preprocessing.preprocessing(df1,month_name1,fiscal_year,l3,df_path1)
df2=fiscal_df_preprocessing.preprocessing(df2,month_name2,fiscal_year,l4,df_path2)

#business dates list for both months
list1,list2=fiscal_df_preprocessing.date_change()

fiscal_list1=[i.replace('_x','') for i in list1]
fiscal_list2=[j.replace('_y','') for j in list2]
df1=fiscal_df_preprocessing.absent_creation(df1,fiscal_list1,month_name1)
df2=fiscal_df_preprocessing.absent_creation(df2,fiscal_list2,month_name2)

#merging df using outer join to support both month's data
merged_df=df1.merge(df2,how='outer', on=['Emp_Id','Emp_name'])
fiscal_year=[fiscal_year]*len(merged_df.index)
#creating req cols for extraction from merged df
req_cols=['Emp_Id','Emp_name','Month_y','Absent_date_x','Absent_date_y']

req_cols.extend(list1)
req_cols.extend(list2)
#extracting the req cols from merged df
final_df=merged_df[req_cols]
final_df['Month_y']=final_df['Month_y'].replace(np.nan,month_name2)
final_df['Absent_date_x']=fiscal_df_preprocessing.dates_sorting(final_df,'Absent_date_x')
final_df['Absent_date_y']=fiscal_df_preprocessing.dates_sorting(final_df,'Absent_date_y')

#cols_pos=[3,9,15,21,27,28]  #to create columns at specific position
#cols_pos=[3,11,19,27,35,43,44]  #to create columns at specific position
cols_pos=[3,13,21,29,37,45,46]  #to create columns at specific position
final_df.insert(cols_pos[0],'Fiscal_year',fiscal_year)

#creating final db schema columns and df
final_cols=['Emp_Id','Emp_name','Fiscal_month','Fiscal_year','Absent_date_x','Absent_date_y']
#final_cols.extend(['BD_{}'.format(i) for i in range(1,21)])   #based on column weeks
final_cols.extend(['BD_{}'.format(i) for i in range(1,36)])
final_df.columns=final_cols

#creating fiscal weeks
fiscal_week1=['BD_{}'.format(i) for i in range(1,8)]
fiscal_week2=['BD_{}'.format(i) for i in range(8,15)]
fiscal_week3=['BD_{}'.format(i) for i in range(15,22)]
fiscal_week4=['BD_{}'.format(i) for i in range(22,29)]
fiscal_week5=['BD_{}'.format(i) for i in range(29,36)]

total_fiscal_days=[*fiscal_week1,*fiscal_week2,*fiscal_week3,*fiscal_week4,*fiscal_week5]
weekend_dates=['BD_1','BD_7','BD_8','BD_14','BD_15','BD_21','BD_22','BD_28','BD_29','BD_35']
final_df=fiscal_df_preprocessing.weekend_handle(final_df,weekend_dates)

#creation of fiscal efforts attributes in df
final_df=fiscal_df_preprocessing.fiscal_efforts(final_df,fiscal_week1,'Fiscal_week1_effort(hr)',cols_pos[1])
final_df=fiscal_df_preprocessing.fiscal_efforts(final_df,fiscal_week2,'Fiscal_week2_effort(hr)',cols_pos[2])
final_df=fiscal_df_preprocessing.fiscal_efforts(final_df,fiscal_week3,'Fiscal_week3_effort(hr)',cols_pos[3])
final_df=fiscal_df_preprocessing.fiscal_efforts(final_df,fiscal_week4,'Fiscal_week4_effort(hr)',cols_pos[4])
final_df=fiscal_df_preprocessing.fiscal_efforts(final_df,fiscal_week5,'Fiscal_week5_effort(hr)',cols_pos[5])
final_df=fiscal_df_preprocessing.fiscal_efforts(final_df,total_fiscal_days,'Total_Fiscal_effort(hr)',cols_pos[6])
final_df=fiscal_df_preprocessing.calculate(final_df,total_fiscal_days)
final_df=fiscal_df_preprocessing.absent_dates(final_df)
final_df.to_excel(final_df_path,index=False)