import sharepy
from sharepy import connect
from sharepy import SharePointSession
import logging
import pandas as pd
import numpy as np
from pathlib import Path
import fiscal_df_preprocessing

def dataframe_creation():
    SPUrl = "https://assetmark-my.sharepoint.com"
    username ="Praveen.Singh@assetmark.com"
    password ="Welcome208435!" 

    # site = "https://assetmark-my.sharepoint.com/:x:/r/personal/rishi_bhadoria_assetmark_com/Documents/Documents/AM_2022/QA%20Transition/QA_Leave_Tracker_2022.xlsx"
    # s = sharepy.connect(SPUrl,username,password)
    #site = "https://assetmark-my.sharepoint.com/:x:/r/personal/anuj_kogata_assetmark_com1/Documents/QA%20Leave%20Tracker/QA%20Leave%20Tracker%202023.xlsx"
    site = "https://assetmark-my.sharepoint.com/:x:/r/personal/anuj_kogata_assetmark_com1/Documents/QA%20Leave%20Tracker/QA%20Leave%20Tracker%202023.xlsx"
    s = sharepy.connect(SPUrl,username,password)

    # Create header for the http request
    my_headers = {
    'accept' : 'application/json;odata=verbose',
    'content-type' : 'application/json;odata=verbose',
    'odata' : 'verbose',
    'X-RequestForceAuthentication' : 'true'
    }
    local_file_path=str(Path.home() / "Downloads" /"SP_file.xlsx" )
    response = s.getfile(site,headers = my_headers,filename = local_file_path)
    #convert excel into dataframe
    month_name='Feb'
    try:
        df=pd.read_excel(local_file_path,sheet_name=month_name)
    except Exception as e:
        print(e)

    return df

def dates_sorting(df,col_name):
    df[col_name]=df[col_name].replace(np.nan,'')
    dates_list=df[col_name].tolist()
    sorted_list=[]
    for i in dates_list:
        words=[word for word in i.split(',')]
        words.sort()
        words.sort(key=lambda x: len(x))
        res=','.join(words)
        sorted_list.append(res)
    return sorted_list

def preprocessing():
    df=dataframe_creation()
    df_path=str(Path.home() / "Downloads" /"month_end_df.xlsx")
    col_list=['Emp_Id','Emp_name','BD_1','BD_2','BD_3','BD_4','BD_5','BD_6','BD_7','BD_8','BD_9','BD_10',
         'BD_11','BD_12','BD_13','BD_14','BD_15','BD_16','BD_17','BD_18','BD_19','BD_20','BD_21','BD_22','BD_23','BD_24',
          'BD_25','BD_26','BD_27','BD_28','BD_29','BD_30','BD_31']  #To match with DB Schema
    push_db=pd.DataFrame(columns=col_list)       #temp df
    ignore_cols=['Unnamed: 3','Unnamed: 31','Unnamed: 32','Unnamed: 33','Unnamed: 34','Unnamed: 35','Unnamed: 36','Unnamed: 37','Unnamed: 38','Unnamed: 39','Unnamed: 40']
    for i in ignore_cols:
        if i in df.columns.tolist():
            df.drop([i],axis=1,inplace=True) 
    df.drop(['S.No.'],axis=1,inplace=True) 
    #df.drop(['S.No.','Unnamed: 3'],axis=1,inplace=True) 
    l1=df.columns.tolist() #to store the column names in a list
    
    #to handle weekends 
    #df.loc[:,[i for i in l1 if (i.startswith('Saturday') or i.startswith('Sunday'))]]='WH'
    df[[i for i in l1 if (i.startswith('Saturday') or i.startswith('Sunday'))]]=df[[i for i in l1 if (i.startswith('Saturday') or i.startswith('Sunday'))]].fillna('WH')
    df.fillna('Y', inplace=True)  #replacing nan with 'Y'
    
    #handle removal of legends and team location name from Name attribute rows
    df=df[(df.Name!='Y') & (df.Name!='Gurgaon Team') & (df.Name!='Chennai Team') & (df.Name!='Interns') & (df.Name!='Mexico Team') & (df.Name!='US Team') & (df.Name!='LEGEND') & (df.Name!='Planned Leave') &  (df.Name!='Sick Leave') & (df.Name!='Unplanned Leave') & (df.Name!='Public Holiday')]
    df = df.reset_index(drop=True).rename(columns={'Name':'Emp name'})
    
    #Location list to handle the location of resources

    l3=['Gurgaon']*28
    [l3.extend(i) for i in (['Chennai']*9,['Mexico']*25,['US']*1)]
    
    #Handle of varying month dates like 28,29,30,31
    if len(df.columns)==32:
        df.insert(loc=len(df.columns),column='BD_31',value=['']*len(df.index))
    elif len(df.columns)==30:
        df = df.assign(BD_29=['']*len(df.index), BD_30=['']*len(df.index), BD_31=['']*len(df.index))
    elif len(df.columns)==31:    
        df = df.assign(BD_30=['']*len(df.index), BD_31=['']*len(df.index))
    
    df.columns=push_db.columns                  #assigning name of temp df to our df to match with table schema column names.
    
    month_name='Feb'    #month of script run
    year='2023'  #year of script run
    df=df.assign(Month=[month_name]*len(df.index), Year=[year]*len(df.index),Location=l3)
    df = df.astype(str).replace(r'\.0$', '', regex=True)
    # Date columns
    date_columns=['BD_1','BD_2','BD_3','BD_4','BD_5','BD_6','BD_7','BD_8','BD_9','BD_10',
         'BD_11','BD_12','BD_13','BD_14','BD_15','BD_16','BD_17','BD_18','BD_19','BD_20','BD_21','BD_22','BD_23','BD_24',
          'BD_25','BD_26','BD_27','BD_28','BD_29','BD_30','BD_31']
    
    #calculation of present days and leaves of a resource in a month
    df['Working_days'] = (df[date_columns] == 'Y').sum(axis=1) + (df[date_columns] == 'Working from Mexico').sum(axis=1)             
    df['Absent_count'] = (df[date_columns] == 'PL').sum(axis=1) + (df[date_columns] == 'UL').sum(axis=1) + (df[date_columns] == 'SL').sum(axis=1) + (df[date_columns] == 'PH').sum(axis=1)
    df['Planned_leave']= (df[date_columns] == 'PL').sum(axis=1)
    df['Unplanned_leave'] = (df[date_columns] == 'UL').sum(axis=1)
    df['Sick_leave'] = (df[date_columns] == 'SL').sum(axis=1)
    df['Public_holidays'] = (df[date_columns] == 'PH').sum(axis=1)
    df['Other_holidays'] = (df[date_columns] == 'Travelling to India').sum(axis=1)
    df['Absent_dates'] = df[date_columns].applymap(lambda x: 'PL' in x).dot(df[date_columns].columns + ',').str[:-1] + ',' + df[date_columns].applymap(lambda x: 'UL' in x).dot(df[date_columns].columns + ',').str[:-1] + ',' + df[date_columns].applymap(lambda x: 'SL' in x).dot(df[date_columns].columns + ',').str[:-1] + ',' + df[date_columns].applymap(lambda x: 'PH' in x).dot(df[date_columns].columns + ',').str[:-1] 
    df['Absent_dates'] = df['Absent_dates'].str.replace('nan','')
    df['Absent_dates'] = df['Absent_dates'].str.replace(',,,',',')
    df['Absent_dates'] = df['Absent_dates'].str.replace(',,',',')
    df['Absent_dates'] = df['Absent_dates'].str.replace('BD_',f'{month_name}_')
    df['Absent_dates'] = dates_sorting(df,'Absent_dates')
    df['Absent_dates'] =  df['Absent_dates'].str.lstrip(',')
    df['Absent_dates'] =  df['Absent_dates'].str.rstrip(',')
    
    df.to_excel(df_path,index=False)

preprocessing()  


