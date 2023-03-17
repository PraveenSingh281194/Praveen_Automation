import pandas as pd
import numpy as np
def preprocessing(df,month_name,year,loc_list,df_path):
    col_list=['Emp_Id','Emp_name','BD_1','BD_2','BD_3','BD_4','BD_5','BD_6','BD_7','BD_8','BD_9','BD_10',
         'BD_11','BD_12','BD_13','BD_14','BD_15','BD_16','BD_17','BD_18','BD_19','BD_20','BD_21','BD_22','BD_23','BD_24',
          'BD_25','BD_26','BD_27','BD_28','BD_29','BD_30','BD_31']  #To match with DB Schema
    push_db=pd.DataFrame(columns=col_list)       #temp df
    df.drop(['S.No.','Unnamed: 3'],axis=1,inplace=True) 
    l1=df.columns.tolist() #to store the column names in a list
    
    #handle weekends 
    #df.loc[:,[i for i in l1 if (i.startswith('Saturday') or i.startswith('Sunday'))]]='WH'
    df[[i for i in l1 if (i.startswith('Saturday') or i.startswith('Sunday'))]]=df[[i for i in l1 if (i.startswith('Saturday') or i.startswith('Sunday'))]].fillna('WH')
    df.fillna('Y', inplace=True)  #replacing nan with 'Y'
    
    #handle removal of legends and team location name from Name attribute rows
    df=df[(df.Name!='Y') & (df.Name!='Gurgaon Team') & (df.Name!='Chennai Team') & (df.Name!='Interns') & (df.Name!='Mexico Team') & (df.Name!='LEGEND') & (df.Name!='Planned Leave') &  (df.Name!='Sick Leave') & (df.Name!='Unplanned Leave') & (df.Name!='Public Holiday')]
    df = df.reset_index(drop=True).rename(columns={'Name':'Emp name'})
    
    
    #Handle of varying month dates like 28,29,30,31
    if len(df.columns)==32:
        df.insert(loc=len(df.columns),column='BD_31',value=['']*len(df.index))
    elif len(df.columns)==30:
        df = df.assign(BD_29=['']*len(df.index), BD_30=['']*len(df.index), BD_31=['']*len(df.index))
    elif len(df.columns)==31:    
        df = df.assign(BD_30=['']*len(df.index), BD_31=['']*len(df.index))
    
    df.columns=push_db.columns                  #assigning name of temp df to our df to match with table schema column names.
    
    df=df.assign(Month=[month_name]*len(df.index), Year=[year]*len(df.index),Location=loc_list)
    df = df.astype(str).replace(r'\.0$', '', regex=True)
    
    df.to_excel(df_path,index=False)
    return df

def absent_creation(df,date_columns,month_name):
    # Date columns
    # date_columns=['BD_1','BD_2','BD_3','BD_4','BD_5','BD_6','BD_7','BD_8','BD_9','BD_10',
    #      'BD_11','BD_12','BD_13','BD_14','BD_15','BD_16','BD_17','BD_18','BD_19','BD_20','BD_21','BD_22','BD_23','BD_24',
    #       'BD_25','BD_26','BD_27','BD_28','BD_29','BD_30','BD_31']
    
    df['Absent_date']=  df[date_columns].applymap(lambda x: 'PL' in x).dot(df[date_columns].columns + ',').str[:-1] + ',' + df[date_columns].applymap(lambda x: 'UL' in x).dot(df[date_columns].columns + ',').str[:-1] + ','  + df[date_columns].applymap(lambda x: 'SL' in x).dot(df[date_columns].columns + ',').str[:-1] + ',' + df[date_columns].applymap(lambda x: 'PH' in x).dot(df[date_columns].columns + ',').str[:-1]
    df['Absent_date']=  df['Absent_date'].str.lstrip(',')
    df['Absent_date']=  df['Absent_date'].str.rstrip(',')
    df['Absent_date'] = df['Absent_date'].str.replace(',,,',',')
    df['Absent_date'] = df['Absent_date'].str.replace(',,',',')
    df['Absent_date'] = df['Absent_date'].str.replace('BD_',f'{month_name}_')
    return df


def date_change():
    #this function takes fiscal dates in 2 lists and converts in Req Date format (eg. 'BD_1')

    #creating dictionary to handle fiscal dates
    fiscal_dict={'Nov':{1:list(range(20,31)),2:list(range(1,25))}}
    date_mon_1=fiscal_dict['Nov'][1]
    date_mon_2=fiscal_dict['Nov'][2]

    list1,list2=[],[]
    list1.extend(['BD_{}_x'.format(i) for i in date_mon_1])
    list2.extend(['BD_{}_y'.format(i) for i in date_mon_2])

    return list1,list2

#func to insert columns at specific values
def fiscal_efforts(df,week,col_name,pos):
    df.insert(pos,col_name,(df[week] == 'Y').sum(axis=1)*8)
    return df



def calculate(df,date_columns):
    #calculation of present days and leaves of a resource in a month
    df['Working_days'] = (df[date_columns] == 'Y').sum(axis=1) + (df[date_columns] == 'Working from Mexico').sum(axis=1)             
    df['Absent_count'] = (df[date_columns] == 'PL').sum(axis=1) + (df[date_columns] == 'UL').sum(axis=1) + (df[date_columns] == 'SL').sum(axis=1) + (df[date_columns] == 'PH').sum(axis=1)
    df['Planned_leave']= (df[date_columns] == 'PL').sum(axis=1)
    df['Unplanned_leave'] = (df[date_columns] == 'UL').sum(axis=1)
    df['Sick_leave'] = (df[date_columns] == 'SL').sum(axis=1)
    df['Public_holidays'] = (df[date_columns] == 'PH').sum(axis=1)
    df['Other_holidays'] = (df[date_columns] == 'Travelling to India').sum(axis=1)
    df['Non_billable_days'] = (df[date_columns].isna()).sum(axis=1)
    df = df.astype(str)
    df[date_columns] = df[date_columns].replace('nan','')
    return df

#function to extract absent dates 
def absent_dates(final_df):
    final_df.insert(len(final_df.columns),'Absent_dates',final_df['Absent_date_x'].map(str) + ',' + final_df['Absent_date_y'].map(str))
    final_df['Absent_dates'] = final_df['Absent_dates'].str.replace('nan','')
    final_df['Absent_dates']=final_df['Absent_dates'].str.lstrip(',')
    final_df['Absent_dates']=final_df['Absent_dates'].str.rstrip(',')
    final_df.drop(['Absent_date_x','Absent_date_y'],axis=1,inplace=True)
    return final_df


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


def weekend_handle(final_df,weekend_dates):
    final_df[[i for i in weekend_dates if (i.endswith('1') or i.endswith('7') or i.endswith('8') or i.endswith('14') or i.endswith('15') or i.endswith('21') or i.endswith('22') or i.endswith('28') or i.endswith('29') or i.endswith('35'))]]=final_df[[i for i in weekend_dates if (i.endswith('1') or i.endswith('7') or i.endswith('8') or i.endswith('14') or i.endswith('15') or i.endswith('21') or i.endswith('22') or i.endswith('28') or i.endswith('29') or i.endswith('35'))]].fillna('WH')
    return final_df



  
    