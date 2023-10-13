
import pandas as pd,numpy as np,sys
from UDF.data_compare import compare,multiple_df_to_excel_sheet
def script1():
    data1 = pd.date_range('1/1/2011', periods = 8, freq ='H')
    data2=  pd.date_range('1/1/2011', periods = 10, freq ='H')
    ref_cols=['ID']
    cols_to_comp=['ID','data1','name','age']
    # Define a column mapping dictionary
    column_mapping = {'ID_y': 'ID', 'data1_y': 'data1',
                        'name_y':'name','age_y':'age'}

    
    df1=pd.DataFrame({'ID':[1,2,5,18,8,9,10,16],'data1':data1,'name':['praveen','singh','nidhi','kum','kumar','sample','Rahul','Ganguly'],
        'age':[25,27,28,29,30,'',np.nan,12]})
    df2=pd.DataFrame({'ID_y':[1,2,5,9,11,16,17,18,20,22],'name_y':['praveen','singh','nidhi','kumari','kumar','sample','Kohli','sachin','Azhar','jadeja'],
        'age_y':[12,27,28,89,30,12,34,14,18,29],'data_again':data2,'data1_y':data2})

    # df1.loc[1, 'data1'] = '2011-01-01 18:00:00'
    # df1.loc[2, 'data1'] = '2011-01-01 02:10:00'
    # Rename columns in df1 using the mapping dictionary
    df2.rename(columns=column_mapping, inplace=True)
    #invoking function
    compare(df1,df2,ref_cols,cols_to_comp,'sheet1')
script1()