
import pandas as pd,numpy as np,sys
from UDF.data_compare import compare
def script4():
    # data1 = pd.date_range('1/1/2011', periods = 8, freq ='H')
    # data2=  pd.date_range('1/1/2011', periods = 10, freq ='H')
    ref_cols=['index']
       #['ID','data1','name','age']

    df1=pd.read_csv(r"https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv")
    
    df2=pd.read_csv(r"https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv")
    index=list(range(1,(len(df1.index)+1)))
    df1.insert(1,'index',index)
    df2.insert(1,'index',index)
    cols_to_comp=df1.columns.tolist()

    #invoking function
    compare(df1,df2,ref_cols,cols_to_comp)

script4()