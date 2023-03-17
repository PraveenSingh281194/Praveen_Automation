import sharepy
from sharepy import connect
from sharepy import SharePointSession
import pandas as pd
import numpy as np
from pathlib import Path 
import fiscal_consolidate_preprocessing  #Change it later

SPUrl = "https://assetmark-my.sharepoint.com"
username ="Praveen.Singh@assetmark.com"
password ="Welcome208435!" 

site_dec = "https://assetmark-my.sharepoint.com/:x:/r/personal/rishi_bhadoria_assetmark_com/Documents/Documents/AM_2022/QA%20Transition/QA_Leave_Tracker_2022.xlsx"
site = "https://assetmark-my.sharepoint.com/:x:/r/personal/anuj_kogata_assetmark_com1/Documents/QA%20Leave%20Tracker/QA%20Leave%20Tracker%202023.xlsx"
s = sharepy.connect(SPUrl,username,password)

# Create header for the http request
my_headers = {
'accept' : 'application/json;odata=verbose',
'content-type' : 'application/json;odata=verbose',
'odata' : 'verbose',
'X-RequestForceAuthentication' : 'true'
}

local_file_path= str(Path.home() / "Downloads" /"S_file.xlsx")
local_file_path_dec= str(Path.home() / "Downloads" /"Dec_S_file.xlsx")
df_path1= str(Path.home() / "Downloads" /"Nov_df.xlsx")
df_path2= str(Path.home() / "Downloads" /"Dec_df.xlsx")
merged_df_path= str(Path.home() / "Downloads" /"merged_df.xlsx")
final_df_path= str(Path.home() / "Downloads" /"fiscal_df.xlsx")
response1 = s.getfile(site,headers = my_headers,filename = local_file_path)
response2 = s.getfile(site_dec,headers = my_headers,filename = local_file_path_dec)
#convert excel into dataframe
month_name1='Jan'
month_name2='DEC'
fiscal_year='2023'

try:
    df1=pd.read_excel(local_file_path,sheet_name=month_name1)
    df2=pd.read_excel(local_file_path_dec,sheet_name=month_name2)
    print(len(df1),len(df2))    
except Exception as e:
    print(e)
