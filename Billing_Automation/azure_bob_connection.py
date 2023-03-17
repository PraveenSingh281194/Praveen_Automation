#libraries import
import pandas as pd,os,time,datetime
from azure.storage.blob import ContainerClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient, BlobServiceClient
from io import StringIO
#import pyspark
#from pyspark.sql import SparkSession        # Create a SparkSession
#M:\IT\Praveen\Billing_Automation\advisor_benefit_db_conn.py
a=time.time()
#blob storage dfs url link
account_url = "https://aznct01edhadl1.dfs.core.windows.net/"    #test env
#account_url= "https://azncd01edhadl1.dfs.core.windows.net/"     #dev env

#credentials for azure connectivity
creds = DefaultAzureCredential()
service_client = BlobServiceClient(account_url=account_url,credential=creds)

#hardcodedpath to check the connection logic for now
container_name = "02curated"
file_name='part-00000-tid-177945225372209500-833ad183-8357-4580-8604-2fb84b0bd253-147847-1-c000.snappy.parquet'    #actual file name that needs to be downloaded.
#blob_name = f'PartnerFeeds/Account/{file_name}'     #path to file in azure blob storage
business_date = datetime.datetime.today().strftime('%Y%m%d')
blob_name=f"ODS/Account/AUM/BLKDMD/{file_name}"

#blob url creation
blob_url = f"{account_url}/{container_name}/{blob_name}"
blob_client = BlobClient.from_blob_url(blob_url=blob_url,credential=creds)
blob_download = blob_client.download_blob()

#pandas dataframe creation
df = pd.read_csv(StringIO(blob_download.content_as_text()),encoding = 'unicode_escape', engine ='python')
print('dataframe created')
print('-------------------------------')
print(df.head())
print(len(df.index))
print('total time taken for dataframe creation',time.time()-a, 'Sec')

#opening a file for data reading
# with open("example.csv", "wb") as file_object:
#     data = blob_download
#     data.readinto(file_object)



# df=pd.read_csv("example.csv",sep=',',low_memory=False)
# print(df.head())
# print(len(df.index))
# print('total time taken for dataframe creation',time.time()-a, 'Sec')

# #remove the file
# if os.path.exists("example.csv"):
#     os.remove("example.csv")
#     print("The file has been deleted successfully")
# else:
#     print("The file does not exist!")
