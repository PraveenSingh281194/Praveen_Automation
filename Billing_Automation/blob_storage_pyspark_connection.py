#libraries import
import pandas as pd,os,time
from azure.storage.blob import ContainerClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient, BlobServiceClient
import pyspark
from pyspark.sql import SparkSession        # Create a SparkSession
a=time.time()
print('start time',a)
#blob storage dfs url link
account_url = "https://azncd01edhadl1.dfs.core.windows.net/"

#spark object
spark = (SparkSession.builder.appName("SparkSQLExampleApp").getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("WARN")
#credentials for azure connectivity
creds = DefaultAzureCredential()
service_client = BlobServiceClient(account_url=account_url,credential=creds)

#hardcodedpath to check the connection logic for now
container_name = "02curated"
blob_name = "PartnerFeeds/Account/Account_20211018.csv"

#blob url creation
blob_url = f"{account_url}/{container_name}/{blob_name}"
blob_client = BlobClient.from_blob_url(blob_url=blob_url,credential=creds)
blob_download = blob_client.download_blob()

#opening a file for data reading
with open("example.csv", "wb") as file_object:
    data = blob_download
    data.readinto(file_object)

#creating a pysprk dataframe
try:
    print('pyspark dataframe creation in process')
    spark_df=spark.read.options(delimiter='|').csv("example.csv")
    print('pyspark dataframe created')
    spark_df.show()
    print('The length of pyspark dataframe is' ,spark_df.count(), 'and ' 'no of columns in pyspark dataframe are', len(spark_df.columns))

except Exception as E:
    print(E)
    
#remove the file
if os.path.exists("example.csv"):
    os.remove("example.csv")
    print("The blob file has been deleted successfully")
else:
    print("The file does not exist!")

b=time.time()
print('totaltime taken for task is', b-a,'seconds')
