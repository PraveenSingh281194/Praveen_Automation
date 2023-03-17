import pandas as pd
import numpy as np
import advisor_benefit_db_conn
from pathlib import Path 


#code starts
aum_path='02curated/ODS/Account/AUM/*/'
contribution_path='02curated/ODS/Account/Contribution/*/*/*/*/'
business_date="where BusinessDate like '2022-10-31%'"

aum_df_path=str(Path.home() / "Downloads" /"aum_df.xlsx")
contribution_df_path=str(Path.home() / "Downloads" /"contribution_df.xlsx")
advisor_asset_df_path=str(Path.home() / "Downloads" /"advisor_asset_df.xlsx")
merged_df_path=str(Path.home() / "Downloads" /"merged_df.xlsx")
report_path=str(Path.home() / "Downloads" /"report_df.xlsx")

advisor_asset_df=advisor_benefit_db_conn.advisor_benefit_conn()
aum_df=advisor_benefit_db_conn.edh_connection(aum_path)
contribution_df=advisor_benefit_db_conn.edh_connection(contribution_path,business_date)
merged_df=aum_df.merge(contribution_df,on=['PrimaryAdvisorIdentifier'],how='outer')
aum_df.sort_values(by = ['PrimaryAdvisorIdentifier','LineOfBusiness'])
advisor_asset_df.sort_values(by = ['PrimaryAdvisorIdentifier','LineOfBusiness'])
contribution_df.sort_values(by = ['PrimaryAdvisorIdentifier','LineOfBusiness'])

advisor_asset_df.to_excel(advisor_asset_df_path,index=False)
aum_df.to_excel(aum_df_path,index=False)
contribution_df.to_excel(contribution_df_path,index=False)
merged_df.to_excel(merged_df_path,index=False)


advisor_lob=aum_df['LineOfBusiness'].values.tolist()
aum_lob=aum_df['LineOfBusiness'].values.tolist()
dict1={'Status':[],'Reason':[]}


for i,j in zip(advisor_lob,aum_lob):
    if i !=j:
        dict1['Status'].append('Fail')
        dict1['Reason'].append('LOB')
    else:
        dict1['Status'].append('Pass')
        dict1['Reason'].append('')


report_df=pd.DataFrame(dict1)
report_df.to_excel(report_path)








