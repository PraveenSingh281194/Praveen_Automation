import pandas as pd,numpy as np
import ntpath,sys,datetime
import os
import config_py.config as config

#prerequisite for below data recon function would be 2 pandas dataframe, primary columns and columns to compare.
#2 items pending: 1) columns rename in target dataframe; 2)Column datatype allignment in both dataframes.



# while creating 2 dataframes column with values make sure that columns in both dataframes are in same order from left to right.
# (Column names can be different but the column that eeds to be compared has to be in same order)

def compare(df1,df2,ref_cols,cols_to_comp,report_gen_name='reports'):
    df1 = df1.astype(object)
    df2 = df2.astype(object)
    df1.sort_values(by=ref_cols,inplace=True)
    df1=df1[cols_to_comp]
    
    
    df2.sort_values(by=ref_cols,inplace=True)
    df2=df2[cols_to_comp]
    #df2.columns=df1.columns

    #common data between 2 dataframes
    df_merge = pd.merge(df1, df2, how='inner', on=cols_to_comp)

    df1_only=_df_only(df1,df_merge)
    df2_only=_df_only(df2,df_merge)
    # Label each dataframe
    df1_only['Src/Tgt'] = 'Source'
    df2_only['Src/Tgt'] = 'Target'
    # Identify the duplicated rows
    df3 = df1_only.append(df2_only).reset_index(drop=True)
    df3['Duplicated'] = df3.duplicated(subset=ref_cols, keep=False)       
    # `subset` argument considers only a subset of the columns when identifying duplicated rows.
    # Hence, subset here should be primary key columns

    #df_diff creation for differnce population between 2 dataframes based on primary columns
    df_diff = df3[df3['Duplicated']]
    df_diff=df_diff.sort_values(by=ref_cols).reset_index(drop=True)          #by should be sorting columns
    df_diff=_diff_columns_pop(df_diff)
    # data present in first dataframe only population logic based on Primary keys
    df1_only = df3[(~df3['Duplicated']) &(df3['Src/Tgt']=='Source')]
    df1_only=_df_drop_fixed_columns(df1_only)
    # data present in second dataframe only population logic based on Primary keys
    df2_only = df3[(~df3['Duplicated']) &(df3['Src/Tgt']=='Target')]
    df2_only=_df_drop_fixed_columns(df2_only)

    #print statements
    #print(df_merge)  #df_merge contains common data between two dataframes based on primary key columns provided.
    #print(df1_only)  #df1_only is data present in first dataframe only based on primary key columns provided
    #print(df2_only)  #df2_only is data present in second dataframe only based on primary key columns provided
    #print(df_diff)   # df_diff is the difference of data between two dataframes based on primary key columns provided.
    #logging.info('comparison succesfully done and report generated in folder')

    if report_gen_name=='reports':
        report_gen_name=report_gen_name+ '_' + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    multiple_df_to_excel_sheet([(df_diff,'difference_data'),(df1_only,'df1_only_data(source)'),(df2_only,'df2_only_data(target)'),(df_merge,'matched_data')],report_gen_name)
    
    #return df_merge,df1_only,df2_only,df_diff


#subfunctions below to support the compare function
def _df_only(df,df_merge):
    df_only = df.append(df_merge).reset_index(drop=True)
    df_only['Duplicated'] = df_only.duplicated(keep=False)
    df_only = df_only[~df_only['Duplicated']]
    return df_only

def _df_drop_fixed_columns(df):
    df.drop(['Duplicated', 'Src/Tgt'],inplace=True,axis=1)
    return df

def _diff_columns_pop(df_diff):
    df_diff_columns=df_diff.columns.tolist()
    df_diff_columns=df_diff_columns[-1:] + df_diff_columns[:-1]
    df_diff_columns=df_diff_columns[:-1]
    df_diff=df_diff[df_diff_columns]
    return df_diff

def multiple_df_to_excel_sheet(list_of_tuple,report_name):
    test_result_folder=config.test_result_folder
    with pd.ExcelWriter(f"reports\\{test_result_folder}\\{report_name}.xlsx") as writer:
        list_of_tuple[0][0].to_excel(writer, sheet_name=list_of_tuple[0][1], index=False)
        list_of_tuple[1][0].to_excel(writer, sheet_name=list_of_tuple[1][1], index=False)
        list_of_tuple[2][0].to_excel(writer, sheet_name=list_of_tuple[2][1], index=False)
        list_of_tuple[3][0].to_excel(writer, sheet_name=list_of_tuple[3][1], index=False)




