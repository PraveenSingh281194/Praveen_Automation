from pyspark.sql.types import *
from pyspark.sql import SparkSession
from numpy import divide, double, number, power
import numpy as np
from pyspark.sql.functions import *
import math
from  datetime import date,datetime,timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import expr
from datetime import date, timedelta
import pandas as pd

#All Config Setttings
config_list = {
    "currentview_path_detail":"/mnt/Krdemo/APL/PerformanceDetail/",
    "performance_sumarry_detail":"/mnt/Krdemo/APL/PerformanceSummary/",
    "reconciled_path_detail":"/mnt/Krdemo/APL/Performance/Reconciled/",
    "Accounts_folder":"Account",
    "Household_folder":"Household",
    "Benchmark_folder":"Benchmark",
    "year_value":365.2425,
    "sip_year_value":365.25,
    "scenario_list":{"one_scenario":"One Yr Performance","three_scenario":"Three Yr Performance","five_scenario":"Five Yr Performance","sip_scenario":"SI Performance","ytd_scenario":"YTD Performance","absent_accounts":"Missing Accounts"},
}


#creating spark session

spark  = SparkSession.builder.appName('Spark_Session').getOrCreate()

#All Three Dataframes performance_df , reconciled_df and currentview_df

performance_path = config_list['performance_sumarry_detail']+config_list['Accounts_folder']
performance_df = spark.read.parquet(str(performance_path)).select('AccountID','PerfSector','MonthEndDate','AsOfDate','InceptionDate','OneYrPerformance','ThreeYrPerformance','FiveYrPerformance','YTDPerformance','SIPerformance')

reconciled_path = config_list['reconciled_path_detail']+config_list['Accounts_folder']+'/Qualified'
reconciled_df = spark.read.parquet(str(reconciled_path))

currentview_path =  config_list['currentview_path_detail']+config_list['Accounts_folder']
currentview_df = spark.read.parquet(str(currentview_path)).select('AccountID','Sector','EndingDate','NetOfFeeReturns')

#Start and End Date

performance_df = performance_df.withColumn("Date_diff",datediff(to_date(performance_df.MonthEndDate,'yyyyMMdd'),to_date(performance_df.InceptionDate,'yyyyMMdd')))\
                                .withColumn("Convert_Month_End", to_date(performance_df.MonthEndDate,'yyyyMMdd'))\
                                .withColumn("Date_diff_sip",datediff(to_date(performance_df.AsOfDate,'yyyyMMdd'),to_date(performance_df.InceptionDate,'yyyyMMdd')))\
                                .withColumn("Convert_Inception_Date",to_date(performance_df.InceptionDate,'yyyyMMdd'))\
                                .withColumn("Convert_AsOfDate",to_date(performance_df.AsOfDate,'yyyyMMdd'))


performance_df = performance_df.withColumn("calculation_case",
                                           when((performance_df.Date_diff >=5*365.2425), lit("1"))\
                                           .when((performance_df.Date_diff >= 3*365.2425) & (performance_df.Date_diff < 5*365.2425), lit("2")) \
                                           .when((performance_df.Date_diff >= 1*365.2425) & (performance_df.Date_diff < 3*365.2425), lit("3")) \
                                           .otherwise(lit('4'))
                                          )
performance_df = performance_df.withColumn("calculation_start_date_5",date_sub(performance_df.Convert_Month_End,int(5*365.2425)))
performance_df = performance_df.withColumn("calculation_start_date_3",date_sub(performance_df.Convert_Month_End,int(3*365.2425)))
performance_df = performance_df.withColumn("calculation_start_date_1",date_sub(performance_df.Convert_Month_End,int(1*365.2425)))
performance_df = performance_df.withColumn("calculation_start_date_5",date_add(performance_df.calculation_start_date_5,1))
performance_df = performance_df.withColumn("calculation_start_date_3",performance_df.calculation_start_date_3)
performance_df = performance_df.withColumn("calculation_start_date_1",date_add(performance_df.calculation_start_date_1,1))
performance_df = performance_df.withColumn("calculation_end_date",performance_df.Convert_Month_End)
performance_df = performance_df.withColumn("calculation_end_asof_date",performance_df.AsOfDate)
year_start_date = date.today().replace(month=1,day=1)
performance_df = performance_df.withColumn("year_first_date",lit(str(year_start_date)))
performance_df = performance_df.withColumn("convert_year_first_date",to_date(performance_df.year_first_date,'yyyy-MM-dd'))
# performance_df=performance_df.filter((performance_df.AccountID).isin(['AL7EY7']))
# performance_df.show(8)
currentview_df = currentview_df.withColumn("Convert_EndingDate", to_date(currentview_df.EndingDate,'yyyyMMdd'))


#tempdf2 ---- Product and Plus1 - for 1 Yr, 3 Yr and 5 yr

def OneYrCaculation(performance_df):
    condition = [currentview_df.AccountID == performance_df.AccountID , (currentview_df.Convert_EndingDate >= performance_df.calculation_start_date_1) & (currentview_df.Convert_EndingDate <=performance_df.calculation_end_date )]
    number_of_days = 365.2425
    year_value = 365.2425
    temp_df2 = performance_df.join(currentview_df ,condition, "inner" ).drop(currentview_df.AccountID )
#     print(temp_df2.columns)
    
#     temp_df2.show(4)
    temp_df2 = temp_df2.withColumn("Product", temp_df2.NetOfFeeReturns/100)
    temp_df2 = temp_df2.withColumn("Plus1", temp_df2.Product+1)
    temp_df2 = temp_df2.withColumn('ST_1',when((temp_df2.Convert_EndingDate==temp_df2.calculation_start_date_1)|(temp_df2.Convert_EndingDate==temp_df2.calculation_end_date),lit(1))\
                                  .otherwise(lit(0)))
#     temp_df2.show(30)
    temp_dftest = temp_df2.filter(temp_df2.Convert_EndingDate==temp_df2.calculation_start_date_1)
#     temp_dftest.show(10)
    from pyspark.sql import functions as F
    temp_df_3 = temp_df2.groupBy("AccountID").agg(F.exp(F.sum(F.log('Plus1'))))
    temp_df_st = temp_df2.groupBy("AccountID").agg(F.sum(temp_df2.ST_1))
#     temp_df_st.show(10)
#     temp_df_3.show(10)
    temp_df_4= performance_df.join(temp_df_3,['AccountID']).select('AccountID','MonthEndDate','EXP(sum(ln(Plus1)))')
    temp_df_4 = temp_df_4.withColumn("Result_minus_1",col('EXP(sum(ln(Plus1)))')-1)
    temp_df_4 = temp_df_4.withColumn("Again_plus_1",col('Result_minus_1')+1)
    temp_df_4 = temp_df_4.withColumn("Power",pow('Again_plus_1',year_value/number_of_days))
    temp_df_4 = temp_df_4.withColumn("Power_minus_one",col('Power')-1)
    temp_df_4 = temp_df_4.withColumn("Power_multiply_100",col('Power_minus_one')*100)
    temp_df_4 = temp_df_4.withColumn("round_1",round(col('Power_multiply_100'),2))
    temp_df_5 = temp_df_4.join(temp_df_st,['AccountID']).select('AccountID','round_1','sum(ST_1)')
    temp_df_5 = temp_df_5.withColumn('1YrPerformance',when(temp_df_5['sum(ST_1)']==2,col('round_1'))\
                                    .otherwise(lit(0.00)))
#     temp_df_5.filter(temp_df_5['1YrPerformance']==0.00).show(10)
    dup_cols = ['round_1','AccountID']
    performance_df = performance_df.join(temp_df_5,[temp_df_5.AccountID == performance_df.AccountID ],"inner").drop(temp_df_5.round_1).drop(temp_df_5.AccountID)
#     performance_df.show(4)

    performance_df.select('AccountID','OneYrPerformance','1YrPerformance').show(1)
    return performance_df
    
def ThreeYrCalculation(r1_df):
    performance_df = r1_df
    condition = [currentview_df.AccountID == performance_df.AccountID , (currentview_df.Convert_EndingDate >= performance_df.calculation_start_date_3) & (currentview_df.Convert_EndingDate <=performance_df.calculation_end_date )]
    number_of_days = 3*365.2425
    year_value = 365.2425

    temp_df2 = performance_df.join(currentview_df ,condition, "inner" ).drop(currentview_df.AccountID )
#     temp_df2.show(4)
    temp_df2 = temp_df2.withColumn("Product", temp_df2.NetOfFeeReturns/100)
    temp_df2 = temp_df2.withColumn("Plus1", temp_df2.Product+1)
    temp_df2 = temp_df2.withColumn('ST_3',when((temp_df2.Convert_EndingDate==temp_df2.calculation_start_date_3)|(temp_df2.Convert_EndingDate==temp_df2.calculation_end_date),lit(1))\
                                  .otherwise(lit(0)))
    temp_dftest = temp_df2.filter(temp_df2.Convert_EndingDate==temp_df2.calculation_start_date_3)
#     temp_dftest.show(10)
    

    from pyspark.sql import functions as F
    temp_df_3 = temp_df2.groupBy("AccountID").agg(F.exp(F.sum(F.log('Plus1'))))
    temp_df_st = temp_df2.groupBy("AccountID").agg(F.sum(temp_df2.ST_3))
    #     temp_df_3.show()
    temp_df_4= performance_df.join(temp_df_3,['AccountID']).select('AccountID','MonthEndDate','EXP(sum(ln(Plus1)))')
    temp_df_4 = temp_df_4.withColumn("Result_minus_1",col('EXP(sum(ln(Plus1)))')-1)
    temp_df_4 = temp_df_4.withColumn("Again_plus_1",col('Result_minus_1')+1)
    temp_df_4 = temp_df_4.withColumn("Power",pow('Again_plus_1',year_value/number_of_days))
    temp_df_4 = temp_df_4.withColumn("Power_minus_one",col('Power')-1)
    temp_df_4 = temp_df_4.withColumn("Power_multiply_100",col('Power_minus_one')*100)
    temp_df_4 = temp_df_4.withColumn("round_1",round(col('Power_multiply_100'),2))
    temp_df_5 = temp_df_4.join(temp_df_st,['AccountID']).select('AccountID','round_1','sum(ST_3)')
    temp_df_5 = temp_df_5.withColumn('3YrPerformance',when(temp_df_5['sum(ST_3)']==1,col('round_1'))\
                                    .otherwise(lit(0.00)))
#     temp_df_5 = temp_df_5.withColumn('3YrPerformance',col('round_1'))
#     temp_df_5.show(4)
#     temp_df_5.filter(temp_df_5['3YrPerformance']==0.00).show(10)
    
    performance_df = performance_df.join(temp_df_5,[temp_df_5.AccountID == performance_df.AccountID ],"inner").drop(temp_df_5.round_1).drop(temp_df_5.AccountID)
    performance_df.select("AccountID",'OneYrPerformance','1YrPerformance','ThreeYrPerformance','3YrPerformance').show(1)
#     performance_df.show(40)
    return performance_df
    
r2_df = ThreeYrCalculation(r1_df)
r1_df=""


def FiveYrCalculation(r2_df):
    performance_df = r2_df
    condition = [currentview_df.AccountID == performance_df.AccountID , (currentview_df.Convert_EndingDate >= performance_df.calculation_start_date_5) & (currentview_df.Convert_EndingDate <=performance_df.calculation_end_date )]
    number_of_days = 5*365.2425
    year_value = 365.2425
#     temp_df2.show(10)

    temp_df2 = performance_df.join(currentview_df ,condition, "inner" ).drop(currentview_df.AccountID )
#     temp_df2.show(4)
    temp_df2 = temp_df2.withColumn("Product", temp_df2.NetOfFeeReturns/100)
    temp_df2 = temp_df2.withColumn("Plus1", temp_df2.Product+1)
    temp_df2 = temp_df2.withColumn('ST_5',when((temp_df2.Convert_EndingDate==temp_df2.calculation_start_date_5)|(temp_df2.Convert_EndingDate==temp_df2.calculation_end_date),lit(1))\
                                  .otherwise(lit(0)))
#     temp_df2.show(30)
    temp_dftest = temp_df2.filter(temp_df2.Convert_EndingDate==temp_df2.calculation_start_date_5)

    from pyspark.sql import functions as F
    temp_df_3 = temp_df2.groupBy("AccountID").agg(F.exp(F.sum(F.log('Plus1'))))
    temp_df_st = temp_df2.groupBy("AccountID").agg(F.sum(temp_df2.ST_5))
#     temp_df_st.show(3)
    temp_df_4= performance_df.join(temp_df_3,['AccountID']).select('AccountID','MonthEndDate','EXP(sum(ln(Plus1)))')
    temp_df_4 = temp_df_4.withColumn("Result_minus_1",col('EXP(sum(ln(Plus1)))')-1)
    temp_df_4 = temp_df_4.withColumn("Again_plus_1",col('Result_minus_1')+1)
    temp_df_4 = temp_df_4.withColumn("Power",pow('Again_plus_1',year_value/number_of_days))
    temp_df_4 = temp_df_4.withColumn("Power_minus_one",col('Power')-1)
    temp_df_4 = temp_df_4.withColumn("Power_multiply_100",col('Power_minus_one')*100)
    temp_df_4 = temp_df_4.withColumn("round_1",round(col('Power_multiply_100'),2))
    temp_df_5 = temp_df_4.join(temp_df_st,['AccountID']).select('AccountID','round_1','sum(ST_5)')
    temp_df_5 = temp_df_5.withColumn('5YrPerformance',when(temp_df_5['sum(ST_5)']==1,col('round_1'))\
                                    .otherwise(lit(0.00)))
#     temp_df_5.show(4)
#     temp_df_5.filter(temp_df_5['5YrPerformance']==0.00).show(10)
    performance_df = performance_df.join(temp_df_5,[temp_df_5.AccountID == performance_df.AccountID ],"inner").drop(temp_df_5.round_1).drop(temp_df_5.AccountID)
#     performance_df.select("AccountID",'OneYrPerformance','1YrPerformance','ThreeYrPerformance','3YrPerformance','FiveYrPerformance','5YrPerformance').show(50)
    return performance_df
    
r3_df = FiveYrCalculation(r2_df)
r2_df =''


def sipYrPerformance(r3_df):
    performance_df = r3_df
    condition = [currentview_df.AccountID == performance_df.AccountID , (currentview_df.Convert_EndingDate >= performance_df.Convert_Inception_Date) & (currentview_df.Convert_EndingDate <=performance_df.Convert_AsOfDate )]
    number_of_days = 365.25
    year_value = 365.25
    temp_df2 = performance_df.join(currentview_df ,condition, "inner" ).drop(currentview_df.AccountID )
    #     temp_df2.show(4)
    temp_df2 = temp_df2.withColumn("Product", temp_df2.NetOfFeeReturns/100)
    temp_df2 = temp_df2.withColumn("Plus1", temp_df2.Product+1)
    

    from pyspark.sql import functions as F
    temp_df_3 = temp_df2.groupBy("AccountID").agg(F.exp(F.sum(F.log('Plus1'))))
    temp_df_st = temp_df2.groupBy("AccountID").agg(max('Convert_EndingDate'))
#     temp_df_st.show(4)
    temp_df_4= performance_df.join(temp_df_3,['AccountID']).select('AccountID','MonthEndDate','EXP(sum(ln(Plus1)))','Date_diff_sip','AsOfDate')
    temp_df_4 = temp_df_4.withColumn("Result_minus_1",col('EXP(sum(ln(Plus1)))')-1)
    temp_df_4 = temp_df_4.withColumn("Again_plus_1",col('Result_minus_1')+1)
    temp_df_4 = temp_df_4.withColumn("Power",pow('Again_plus_1',year_value/col('Date_diff_sip')))
    temp_df_4 = temp_df_4.withColumn("Power_minus_one",col('Power')-1)
    temp_df_4 = temp_df_4.withColumn("Power_multiply_100",col('Power_minus_one')*100)
    temp_df_4 = temp_df_4.withColumn("round_1",round(col('Power_multiply_100'),2))
    temp_df_5 = temp_df_4.join(temp_df_st,['AccountID']).select('AccountID','round_1','max(Convert_EndingDate)','AsOfDate')
    temp_df_5 = temp_df_5.withColumn('SIPYrPerformance',when(temp_df_5['max(Convert_EndingDate)']<temp_df_5['AsOfDate'],lit(0.00)).otherwise(col('round_1')))
#     temp_df_5.show(4)

    performance_df = performance_df.join(temp_df_5,[temp_df_5.AccountID == performance_df.AccountID ],"inner").drop(temp_df_5.round_1).drop(temp_df_5.AccountID).drop(temp_df_5.AsOfDate)
    performance_df.select("AccountID",'OneYrPerformance','1YrPerformance','ThreeYrPerformance','3YrPerformance','FiveYrPerformance','5YrPerformance','SIPerformance','SIPYrPerformance').show(10)
    return performance_df


r4_df = sipYrPerformance(r3_df)
# r3_df=''

def dataframe_case_sip(case_number,performance_df):
    if case_number == 'sip':
        condition = [currentview_df.AccountID == performance_df.AccountID , (currentview_df.Convert_EndingDate >= performance_df.Convert_Inception_Date) & (currentview_df.Convert_EndingDate <=performance_df.Convert_AsOfDate )]
        number_of_days = 365.25
        year_value = 365.25
    elif case_number =='ytd':
        condition = [currentview_df.AccountID == performance_df.AccountID , (currentview_df.Convert_EndingDate >= performance_df.convert_year_first_date) & (currentview_df.Convert_EndingDate <=performance_df.Convert_AsOfDate )]
        
    
    temp_df2 = performance_df.join(currentview_df ,condition, "inner" ).drop(currentview_df.AccountID )
#     temp_df2.show(4)
    temp_df2 = temp_df2.withColumn("Product", temp_df2.NetOfFeeReturns/100)
    temp_df2 = temp_df2.withColumn("Plus1", temp_df2.Product+1)
    
    from pyspark.sql import functions as F
    temp_df_3 = temp_df2.groupBy("AccountID").agg(F.exp(F.sum(F.log('Plus1'))))
    temp_df_4= performance_df.join(temp_df_3,['AccountID']).select('AccountID','MonthEndDate','EXP(sum(ln(Plus1)))','Date_diff_sip','AsOfDate')
    temp_df_4 = temp_df_4.withColumn("Result_minus_1",col('EXP(sum(ln(Plus1)))')-1)
    if case_number=='sip':
        temp_df_4 = temp_df_4.withColumn("Again_plus_1",col('Result_minus_1')+1)
        temp_df_4 = temp_df_4.withColumn("Power",pow('Again_plus_1',year_value/col('Date_diff_sip')))
        temp_df_4 = temp_df_4.withColumn("Power_minus_one",col('Power')-1)
        temp_df_4 = temp_df_4.withColumn("Power_multiply_100",col('Power_minus_one')*100)
    else:
        temp_df_4 = temp_df_4.withColumn("Power_multiply_100",col('Result_minus_1')*100)
    temp_df_4 = temp_df_4.withColumn("round_1",round(col('Power_multiply_100'),2))
#     temp_df_4.show()

def ytdYrPerformance(r4_df):
    performance_df = r4_df
    condition = [currentview_df.AccountID == performance_df.AccountID , (currentview_df.Convert_EndingDate >= performance_df.convert_year_first_date) & (currentview_df.Convert_EndingDate <=performance_df.Convert_AsOfDate )]
        
    temp_df2 = performance_df.join(currentview_df ,condition, "inner" ).drop(currentview_df.AccountID )
    #     temp_df2.show(4)
    temp_df2 = temp_df2.withColumn("Product", temp_df2.NetOfFeeReturns/100)
    temp_df2 = temp_df2.withColumn("Plus1", temp_df2.Product+1)

    from pyspark.sql import functions as F
    temp_df_3 = temp_df2.groupBy("AccountID").agg(F.exp(F.sum(F.log('Plus1'))))
    temp_df_4= performance_df.join(temp_df_3,['AccountID']).select('AccountID','MonthEndDate','EXP(sum(ln(Plus1)))','Date_diff_sip','AsOfDate')
    temp_df_4 = temp_df_4.withColumn("Result_minus_1",col('EXP(sum(ln(Plus1)))')-1)
    temp_df_4 = temp_df_4.withColumn("Power_multiply_100",col('Result_minus_1')*100)
    temp_df_4 = temp_df_4.withColumn("round_1",round(col('Power_multiply_100'),2))
    temp_df_5 = temp_df_4.select('AccountID','round_1')
    temp_df_5 = temp_df_5.withColumn('YTDYrPerformance',col('round_1'))
#     temp_df_5.show(4)

    performance_df = performance_df.join(temp_df_5,[temp_df_5.AccountID == performance_df.AccountID ],"inner").drop(temp_df_5.round_1).drop(temp_df_5.AccountID) 

    performance_df.select("AccountID",'OneYrPerformance','1YrPerformance','ThreeYrPerformance','3YrPerformance','FiveYrPerformance','5YrPerformance','SIPerformance','SIPYrPerformance','YTDPerformance','YTDYrPerformance').show(5)
    return performance_df


r5_df = ytdYrPerformance(r4_df)


        
        
r1_df = OneYrCaculation(performance_df)

performance_df = r3_df.select('AccountID','MonthEndDate','AsOfDate','InceptionDate','OneYrPerformance','1YrPerformance','ThreeYrPerformance','3YrPerformance')#,'FiveYrPerformance','5YrPerformance','SIPerformance','SIPYrPerformance')
                             
final_df = performance_df.join(reconciled_df,[reconciled_df.AccountID == performance_df.AccountID],"inner").drop(reconciled_df.AccountID).drop(reconciled_df.MonthEndDate).drop(reconciled_df.AsOfDate).drop(reconciled_df.InceptionDate).drop(reconciled_df.OneYrPerformance).drop(reconciled_df.ThreeYrPerformance).drop(reconciled_df.FiveYrPerformance).drop(reconciled_df.SIPerformance)
final_df = final_df.select('AccountID','MonthEndDate','AsOfDate','InceptionDate','OneYrPerformance','1YrPerformance','Is1YrValidated','ThreeYrPerformance','3YrPerformance','Is3YrValidated')#,\
                          #'FiveYrPerformance','5YrPerformance','Is5YrValidated','SIPerformance','SIPYrPerformance','IsSIValidated')
final_df = final_df.withColumn('1YrResult',final_df.OneYrPerformance.cast('double'))
# final_df = final_df.withColumn('3YrResult',final_df.ThreeYrPerformance.cast('double'))
# final_df = final_df.withColumn('5YrResult',final_df.FiveYrPerformance.cast('double'))
# final_df = final_df.withColumn('SIPYrResult',final_df.SIPerformance.cast('double'))
# final_df = final_df.withColumn('YTDYrResult',final_df.YTDPerformance.cast('double'))
final_df = final_df.withColumn("resultts_1",F.when((final_df['1YrResult'] != final_df['1YrPerformance']) & (final_df.Is1YrValidated=='1'),lit('Mismatch')).otherwise(lit('Pass')))
# final_df = final_df.withColumn("resultts_3",F.when((final_df['3YrResult'] != final_df['3YrPerformance']) & (final_df.Is3YrValidated=='1'),lit('Mismatch')).otherwise(lit('Pass')))
# final_df = final_df.withColumn("resultts_5",F.when((final_df['5YrResult'] != final_df['5YrPerformance']) & (final_df.Is5YrValidated=='1'),lit('Mismatch')).otherwise(lit('Pass')))
# final_df = final_df.withColumn("resultts_sip",F.when((final_df['SIPYrResult'] != final_df['SIPYrPerformance']) & (final_df.IsSIValidated=='1'),lit('Mismatch')).otherwise(lit('Pass')))
# final_df = final_df.withColumn("resultts_ytd",F.when((final_df['YTDYrResult'] != final_df['YTDYrPerformance']) & (final_df.IsYTDValidated=='1'),lit('Mismatch')).otherwise(lit('Pass')))

# final_df.show(10)


first_df = final_df.filter(final_df.resultts_1=='Mismatch').select('AccountID')
print(first_df.count())
    
# one_yr_mismatches=list(final_df.filter(final_df.resultts_1=='Mismatch').select('AccountID').toPandas()['AccountID'])
# three_yr_mismatches=list(final_df.filter(final_df.resultts_3=='Mismatch').select('AccountID').toPandas()['AccountID'])
# five_yr_mismatches=list(final_df.filter(final_df.resultts_5=='Mismatch').select('AccountID').toPandas()['AccountID'])
# sip_yr_mismatches=list(final_df.filter(final_df.resultts_sip=='Mismatch').select('AccountID').toPandas()['AccountID'])
# print(len(one_yr_mismatches))
# print(len(three_yr_mismatches))
# print(len(five_yr_mismatches))
# print(len(sip_yr_mismatches))

#Excel Generation from final_df

import pandas as pd
column_name_dict  = {'Id':[],
            'Scenario':[],
            'Result':[],
            'Reason':[],
            'FileName':[],
        }

# five_year_reasons_list =['a','b','c']
excel_df = pd.DataFrame(column_name_dict)
# one_yr_mismatches = five_year_reasons_list
# three_yr_mismatches = []
# five_yr_mismatches = []
# sip_yr_mismatches = []
ytd_yr_mismatches=[]
missing_accounts=[]

if len(one_yr_mismatches) ==0:
    excel_df.loc[len(excel_df.index)]=['C0001','One Yr Performance', 'Pass','',str('Account')]
else:
    excel_df.loc[len(excel_df.index)]=['C0001','One Yr Performance', 'Fail','Mismatch Account are:'+str(','.join(one_yr_mismatches)),str('Account')]
    
if len(three_yr_mismatches) ==0:
    excel_df.loc[len(excel_df.index)]=['C0001','Three Yr Performance', 'Pass','',str('Account')]
else:
    excel_df.loc[len(excel_df.index)]=['C0001','Three Yr Performance', 'Fail','Mismatch Account are:'+str(','.join(three_yr_mismatches)),str('Account')]
    
if len(five_yr_mismatches) ==0:
    excel_df.loc[len(excel_df.index)]=['C0001','Five Yr Performance', 'Pass','',str('Account')]
else:
    excel_df.loc[len(excel_df.index)]=['C0001','Five Yr Performance', 'Fail','Mismatch Account are:'+str(','.join(five_yr_mismatches)),str('Account')]
    
if len(sip_yr_mismatches) ==0:
    excel_df.loc[len(excel_df.index)]=['C0001','SI Performance', 'Pass','',str('Account')]
else:
    excel_df.loc[len(excel_df.index)]=['C0001','SI Performance', 'Fail','Mismatch Account are:'+str(','.join(sip_yr_mismatches)),str('Account')]
    
if len(ytd_yr_mismatches) ==0:
    excel_df.loc[len(excel_df.index)]=['C0001','YTD Performance', 'Pass','',str('Account')]
else:
    excel_df.loc[len(excel_df.index)]=['C0001','YTD Performance', 'Fail','Mismatch Account are:'+str(','.join(ytd_yr_mismatches)),str('Account')]
    
if len(missing_accounts) ==0:
    excel_df.loc[len(excel_df.index)]=['C0001','Missing Accounts', 'Pass','',str('Account')]
else:
    excel_df.loc[len(excel_df.index)]=['C0001','MissingAccounts', 'Fail','Missings Account are:'+str(','.join(missing_accounts)),str('Account')]
    
# excel_df.to_excel('PE_Accounts.xlsx',sheet_name="Sheet 1",index=False)


columns = ["Id","Scenario","Result","Reason","Filename"]
pysparkDF = spark.createDataFrame(data = excel_df, schema = columns)

pysparkDF.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').option("header", "true").save('/mnt/Krdemo/test'+"temp")
file = dbutils.fs.ls('/mnt/Krdemo/test'+"temp")[-1].path
dbutils.fs.cp(file ,'/mnt/Krdemo/test/PEAAccounts.csv' )
dbutils.fs.rm('/mnt/Krdemo/test'+"temp" , recurse = True)

pysparkDF.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').option("header", "true").save('/mnt/Krdemo/test')
