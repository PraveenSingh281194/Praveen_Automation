import great_expectations as gx
import pandas as pd,numpy as np


# data1 = pd.date_range('1/1/2011', periods = 8, freq ='H')
# data2=  pd.date_range('1/1/2011', periods = 10, freq ='H')
# ref_cols=['ID']
# cols_to_comp=['ID','data1','name','age']
# # Define a column mapping dictionary
# column_mapping = {'ID_y': 'ID', 'data1_y': 'data1',
#                     'name_y':'name','age_y':'age'}


# df1=pd.DataFrame({'ID':[1,2,5,18,8,9,10,16],'data1':data1,'name':['praveen','singh','nidhi','kum','kumar','sample','Rahul','Ganguly'],
#     'age':[25,27,28.22444467,29,30,'',np.nan,12]})
# df2=pd.DataFrame({'ID_y':[1,2,5,9,11,16,17,18,20,22],'name_y':['praveen','singh','nidhi','kumari','kumar','sample','Kohli','sachin','Azhar','jadeja'],
#     'age_y':[12,27,28.2244446,89,30,12,34,14,18,29],'data_again':data2,'data1_y':data2})

context = gx.get_context()

validator = context.sources.pandas_default.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)

validator.expect_column_values_to_not_be_null("pickup_datetime")
validator.expect_column_values_to_be_between("passenger_count", auto=True)
validator.save_expectation_suite()

checkpoint = context.add_or_update_checkpoint(
    name="my_quickstart_checkpoint",
    validator=validator,
)

checkpoint_result = checkpoint.run()

context.view_validation_result(checkpoint_result)