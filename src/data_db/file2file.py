import pandas as pd

def file2file_function():
    print("""\n******* It Seems you want to Validate 2 Files  *******
              """)
    source_file_location = input("Please Enter the Source File Location :  ")
    target_file_location = input("""\nPlease Enter  Target File Location: """)



    get_source_file_extension  = source_file_location.split('.')[-1]
    if get_source_file_extension== 'csv':
        source_file_dataframe = pd.read_csv(source_file_location)
    elif get_source_file_extension == 'xlsx':
        source_file_dataframe = pd.read_excel(source_file_location)
    else:
        source_file_dataframe= open(source_file_location,"r")



    


    get_target_file_extension  = target_file_location.split('.')[-1]
    if get_target_file_extension== 'csv':
        target_file_dataframe = pd.read_csv(target_file_location)
    elif get_target_file_extension == 'xlsx':
        target_file_dataframe = pd.read_excel(target_file_location)
    else:
        target_file_dataframe= open(target_file_location,"r")



        ##### Validating dataframe


    print("\nAnalysis Started for Validating the dataframe\n\n")

    if len(source_file_dataframe.index)== len(target_file_dataframe.index):
        print("1. Both Files have Equal Number of Rows")

    else:
        print("1 . Rows mismatch : Source File has {0} rows and Target File has {1} rows".format(len(source_file_dataframe),len(target_file_dataframe)))
    
    df_diff = pd.concat([source_file_dataframe,target_file_dataframe]).drop_duplicates(keep=False)

    print('\n')

    print(df_diff)
    
    

