import pandas as pd

def file2db_function():
    print("""\n******* It Seems you want to validate a File with a DB  *******
              """)
    source_file_location = input("Please Enter the Source File Location :  ")
    target_db_name = input("\n Please Specify the DB you want the data to be Validated from")

    def get_source_dataframe(source_file_location):

        get_source_file_extension  = source_file_location.split('.')[-1]
        if get_source_file_extension== 'csv':
            source_file_dataframe = pd.read_csv(source_file_location)
        elif get_source_file_extension == 'xlsx':
            source_file_dataframe = pd.read_excel(source_file_location)
        else:
            source_file_dataframe= open(source_file_location,"r")
            # print(source_file_dataframe)

        return source_file_dataframe
    
    get_source_dataframe(source_file_location)



    def get_target_dataframe_option2():
        
        pass