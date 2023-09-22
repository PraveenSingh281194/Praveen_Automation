import mysql.connector
from Validation.CheckNullsModule import calltoNullValidation
from Validation.ReconModule import calltoReconvalidation
from Validation.Countcheck import callToCountValidation
from Validation.SchemaCheckValidation import callToSchemaValidation

Welcome="""
***********************************************************************************
***********************************************************************************
**                                                                               **
*                                                                                 *
*           Welcome to Types of Validations between 2 datasets         *
*                                                                                 *
**                                                                               **
***********************************************************************************
***********************************************************************************
"""
print(Welcome)

ValidationType=input("""
Please select the type of validations that you would like to perform on DB tables from below.

***Note: Please keep the table name handy with you***
                     
1.Null Validation (Only one table name is required)
2.Recon Validation (Two tables are required to perform operation)
3.count Validation (Two tables are required to perform operation)
4.schema validation (Two tables are required to perform operation)

 : """ )


def __validation(ValidationType=''):

    if int(ValidationType) == 1:
        calltoNullValidation()
    elif int(ValidationType) == 2:
        calltoReconvalidation()  
    elif int(ValidationType) == 3:
        callToCountValidation()
    elif int(ValidationType) == 4:
        callToSchemaValidation()
__validation(ValidationType)
