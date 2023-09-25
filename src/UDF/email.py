import win32com.client
from UDF.logger import logging
def email_Send(to_reciepient,cc_reciepient=[],file=[],n=1):
    attach=[]
    for i in file:
        #file=input('Enter the file path you want to attach : ')
        attach.append(file)
    ol=win32com.client.Dispatch("outlook.application")
    olmailitem=0x0              #size of the new email
    newmail=ol.CreateItem(olmailitem)       #mail object 
    to_reciepient=_reciepient_format(to_reciepient)
    cc_reciepient=_reciepient_format(cc_reciepient)
    newmail.Subject= 'Report Attachment'
    newmail.To=to_reciepient
    if cc_reciepient is not None:
        newmail.CC=cc_reciepient 
    newmail.Body= """ Hi Py_Utilities User,

 Please find attached the report generated from the Py_Utilities Framework.

 Regards,
 Py_Utilities Team
    """
    for file in attach:
        newmail.Attachments.Add(file)
    #newmail.Display() 
    newmail.Send()
    logging.info('Email sent to reciepients successfully.')

def _reciepient_format(list1):
    if list1!=[]:
        str1=''
        for i in list1:
            str1+=i+';'
        str1=str1[0:len(str1)-1]
        return str1
    
    
