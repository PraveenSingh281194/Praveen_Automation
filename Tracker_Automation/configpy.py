import pandas as pd
import json

with open(r'D:\Users\Praveen\pendrive\PycharmProjects\Tracker_Automation\config.json','r') as context:
    data=context.read()
    properties=json.loads(data)
    SPUrl = properties["tracker"]["SPUrl"]
    username = properties["tracker"]["username"]
    password = properties["tracker"]["password"]
    site = properties["tracker"]["site"]
    my_headers = properties["tracker"]["my_headers"]
    tracker_path = properties["tracker"]["tracker_path"]
    month1 = properties["tracker"]["month1"]
    month2 = properties["tracker"]["month2"]
    fiscal_year=properties["tracker"]["fiscal_year"]

my_headers["X-RequestForceAuthentication"]=True

#location list for 1st Month(ex, for march fiscal, 1st Month is Feb and 2nd Month is March)
l3=['Gurgaon']*28
[l3.extend(i) for i in (['Chennai']*9,['Mexico']*25,['US']*1)]

#location list for 2nd Month
l4=['Gurgaon']*28
[l4.extend(i) for i in (['Chennai']*9,['Mexico']*25,['US']*1)]

fiscal_dict={'Jan':{1:list(range(25,32)),2:list(range(1,22))},
                 'Feb':{1:list(range(22,32)),2:list(range(1,19))},
                 'March':{1:list(range(19,29)),2:list(range(1,26))}}

fiscal_dates_1st_month=fiscal_dict[month2][1]
fiscal_dates_2nd_month=fiscal_dict[month2][2]

if month2 == 'Feb':
    calendar_month = ['BD_{}_Month2'.format(i) for i in (range(1,29))]
elif month2 in ['Jan','March','May','July','Aug','Oct','Dec']:
    calendar_month = ['BD_{}_Month2'.format(i) for i in (range(1,32))]
else:
    calendar_month = ['BD_{}_Month2'.format(i) for i in (range(1,31))]
