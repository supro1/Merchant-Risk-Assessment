#Data Frames
import pandas as pd;
import uuid
import datetime
import random
import json
from azure.servicebus import ServiceBusService
import time

sbs = ServiceBusService(service_namespace='eventhub-name-space', shared_access_key_name='RootManageSharedAccessKey', shared_access_key_value='ZiIEyH2H7qOVAKEvTy7TV1xgD0JDzizfXcVNOuQiY7k=')

readcsv= pd.read_csv("fitbit.csv");
my_df = pd.DataFrame(readcsv);
#print(my_df);

length = len(my_df);
print("my_df.len: ",length );


for y in range(0,10):
    for i in range (0,length):
        # for sensor 1
        reading = {'id': int(my_df.iloc[i, 0]), 'timestamp': str(datetime.datetime.utcnow()), 'sensor': 1,
                   'TotalSteps': int(my_df.iloc[i, 2]), 'TotalDistance': float(my_df.iloc[i, 3]),
                   'VeryActiveDistance': float(my_df.iloc[i, 4]), 'ModeratelyActiveDistance': float(my_df.iloc[i, 5]),
                   'LightActiveDistance': float(my_df.iloc[i, 6]), 'VeryActiveMinutes': int(my_df.iloc[i, 7]),
                   'FairlyActiveMinutes': int(my_df.iloc[i, 8]), 'LightlyActiveMinutes': int(my_df.iloc[i, 9]),
                   'HeartRate': int(my_df.iloc[i, 10]), 'Calories': int(my_df.iloc[i, 11])}
        s = json.dumps(reading)
        sbs.send_event('myeventhub', s)
        print(reading)
        time.sleep(0.5);

        # for sensor 2
        reading = {'id': int(my_df.iloc[i, 0]), 'timestamp': str(datetime.datetime.utcnow()), 'sensor': 2,
                   'TotalSteps': int(my_df.iloc[i, 2]), 'TotalDistance': float(my_df.iloc[i, 3]),
                   'VeryActiveDistance': float(my_df.iloc[i, 4]), 'ModeratelyActiveDistance': float(my_df.iloc[i, 5]),
                   'LightActiveDistance': float(my_df.iloc[i, 6]), 'VeryActiveMinutes': int(my_df.iloc[i, 7]),
                   'FairlyActiveMinutes': int(my_df.iloc[i, 8]), 'LightlyActiveMinutes': int(my_df.iloc[i, 9]),
                   'HeartRate': int(my_df.iloc[i, 10]), 'Calories': int(my_df.iloc[i, 11])}

        s = json.dumps(reading)
        sbs.send_event('myeventhub', s)
        print(reading)
        time.sleep(0.5);
    print (y);