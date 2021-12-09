from time import sleep
from json import dumps
from kafka import KafkaProducer
import random
import datetime as dt
import csv
import pandas as pd
import os
import re
import numpy as np

os.chdir("/home/jovyan/work/Documents/5202/as2b/FIT5202as2partB")


def read_csv(folder_name):
    lst_folder = os.listdir(folder_name)
    pattern1 = re.compile(r'(^flight)',re.I)
    lst_file = []
    for file in lst_folder:
        if len(pattern1.findall(file)):
            lst_file.append(file)
    data = []
    #WRITE THE CODE TO READ CSV FILE
    for file in lst_file:
        with open(folder_name + "/" + file) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                data.append(row)
    return data


def flightRecords(data,keys):
    return_data = {}
    for i in keys:
        return_data[i] = [x for x in rows if x["DAY_OF_WEEK"] == str(i)]
    return return_data


def publish_message(producer_instance, topic_name, data):
    try:
        producer_instance.send(topic_name, data)
        producer_instance.flush()
        print('Message published successfully. Data: '+str(data))
    except Exception as ex:
        print('Exception in publishing message.')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                  value_serializer=lambda x: dumps(x).encode('ascii'),
                                  api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka.')
        print(str(ex))
    finally:
        return _producer


#//////////////////////////////////////////////////
if __name__ == '__main__':
    topic = 'flightTopic'
    rows = read_csv("flight-delays")
    df = pd.DataFrame(rows)
    print('Publishing records..')
    flightProducer = connect_kafka_producer()
    A = [[] for i in range(7)]
    B = [[] for i in range(7)]
    KeyFlights = np.sort(df["DAY_OF_WEEK"].unique())
    data = flightRecords(rows,KeyFlights)
    length_list = [len(data[str(i)]) for i in range(1,8)]
    start_point_list = [0 for i in range(7)]
    while True:
        print(start_point_list[0])
        random_number_A_list = [random.randint(70, 100) for i in range(7)]
        random_number_B_list = [random.randint(5, 10) for i in range(7)]
        for i in range(7):
            ts = int(dt.datetime.utcnow().timestamp())
            if start_point_list[i] + random_number_A_list[i] > length_list[i]:
                data_to_be_stream_x = data[str(i+1)][start_point_list[i]:] + \
                    data[str(i+1)][:(start_point_list[i] + random_number_A_list[i] - length_list[i])]
                start_point_list[i] = start_point_list[i] + random_number_A_list[i] - length_list[i]
            else:
                data_to_be_stream_x = data[str(i+1)][start_point_list[i]:(start_point_list[i] + random_number_A_list[i])]
                start_point_list[i] += random_number_A_list[i]
            if start_point_list[i] + random_number_B_list[i] > length_list[i]:
                data_to_be_stream_y = data[str(i+1)][start_point_list[i]:] + \
                    data[str(i+1)][:(start_point_list[i] + random_number_B_list[i] - length_list[i])]
                start_point_list[i] = start_point_list[i] + random_number_B_list[i] - length_list[i]
            else:
                data_to_be_stream_y = data[str(i+1)][start_point_list[i]:start_point_list[i] + random_number_B_list[i]]
                start_point_list[i] += random_number_B_list[i]
            B[i] = data_to_be_stream_y
            for u in range(len(data_to_be_stream_x)):
                data_to_be_stream_x[u]["ts"] = ts
            A[i] = data_to_be_stream_x
        data_to_be_sent = []
        for i in range(7):
            data_to_be_sent = data_to_be_sent + (A[i] + B[i])
        publish_message(flightProducer, topic, data_to_be_sent)
        sleep(5)






############## Trial
# read data

os.listdir()
rows = read_csv("flight-delays")
df = pd.DataFrame(rows)

# build list
KeyFlights = np.sort(df["DAY_OF_WEEK"].unique())
data = flightRecords(rows,KeyFlights)

# Build A and B

list_A = ['A' + str(x) for x in range(1,8)]

list_B = ['B' + str(x) for x in range(1,8)]

a.update({"yes":ts})
[x for x in rows if x["DAY_OF_WEEK"] == "1"]
map(lambda a: a.update({"ts":ts}),lst_keyFlight[0])

# Build A

A = {}
B = {}

start_point_list = [0 for i in range(7)]
length_list = [len(data[str(i)]) for i in range(1,8)]
for i in range(1,8):
    A.update({str(i):[]})
    B.update({str(i):[]})

# Inside loop
ts = int(dt.datetime.utcnow().timestamp())
random_number_A_list = [random.randint(70, 100) for i in range(7)]
random_number_B_list = [random.randint(5, 10) for i in range(7)]

for i in range(7):
    if start_point_list[i] + random_number_A_list[i] > length_list[i]:
        data_to_be_stream_x = data[str(i+1)][start_point_list[i]:] + \
            data[str(i+1)][:start_point_list[i] + random_number_A_list[i] - length_list[i]]
        start_point_list[i] = start_point_list[i] + random_number_A_list[i] - length_list[i]
    else:
        data_to_be_stream_x = data[str(i+1)][start_point_list[i]:start_point_list[i] + random_number_A_list[i]]
        start_point_list[i] += random_number_A_list[i]
    for u in range(len(data_to_be_stream_x)):
        data_to_be_stream_x[u]["ts"] = ts
    A[str(i+1)] = data_to_be_stream_x + B[str(i+1)]

for i in range(1,8):
    data_to_be_sent[str(i)] = A[str(i)] + B[str(i)]
    #publish_message(flightProducer, topic, data_to_be_sent)
data_to_be_sent["1"]

# Build B

for i in range(7):
    if start_point_list[i] + random_number_B_list[i] > length_list[i]:
        data_to_be_stream_y = data[str(i+1)][start_point_list[i]:] + \
            data_to_be_stream[:start_point_list[i] + random_number_B_list[i] - length_list[i]]
        start_point_list[i] += random_number_B_list[i]
    else:
        data_to_be_stream_y = data[str(i+1)][start_point_list[i]:start_point_list[i] + random_number_B_list[i]]
        start_point_list[i] += random_number_B_list[i]
    B[str(i+1)] = data_to_be_stream_y + B[str(i+1)]
    for u in range(len(data_to_be_stream_y)):
        data_to_be_stream_y[u]["ts"] = ts

# Old version

if __name__ == '__main__':
    topic = 'flightTopic'
    rows = read_csv("flight-delays")
    df = pd.DataFrame(rows)
    print('Publishing records..')
    flightProducer = connect_kafka_producer()
    A = {}
    B = {}
    KeyFlights = np.sort(df["DAY_OF_WEEK"].unique())
    data = flightRecords(rows,KeyFlights)
    B_pending = {}
    length_list = [len(data[str(i)]) for i in range(1,8)]
    for i in range(1,8):
        A.update({str(i):[]})
        B.update({str(i):[]})
        B_pending.update({str(i):[]})
    start_point_list = [0 for i in range(7)]
    while True:
        print(start_point_list[0])
        random_number_A_list = [random.randint(70, 100) for i in range(7)]
        random_number_B_list = [random.randint(5, 10) for i in range(7)]

        for i in range(7):
            ts = int(dt.datetime.utcnow().timestamp())
            if start_point_list[i] + random_number_A_list[i] > length_list[i]:
                data_to_be_stream_x = data[str(i+1)][start_point_list[i]:] + \
                    data[str(i+1)][:(start_point_list[i] + random_number_A_list[i] - length_list[i])]
                start_point_list[i] = start_point_list[i] + random_number_A_list[i] - length_list[i]
            else:
                data_to_be_stream_x = data[str(i+1)][start_point_list[i]:(start_point_list[i] + random_number_A_list[i])]
                start_point_list[i] += random_number_A_list[i]
            if start_point_list[i] + random_number_B_list[i] > length_list[i]:
                data_to_be_stream_y = data[str(i+1)][start_point_list[i]:] + \
                    data[str(i+1)][:(start_point_list[i] + random_number_B_list[i] - length_list[i])]
                start_point_list[i] = start_point_list[i] + random_number_B_list[i] - length_list[i]
            else:
                data_to_be_stream_y = data[str(i+1)][start_point_list[i]:start_point_list[i] + random_number_B_list[i]]
                start_point_list[i] += random_number_B_list[i]
            B[str(i+1)] = data_to_be_stream_y
            for u in range(len(data_to_be_stream_x)):
                data_to_be_stream_x[u]["ts"] = ts
            A[str(i+1)] = data_to_be_stream_x
        data_to_be_sent = {}
        for i in range(1,8):
            data_to_be_sent[str(i)] = A[str(i)] + B_pending[str(i)]
        publish_message(flightProducer, topic, data_to_be_sent)
        B_pending = B
        sleep(5)
















































































#
