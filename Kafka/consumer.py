from time import sleep
from kafka import KafkaConsumer
import datetime as dt
import matplotlib.pyplot as plt
import statistics
import json
import time
import os
import datetime
# this line is needed for the inline display of graphs in Jupyter Notebook
%matplotlib notebook


topic = 'flightTopic'
def connect_kafka_consumer(topic_1):
    _consumer = None
    try:
         _consumer = KafkaConsumer(topic_1,
                                   consumer_timeout_ms=10000,
                                   auto_offset_reset='earliest',
                                   bootstrap_servers=['localhost:9092'],
                                   api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _consumer

def init_plots():
    try:
        width = 9.5
        height = 6
        fig = plt.figure(figsize=(width,height))
        fig.subplots_adjust(hspace=0.8)
        ax1 = fig.add_subplot(111)
        ax1.set_xlabel('Time')
        ax1.set_ylabel('Value')
        fig.suptitle('Real-time uniform stream data visualization with interesting points') # giving figure a title
        fig.show() # displaying the figure
        fig.canvas.draw() # drawing on the canvas
        return fig, ax1
    except Exception as ex:
        print(str(ex))

def consume_messages(consumer, fig, ax1):
    d1,d2,d3 = [],[],[]
    y1, y2, y3, x = [], [], [], []
    print('Waiting for messages')
    previous_time = datetime.datetime.now()
    for message in consumer:
        now_time = datetime.datetime.now()
        print((now_time-previous_time).total_seconds())
        if (now_time-previous_time).total_seconds() > 4.0:
            data = json.loads(message.value.decode('UTF-8'))
            d1.append(data["1"])
            d2.append(data["2"])
            d3.append(data["3"])
            if len(d1) >= 24:
                len_1 = 0
                for uu in d1:
                    len_1 += len(uu)
                len_2 = 0
                for uu in d2:
                    len_2 += len(uu)
                len_3 = 0
                for uu in d3:
                    len_3 += len(uu)
                y1.append(len_1)
                y2.append(len_2)
                y3.append(len_3)
                x.append(int(dt.datetime.utcnow().timestamp()))
                d1.pop(0)
                d2.pop(0)
                d3.pop(0)
            if len(x) > 6:
                y1.pop(0)
                y2.pop(0)
                y3.pop(0)
                x.pop(0)
            ax.clear()
            ax.plot(x, y1, color= "yellow")
            ax.plot(x, y2, color= "blue")
            ax.plot(x, y3, color= "red")
            fig.canvas.draw()
            previous_time = now_time




if __name__ == '__main__':

    consumer = connect_kafka_consumer("flightTopic")
    fig, ax1 = init_plots()
    consume_messages(consumer, fig, ax1)








## Trial

d1,d2,d3 = [],[],[]
y1, y2, y3, x = [], [], [], []
print('Waiting for messages')
previous_time = datetime.datetime.now()
for message in consumer:
    now_time = datetime.datetime.now()
    print((now_time-previous_time).total_seconds())
    if (now_time-previous_time).total_seconds() > 4.0:
        data = json.loads(message.value.decode('UTF-8'))
        d1.append(data["1"])
        d2.append(data["2"])
        d3.append(data["3"])
        if len(d1) >= 24:
            len_1 = 0
            for uu in d1:
                len_1 += len(uu)
            len_2 = 0
            for uu in d2:
                len_2 += len(uu)
            len_3 = 0
            for uu in d3:
                len_3 += len(uu)
            y1.append(len_1)
            y2.append(len_2)
            y3.append(len_3)
            x.append(int(dt.datetime.utcnow().timestamp()))
            d1.pop(0)
            d2.pop(0)
            d3.pop(0)
        if len(x) > 6:
            y1.pop(0)
            y2.pop(0)
            y3.pop(0)
            x.pop(0)

        previous_time = now_time

y1.pop(0)
len(y1)
y1
x
y2
y3
d1,d2,d3 = [],[],[]
x1, x2, x3, y = [], [], [], []
# print('Waiting for messages')
for message in consumer:
    data = json.loads(message.value.decode('UTF-8'))
    time = dt.datetime.utcnow().strftime("%H:%M:%S")
    y.append(time)
    try:
        d1 += data["1"]
    except:
        pass
    try:
        d2 += data["2"]
    except:
        pass
    try:
        d3 += data["3"]
    except:
        pass
    x1.append(len(d1))
    x2.append(len(d2))
    x3.append(len(d3))
    # we start plotting only when we have 10 data points
    if len(y) == 24:
        ax.clear()
        ax.plot(x1, y)
        ax.set_xlabel('Time')
        ax.set_ylabel('Value')
        ax.set_ylim(0,110)
        ax.set_yticks([0,20,40,60,80,100])
        fig.canvas.draw()
        x1.pop(0)
        d1.pop(0)
        x2.pop(0)
        d2.pop(0)
        x3.pop(0)
        d3.pop(0) # removing the item in the first position
        y.pop(0)
    print(time)


plt.close('all')


consumer.pol


print(datetime.datetime.now())
consumer = connect_kafka_consumer(topic)
a = 0
prev_time = datetime.datetime.now()
while True:
    b = consumer.poll()
    if len(b) >0:
        now_time = datetime.datetime.now()
        print((now_time-prev_time).total_seconds())
        prev_time = now_time
        a = b
for message in consumer:
    print(message.timestamp)
data = json.loads(a.value.decode('UTF-8'))
type(a.values())

prev_time = datetime.datetime.now()
for message in consumer:
    now_time = datetime.datetime.now()
    print((now_time-prev_time).total_seconds())
    prev_time = now_time


data = json.loads(message.value.decode('UTF-8'))
data["6"]

consumer.poll()


consumer.seek_to_end()
consumer = KafkaConsumer(topic,
                          consumer_timeout_ms=10000,
                          bootstrap_servers=['localhost:9092'])


consumer.poll()


import time

a = time.time()
b = time.time()
b - a

str(datetime.datetime.now().time())
a = datetime.datetime.now()
b = datetime.datetime.now()
(b-a).total_seconds()

'''def consume_messages(consumer, fig, ax1):
    try:
        d1,d2,d3 = [],[],[]
        y1, y2, y3, x = [], [], [], []
        # print('Waiting for messages')
        for message in consumer:
            data = json.loads(message.value.decode('UTF-8'))
            time = dt.datetime.utcnow().strftime("%H:%M:%S")
            x.append(time)
            try:
                d1.append(data["1"])
            except:
                pass
            try:
                d2.append(data["2"])
            except:
                pass
            try:
                d3.append(data["3"])
            except:
                pass
            len_1 = 0
            for uu in d1:
                len_1 += len(uu)
            len_2 = 0
            for uu in d2:
                len_2 += len(uu)
            len_3 = 0
            for uu in d3:
                len_3 += len(uu)
            y1.append(len_1)
            y2.append(len_2)
            y3.append(len_3)
            # we start plotting only when we have 10 data points
            if len(x) == 24:
                ax.clear()
                ax.plot(x, y1, color= "blue")
                ax.plot(x, y2, color= "red")
                ax.plot(x, y3, color = "yellow")
                ax.set_xlabel('Time')
                ax.set_ylabel('Value')
                ax.set_ylim(0,110)
                ax.set_yticks([0,20,40,60,80,100])
                fig.canvas.draw()
                y1.pop(0)
                d1.pop(0)
                y2.pop(0)
                d2.pop(0)
                y3.pop(0)
                d3.pop(0) # removing the item in the first position
                x.pop(0)
            print(time)

    except:
        print("wrong")'''







































































#
