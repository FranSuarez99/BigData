from confluent_kafka import Producer
from csv import reader
import time

my_topic = "test"
p = Producer({'bootstrap.servers': 'localhost:9092'})
numRows = 0

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

with open('dataClean/part-00000-bb7e0654-0ae0-4562-8b97-e3ac1ad04e52-c000.csv', 'r') as csv_file:
    #reads the first cvs file
    header = next(csv_file)
    for row in csv_file:
        if row != header:
            if numRows < 1500:
                time.sleep(0.3)
                #print(row)
                #p.poll(0)
                p.produce(my_topic, row.encode('utf8'), callback=delivery_report)
"""
with open('dataClean/part-00001-bb7e0654-0ae0-4562-8b97-e3ac1ad04e52-c000.csv', 'r') as csv_file:
    #reads the second cvs file
    header = next(csv_file)
    for row in csv_file:
        if row != header:
            time.sleep(0.3)
            #print(row)
            #p.poll(0)
            p.produce(my_topic, row.encode('utf8'), callback=delivery_report)

with open('dataClean/part-00002-bb7e0654-0ae0-4562-8b97-e3ac1ad04e52-c000.csv', 'r') as csv_file:
    #reads the third cvs file
    header = next(csv_file)
    for row in csv_file:
        if row != header:
            time.sleep(0.3)
            #print(row)
            #p.poll(0)
            p.produce(my_topic, row.encode('utf8'), callback=delivery_report)

with open('dataClean/part-00003-bb7e0654-0ae0-4562-8b97-e3ac1ad04e52-c000.csv', 'r') as csv_file:
    #reads the last cvs file
    header = next(csv_file)
    for row in csv_file:
        if row != header:
            time.sleep(0.3)
            #print(row)
            #p.poll(0)
            p.produce(my_topic, row.encode('utf8'), callback=delivery_report)
"""
p.flush()