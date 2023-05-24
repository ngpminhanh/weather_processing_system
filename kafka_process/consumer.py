from kafka import KafkaConsumer
from json import loads
import sys

bootstrap_servers = ['localhost:9092']
topicname = 'weather6'
consumer = KafkaConsumer(topicname, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest', value_deserializer =lambda x:loads(x.decode('utf-8')))
limit = 1
count = 0
# try:
for message in consumer:
    print(message.value)
    count = count + 1
    if(count == limit):
        break
# except KeyboardInterrupt:  
#     sys.exit() 