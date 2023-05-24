from kafka import KafkaProducer
from csv import DictReader
import json
import sys
import time

# input_file = sys.argv[1]
input_file = "smallerCSV/2002.csv"
bootstrap_servers = ['localhost:9092']
topicname = 'weather6'


start = time.time()
print("Starting import file" + input_file + " " + str(start))
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
producer = KafkaProducer()

part = 0
#NOT DEFINE KEY MESSAGE, DEFAULT PARTITION IS 0
with open(input_file, "r", encoding="utf-8") as new_obj:
    csv_dict_reader = DictReader(new_obj)
    for row in csv_dict_reader:
        # part = int(row['year']) % 1000
        producer.send(topicname, json.dumps(row).encode('utf-8'))

# , partition=part
end = time.time()
total_time = end-start
print("Finishing import file " + input_file + " " + str(end))
print("Total time " + str(total_time) + "\n")

#PUT DATA TO SPECIFIC PARTITION
# part = 0
# prevY = ''
# with open(input_file, "r") as new_obj:
#     reader = DictReader(new_obj)
#     for row in reader:
#         part = int(row['year']) % 1000
#         msg = {
#             "date": row['date'],
#             "hour": row['hour'],
#             "prcp": row['prcp'],
#             "stp": row['stp'],
#             "smax": row['smax'],
#             "smin": row['smin'],
#             "gbrd": row['gbrd'],
#             "temp": row['temp'],
#             "dewp": row['dewp'],
#             "tmax": row['tmax'],
#             "tmin": row['tmin'],
#             "dmax": row['dmax'],
#             "dmin": row['dmin'],
#             "hmax": row['hmax'],
#             "hmin": row['hmin'],
#             "hmdy": row['hmdy'],
#             "wdct": row['wdct'],
#             "gust": row['gust'],
#             "wdsp": row['wdsp'],
#             "region": row['region'],
#             "state": row['state'],
#             "station": row['station'],
#             "station_code": row['station_code'],
#             "latitude": row['latitude'],
#             "longitude": row['longitude'],
#             "height": row['height']
#         }
#         ack = producer.send(topicname,value = msg, partition=part)
#         metadata = ack.get()
#         print(row['year'], metadata.topic, metadata.partition)

