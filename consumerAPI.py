from kafka import KafkaConsumer
from json import loads
from pymongo import MongoClient

consumer = KafkaConsumer(
    'topic_test',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group-id',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["SSD-NEOS"]
collection = db["API.Sentry"]

for event in consumer:
    event_data = event.value
    collection.insert_one(event_data)
