from kafka import KafkaProducer
from kafka.errors import KafkaError
import asyncio
import json
import logging

def main():
    topic = "topic"
    producer = KafkaProducer(bootstrap_servers=['localhost:29092','localhost:39092'], value_serializer=lambda m: json.dumps(m).encode('ascii'))

    while True:
        key = input("Enter key: ")
        value = input("Enter value: ")
        future = producer.send(topic, {key : value})

        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
            print (record_metadata.topic)
            print (record_metadata.partition)
            print (record_metadata.offset)
        except KafkaError as e:
            # Decide what to do if produce request failed...
            logging.log(e)
            continue


if __name__ == "__main__":
    main()