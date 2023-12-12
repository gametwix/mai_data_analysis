from kafka import KafkaConsumer
import asyncio
import json
import logging

def main():
    topic = "topic"

    logger = logging.getLogger("consumer")

    consumer = KafkaConsumer(topic,
                         group_id='default',
                         bootstrap_servers=['localhost:29092','localhost:39092'])

    for message in consumer:
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                            message.offset, message.key,
                                            message.value))

if __name__ == "__main__":
    main()