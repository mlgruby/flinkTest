from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
import random
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_producer(retries=5, delay=5):
    servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9093').split(',')
    for attempt in range(retries):
        try:
            logger.info(f"Attempting to connect to Kafka broker (attempt {attempt + 1})")
            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all'
            )
            logger.info("Successfully connected to Kafka broker")
            return producer
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka broker: {str(e)}")
            if attempt < retries - 1:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error("Max retries reached. Exiting.")
                raise

def generate_event():
    return {
        "id": f"user_{random.randint(1, 100000)}",
        "timestamp": int(time.time() * 1000),
        "data": f"Event data {random.randint(1, 1000)}"
    }

def main():
    producer = create_producer()
    try:
        while True:
            event = generate_event()
            producer.send('input-topic', event)
            logger.info(f"Sent event: {event}")
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()