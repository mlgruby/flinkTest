from kafka import KafkaConsumer
import json

# Kafka configuration
bootstrap_servers = ['localhost:9092']  # Replace with your Kafka broker(s)
topic_name = 'input-topic'  # Replace with your topic name

# Create Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',  # Replace with your consumer group id
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Read messages from the topic
print(f"Listening for messages on topic: {topic_name}")
for message in consumer:
    print(f"Received message: {message.value}")

# Close the consumer (this part will not be reached in this infinite loop example)
consumer.close()
