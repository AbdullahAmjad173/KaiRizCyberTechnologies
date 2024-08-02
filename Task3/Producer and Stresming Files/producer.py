from time import sleep
from json import dumps
from kafka import KafkaProducer
import random

# Kafka broker address
bootstrap_servers = ['localhost:9092']

# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

# Simulation loop to produce messages
while True:
    # Simulate some data
    data = {
        "feature1": random.randint(1, 100),
        "feature2": random.random(),
        "feature3": random.choice(['A', 'B', 'C'])
    }
    
    # Send the message to Kafka
    producer.send('real-time-data', value=data)
    print(f"Sent message: {data}")

    # Delay for a random amount of time
    sleep(random.uniform(0.5, 2.0))
