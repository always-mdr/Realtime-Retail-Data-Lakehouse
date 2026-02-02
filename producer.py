import json
import time
import random
from faker import Faker
from kafka import KafkaProducer

# 1. Initialize Faker to generate fake data
fake = Faker()

# 2. Configure the Kafka Producer
# 'bootstrap_servers' tells Python where to find Redpanda.
# 'value_serializer' automatically converts our dictionary to JSON bytes.
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_order():
    """Generates a dictionary representing a fake retail order."""
    return {
        "order_id": fake.uuid4(),
        "customer_id": fake.uuid4(),
        "product_id": random.choice(['PROD-001', 'PROD-002', 'PROD-003', 'PROD-004']),
        "price": round(random.uniform(10.0, 100.0), 2),
        "quantity": random.randint(1, 5),
        "timestamp": str(fake.date_time_this_year())
    }

print("🛍️  Starting Retail Order Stream... (Press Ctrl+C to stop)")

try:
    while True:
        # 3. Generate a fake order
        order = generate_order()
        
        # 4. Send data to the topic named 'retail_orders'
        # The producer is asynchronous; it sends data in the background.
        producer.send('retail_orders', value=order)
        
        print(f"Sent: {order['order_id']} | ${order['price']}")
        
        # 5. Sleep for 1 second to simulate real-time traffic
        time.sleep(1)
        
except KeyboardInterrupt:
    print("\n🛑 Stream stopped.")
    producer.close()