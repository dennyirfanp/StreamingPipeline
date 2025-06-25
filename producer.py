from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta
import uuid

class PurchasingEventProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='purchasing-events'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.topic = topic
    
    def generate_purchase_event(self):
        """Generate a random purchase event"""
        return {
            "event_id": str(uuid.uuid4()),
            "user_id": f"user_{random.randint(1, 1000)}",
            "product_id": f"product_{random.randint(1, 100)}",
            "amount": round(random.uniform(10.0, 500.0), 2),
            "timestamp": datetime.now().isoformat(),
            "currency": "USD"
        }
    
    def start_producing(self, interval=2):
        """Start producing events at specified interval"""
        print(f"Starting to produce purchasing events to topic: {self.topic}")
        try:
            while True:
                event = self.generate_purchase_event()
                self.producer.send(self.topic, value=event)
                print(f"Sent: {event}")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("Stopping producer...")
        finally:
            self.producer.close()

if __name__ == "__main__":
    producer = PurchasingEventProducer()
    producer.start_producing()