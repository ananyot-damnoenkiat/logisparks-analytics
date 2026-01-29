import json
import random
import time
from faker import Faker
from datetime import datetime

fake = Faker()

def generate_shipment_data(num_records):
    data = []
    for _ in range(num_records):
        record = {
            "shipment_id": fake.uuid4(),
            "origin": fake.city(),
            "destination": fake.city(),
            "weight_kg": round(random.uniform(1.0, 100.0), 2),
            "timestamp": datetime.now().isoformat(),
            "status": random.choice(["SHIPPED", "IN_TRANSIT", "DELIVERED", "DELAYED"]),
            "shipping_cost": round(random.uniform(10.0, 500.0), 2) 
        }
        data.append(record)
    return data

if __name__ == "__main__":
    # Generate 1000 records
    shipments = generate_shipment_data(1000)

    # Save to local JSON file
    filename = f"shipments_{int(time.time())}.json"
    with open(filename, "w") as f:
        json.dump(shipments, f)

    print(f"Generated {filename}")