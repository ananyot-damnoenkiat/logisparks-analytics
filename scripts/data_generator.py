import json
import random
import os
from faker import Faker
from datetime import datetime

# Initialize Faker
fake = Faker()

def generate_logistics_data(num_records=1000, output_path='/opt/airflow/data/raw_data.json'):
    """
    Generates synthetic logistics data and saves it as JSON.
    """
    data = []
    print(f"Generating {num_records} records...")
    
    for _ in range(num_records):
        record = {
            "transaction_id": fake.uuid4(),
            "date": datetime.now().strftime("%Y-%m-%d"),
            "origin": fake.city(),
            "destination": fake.city(),
            "weight_kg": round(random.uniform(5.0, 500.0), 2),
            "cost_usd": round(random.uniform(50.0, 2000.0), 2),
            "priority": random.choice(["Standard", "Express", "Same-Day"])
        }
        data.append(record)
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Write to file
    with open(output_path, 'w') as f:
        json.dump(data, f)
    
    print(f"Data successfully saved to {output_path}")

if __name__ == "__main__":
    generate_logistics_data()