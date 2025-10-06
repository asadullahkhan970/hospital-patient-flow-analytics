import json
import random
import uuid
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

# ================= CONFIGURATION =================
# Running mode: "azure"
MODE = "azure"

# ---------- Azure Event Hubs ----------
EVENTHUBS_NAMESPACE = "<<NAMESPACE_HOSTNAME>>"   # e.g., myhub.servicebus.windows.net
EVENT_HUB_NAME = "<<EVENT_HUB_NAME>>"
CONNECTION_STRING = "<<NAMESPACE_CONNECTION_STRING>>"
BOOTSTRAP_SERVERS = f"{EVENTHUBS_NAMESPACE}:9093"

# ================= PRODUCER SETUP =================
producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="$ConnectionString",
    sasl_plain_password=CONNECTION_STRING,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(2, 0, 0)  # optional but can help with compatibility
)

# ================= DATA GENERATION =================
departments = ["Emergency", "Surgery", "ICU", "Pediatrics", "Maternity", "Oncology", "Cardiology"]
genders = ["Male", "Female"]

def inject_dirty_data(record):
    if random.random() < 0.05:
        record["age"] = random.randint(101, 150)
    if random.random() < 0.05:
        record["admission_time"] = (datetime.utcnow() + timedelta(hours=random.randint(1, 72))).isoformat()
    return record

def generate_patient_event():
    admission_time = datetime.utcnow() - timedelta(hours=random.randint(0, 72))
    discharge_time = admission_time + timedelta(hours=random.randint(1, 72))
    event = {
        "patient_id": str(uuid.uuid4()),
        "gender": random.choice(genders),
        "age": random.randint(1, 100),
        "department": random.choice(departments),
        "admission_time": admission_time.isoformat(),
        "discharge_time": discharge_time.isoformat(),
        "bed_id": random.randint(1, 500),
        "hospital_id": random.randint(1, 7)
    }
    return inject_dirty_data(event)

# ================= MAIN LOOP =================
if __name__ == "__main__":
    print(f"Sending events to AZURE Event Hub '{EVENT_HUB_NAME}'...")
    while True:
        event = generate_patient_event()
        producer.send(EVENT_HUB_NAME, event)
        producer.flush()
        print(f"✅ Sent: {event}")
        time.sleep(1)
