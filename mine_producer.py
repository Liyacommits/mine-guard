import time
import json
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Mining IoT Data STream started... Press CTRL+C to exit")

#loop to simulate continuos live mine state

while True:
    #generating vibration metrics (normal is around 3.0 to 4.5 mm/s)
    #Spikes could be 5.5 to test if insurance kicks in
    simulated_vibration = round(random.uniform(2.5, 6.2), 2)

    payload = {
        "tenant_id": 1,
        "equipment_id": "Crusher_01",
        "vibration_mm_s": simulated_vibration,
        "timestamp": int(time.time())
    }

    producer.send("mine-iot-telemetry", value=payload)
    print(f"Sent Telemetry: {payload}")

    time.sleep(2)

