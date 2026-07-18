import time
import json
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

# Initialize Kafka Producer
# 'kafka:29092' or 'localhost:9092' depending on network routing
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'cyber_threats'

# Attack profiles to simulate realistic malicious intents
ATTACK_TYPES = ['DDOS', 'SQL_INJECTION', 'BRUTE_FORCE', 'MALWARE_BEACONING']

print("Threat Generator Service is live... Press Ctrl+C to stop.")

try:
    while True:
        # 1. Simulate base network traffic parameters
        is_attack = random.choices([0, 1], weights=[0.85, 0.15])[0] # 15% chance of anomaly

        log_entry = {
            # datetime.now(timezone.utc) se har dafa bilkul current server time generate hoga
            "timestamp": datetime.now(timezone.utc).isoformat(),
            # TESTING: Mapped to a small IP range (2 to 6) so sliding windows can aggregate counts properly
            "source_ip": f"192.168.1.{random.randint(2, 6)}",
            "dest_ip": f"10.0.0.{random.randint(5, 50)}",
            "packet_size": random.randint(40, 1500) if not is_attack else random.randint(5000, 65000),
            "connection_duration": round(random.uniform(0.1, 5.0), 3) if not is_attack else round(random.uniform(30.0, 180.0), 3),
            "is_malicious": is_attack,
            "attack_type": random.choice(ATTACK_TYPES) if is_attack else "NORMAL"
        }

        # 2. Push telemetry log payload to the Kafka broker stream
        producer.send(TOPIC_NAME, value=log_entry)
        print(f" Sent Log -> IP: {log_entry['source_ip']} | Type: {log_entry['attack_type']} | Size: {log_entry['packet_size']}")

        # TESTING: Reduced interval to flood data rapidly and populate streaming batches
        time.sleep(0.05)

except KeyboardInterrupt:
    print("\n Threat Generator gracefully stopped.")
finally:
    producer.close()
