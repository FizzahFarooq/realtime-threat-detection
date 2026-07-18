import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
import joblib

print("Simulating baseline network profile for training...")

# 1. Generate Normal Traffic Data (High volume, low packet size, low duration)
normal_packets = np.random.randint(40, 1500, size=1000)
normal_duration = np.random.uniform(0.1, 5.0, size=1000)
df_normal = pd.DataFrame({
    'packet_size': normal_packets,
    'connection_duration': normal_duration
})

# 2. Generate Malicious/Anomaly Traffic Data (Low volume, extreme sizes or durations)
anomaly_packets = np.random.randint(5000, 65000, size=150)
anomaly_duration = np.random.uniform(30.0, 180.0, size=150)
df_anomaly = pd.DataFrame({
    'packet_size': anomaly_packets,
    'connection_duration': anomaly_duration
})

# Combine into a single training dataset
df_train = pd.concat([df_normal, df_anomaly], ignore_index=True)

print(f" Training dataset prepared with {len(df_train)} network logs.")

# 3. Initialize and Fit Isolation Forest
# contamination=0.13 ka matlab hai hum expect kar rahe hain ~13% data anomalous ho sakta hai
model = IsolationForest(n_estimators=100, contamination=0.13, random_state=42)
model.fit(df_train[['packet_size', 'connection_duration']])

# 4. Serialize (Save) the model artifact
model_filename = 'isolation_forest_cyber.joblib'
joblib.dump(model, model_filename)

print(f" Success! Trained model artifact serialized and saved as: '{model_filename}'")
