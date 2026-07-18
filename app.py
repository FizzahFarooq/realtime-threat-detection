from flask import Flask, render_template, jsonify
import json
import os
from datetime import datetime

# Python custom system properties fixing double underscore syntax
app = Flask(__name__)
JSON_PATH = "alerts_data.json"

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/api/alerts")
def get_alerts():
    if os.path.exists(JSON_PATH):
        try:
            with open(JSON_PATH, "r") as f:
                data = json.load(f)
        except Exception:
            return jsonify([])

        processed_data = []
        for d in data:
            # Window schemas checking
            ts_str = d.get("window_end") or d.get("timestamp") or ""
            source_ip = d.get("source_ip", "Unknown Node")
            ml_attack_count = d.get("ml_attack_count", 1)

            # Strict formatting for Spark Timestamp to Frontend D3 structure
            formatted_date = "2026-07-18"
            formatted_time = "00:00:00"

            if ts_str:
                try:
                    # if  Spark format "2026-07-18 16:15:00"
                    if " " in ts_str:
                        dt_obj = datetime.strptime(ts_str.strip(), "%Y-%m-%d %H:%M:%S")
                    else:
                        # Standard ISO parser
                        dt_obj = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    formatted_date = dt_obj.strftime("%Y-%m-%d")
                    formatted_time = dt_obj.strftime("%H:%M:%S")
                except Exception:
                    # if both  fail then safe substring extraction
                    try:
                        parts = ts_str.split(" ")
                        if len(parts) == 2:
                            formatted_date = parts[0]
                            formatted_time = parts[1]
                    except Exception:
                        pass

            processed_data.append({
                "source_ip": source_ip,
                "ml_attack_count": int(ml_attack_count),
                "date": formatted_date,
                "time": formatted_time,
                "timestamp": ts_str
            })

        return jsonify(processed_data)
    return jsonify([])

if __name__ == "__main__":
    app.run(debug=True, port=8085)
