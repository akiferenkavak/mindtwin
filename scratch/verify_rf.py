import json
import os

rf_data = json.load(open("artifacts/rf_results.json"))
overall = rf_data.get("overall", [])
print(f"Total overall: {len(overall)}")
print(f"Frame 1 overall: {overall[1]}")

for i in range(1, 7):
    feat = f"TORQUE_A{i}"
    l = rf_data.get("per_feature", {}).get(feat, [])
    print(f"{feat} Frame 1: {l[1]}")
