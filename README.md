# MindTwin

**MindTwin** is a lightweight digital twin–based monitoring system designed for industrial robots.  
The system enables **real-time replay and visualization of torque and thermal data** using user-provided CSV files.

Instead of relying on simulated or hardcoded signals, MindTwin works directly with **real operational data**, allowing users to analyze anomalies under realistic conditions.

---

## 🚀 Key Features

- 📊 **CSV-based data ingestion**  
  Load torque and thermal data directly from CSV files (e.g., exported robot logs).

- ☁️ **Google Drive integration**  
  CSV files can be selected directly from Google Drive (via Drive for Desktop), enabling seamless use of shared or cloud-stored datasets.

- ⏱ **Real-time replay**  
  Logged data is streamed frame-by-frame to simulate live operation.

- 🌡 **Thermal monitoring**  
  Min / mean / max temperature values are computed on the fly and visualized in the Thermal UI.

- ⚙️ **Torque monitoring**  
  Joint-level torque deviations are detected and visualized in real time.

- 🚨 **Event & anomaly logging**  
  Detected anomalies are logged and displayed instantly in the Events dashboard.

---

## 🧠 System Overview

MindTwin follows a simple producer–consumer architecture:

- **Producers**
  - Read CSV files (thermal or torque data)
  - Stream data as JSON frames over TCP

- **Consumer (Backend)**
  - Receives streamed data
  - Detects anomalies based on thresholds
  - Serves live data to the web UI using FastAPI & WebSockets

- **Web UI**
  - Thermal dashboard
  - Torque dashboard
  - Events & anomaly log

---

## 📁 Data Input

- Supported format: **CSV**
- Data source: **User-provided files**
- Typical workflow:
  1. Store CSV files locally or in Google Drive
  2. Select the desired file via file picker
  3. Replay data in real time through the UI

This design allows MindTwin to work with **any custom dataset**, without modifying the backend code.

---

## 🛠 Tech Stack

- **Backend:** Python, FastAPI, Uvicorn
- **Streaming:** TCP sockets
- **Frontend:** HTML, JavaScript
- **Data:** CSV-based logs
- **Deployment:** Localhost (research & prototyping oriented)

---

## 🎯 Use Cases

- Digital twin prototyping
- Offline analysis of robot logs
- Anomaly detection experiments
- Academic and industrial demonstrations

---

## 📌 Notes

MindTwin is designed as a **flexible research and demonstration tool**.  
Thresholds, data sources, and replay speed can be easily adapted to different experimental setups.

---

## 📂 Sample Datasets (Google Drive)

Example torque and thermal datasets used in this project are available on Google Drive:

🔗 **Google Drive Dataset Folder**  
https://drive.google.com/drive/folders/129q4hMMzVhQVXE9V8ysXHIrQGuOcl5sH

**Recommended file:**  
`kuka_log600_scnd_20hz (çanta kaldırıldı yerine koyuldu).csv`

This CSV file can be used for **both torque and thermal monitoring**, as it contains
the required torque and motor temperature columns.

These CSV files can be loaded directly into the system using **Google Drive for Desktop**
and selected via the file picker at runtime.

---

## 📷 Screenshots

*(See screenshots above for Thermal, Torque, and Events dashboards.)*


<img width="1455" height="830" alt="image" src="https://github.com/user-attachments/assets/aadcbc15-d6bb-4cf2-be35-9e986f9c24ab" />

<img width="1455" height="830" alt="image" src="https://github.com/user-attachments/assets/4abc55e2-1d8a-4cd0-a39b-e68bcf92efc5" />

<img width="1455" height="830" alt="image" src="https://github.com/user-attachments/assets/e4d1b0f3-153f-4f1f-bc7e-69cdf13939a6" />

<img width="1455" height="830" alt="image" src="https://github.com/user-attachments/assets/bab715fe-c84a-4dc9-be37-c808a4b6ca54" />


