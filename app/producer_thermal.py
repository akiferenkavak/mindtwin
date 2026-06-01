# producer_thermal_csv.py
# CSV'deki MOT_TEMP_A1..A6 kolonlarından
# thermal (min / mean / max) üretip consumer'a gönderir

import csv
import json
import time
import socket
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional
from tkinter import Tk
from tkinter.filedialog import askopenfilename

HOST = "127.0.0.1"
PORT = 8765
SEND_INTERVAL = 0.5  # saniye

JOINT_TEMP_KEYS = [
    "mot_temp_a1",
    "mot_temp_a2",
    "mot_temp_a3",
    "mot_temp_a4",
    "mot_temp_a5",
    "mot_temp_a6",
]

@dataclass
class FramePacket:
    image_path: Optional[str]
    timestamp: str
    t_min: float
    t_max: float
    t_mean: float
    frame_no: int

def pick_csv():
    import os
    possible_paths = [
        # Canonical injected 900s dataset (15:16-15:31) — keeps live ON aligned
        # with the static RF / PCA / AE results.
        "data/kuka_log_900scnd_injected.csv",
        "../data/kuka_log_900scnd_injected.csv",
        # Legacy canta dataset (14:33-14:43) — fallback only.
        "data/kuka_log600_scnd_20hz(canta)_injected.csv",
        "../data/kuka_log600_scnd_20hz(canta)_injected.csv",
        "data/kuka_log600_scnd_20hz(canta).csv",
        "../data/kuka_log600_scnd_20hz(canta).csv",
    ]
    for p in possible_paths:
        if os.path.exists(p):
            return p
    # Try searching the data directory
    for data_dir in ["data", "../data", "mindtwin/data", "../mindtwin/data"]:
        if os.path.exists(data_dir):
            files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith(".csv")]
            if files:
                return files[0]
    # Fallback to tkinter if available
    try:
        from tkinter import Tk
        from tkinter.filedialog import askopenfilename
        root = Tk()
        root.withdraw()
        path = askopenfilename(
            title="Google Drive içinden motor sıcaklık CSV seç",
            filetypes=[("CSV files", "*.csv")]
        )
        if path:
            return path
    except Exception:
        pass
    raise RuntimeError("Thermal CSV file could not be found automatically.")

def to_float(x):
    try:
        return float(x)
    except:
        return None

def main():
    csv_path = pick_csv()
    print("[thermal] Selected CSV:", csv_path)

    with open(csv_path, newline="", encoding="utf-8") as f:
        first = f.readline()
        f.seek(0)
        delimiter = ";" if ";" in first else ","
        reader = csv.DictReader(f, delimiter=delimiter)
        rows = list(reader)

    # Header normalize
    headers = {h.lower(): h for h in rows[0].keys() if h}

    temp_cols = []
    for k in JOINT_TEMP_KEYS:
        if k in headers:
            temp_cols.append(headers[k])

    if len(temp_cols) < 2:
        raise RuntimeError(
            f"MOT_TEMP kolonları bulunamadı.\nBulunanlar: {list(headers.keys())}"
        )

    print("[thermal] Using temp columns:", temp_cols)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    for _attempt in range(10):
        try:
            sock.connect((HOST, PORT))
            print(f"[thermal] Consumer'a bağlandı ({HOST}:{PORT})")
            break
        except OSError:
            if _attempt < 9:
                print(f"[thermal] Bağlantı bekleniyor ({_attempt+1}/10) — 3s sonra tekrar...")
                time.sleep(3)
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            else:
                raise RuntimeError(f"[thermal] Consumer'a bağlanılamadı ({HOST}:{PORT}). Consumer çalışıyor mu?")

    frame = 0
    for r in rows:
        temps = []
        for col in temp_cols:
            v = to_float(r.get(col))
            if v is not None:
                temps.append(v)

        if not temps:
            continue

        pkt = FramePacket(
            image_path=None,
            timestamp=datetime.now().isoformat(),
            t_min=min(temps),
            t_max=max(temps),
            t_mean=sum(temps) / len(temps),
            frame_no=frame
        )

        sock.sendall((json.dumps(asdict(pkt)) + "\n").encode())
        print(f"[thermal] sent frame {frame}")

        frame += 1
        time.sleep(SEND_INTERVAL)

    sock.close()
    print("[thermal] done")

if __name__ == "__main__":
    main()
