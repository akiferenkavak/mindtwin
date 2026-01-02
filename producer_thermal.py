# producer_thermal_csv.py
# CSV'deki MOT_TEMP_A1..A6 kolonlarından
# thermal (min / mean / max) üretip consumer'a gönderir

import csv
import json
import time
import socket
from datetime import datetime
from dataclasses import dataclass, asdict
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
    image_path: str | None
    timestamp: str
    t_min: float
    t_max: float
    t_mean: float
    frame_no: int

def pick_csv():
    root = Tk()
    root.withdraw()
    path = askopenfilename(
        title="Google Drive içinden motor sıcaklık CSV seç",
        filetypes=[("CSV files", "*.csv")]
    )
    if not path:
        raise SystemExit("CSV seçilmedi.")
    return path

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
    sock.connect((HOST, PORT))
    print("[thermal] connected to consumer")

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
