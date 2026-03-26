import socket, json, time, csv
from datetime import datetime
from tkinter import Tk
from tkinter.filedialog import askopenfilename
import math

HOST = "127.0.0.1"
PORT = 8766

SEND_INTERVAL = 0.5   # UI'ya kaç saniyede bir frame gönderilsin
TARGET_THR = 0.45     # UI'daki threshold ile aynı kalsın
Q = 0.99              # p99 ile kalibrasyon (spike = anomaly)
CALIB_ROWS = 400      # SCALE'i hesaplamak için ilk kaç satırı kullanacağız
JOINT_KEYS = [f"torque_a{i}" for i in range(1, 7)]

def detect_joint_keys(header_keys):
    # string olmayanları at
    keys = [k for k in header_keys if isinstance(k, str)]

    # büyük/küçük harf normalize
    norm = {k.lower(): k for k in keys}

    # torque_a1..a6
    cand1 = [f"torque_a{i}" for i in range(1, 7)]
    if all(k in norm for k in cand1):
        return [norm[k] for k in cand1]

    # avg_torque_a1..a6
    cand2 = [f"avg_torque_a{i}" for i in range(1, 7)]
    if all(k in norm for k in cand2):
        return [norm[k] for k in cand2]

    # partial avg_torque_ax
    cand3 = sorted(k for k in norm if k.startswith("avg_torque_a"))
    if cand3:
        return [norm[k] for k in cand3]

    raise RuntimeError(f"Torque kolonları bulunamadı. Header: {keys}")



def pick_csv_from_user():
    root = Tk()
    root.withdraw()
    path = askopenfilename(
        title="Torque CSV seç (Google Drive içinden)",
        filetypes=[("CSV files", "*.csv")]
    )
    if not path:
        raise SystemExit("CSV seçilmedi, çıkılıyor.")
    return path

def to_float(x):
    try:
        v = float(x)
        return v
    except:
        return None

def quantile(sorted_vals, q):
    """Basit quantile (0..1). sorted_vals sıralı olmalı."""
    if not sorted_vals:
        return None
    if q <= 0: return sorted_vals[0]
    if q >= 1: return sorted_vals[-1]
    pos = (len(sorted_vals) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_vals[lo]
    frac = pos - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac

def load_rows(csv_path):
    with open(csv_path, newline="", encoding="utf-8") as f:
        first_line = f.readline()
        f.seek(0)

        # Delimiter'ı GARANTİ tespit
        if ";" in first_line and "," not in first_line:
            delimiter = ";"
        elif "," in first_line:
            delimiter = ","
        else:
           
            delimiter = ";"

        reader = csv.DictReader(f, delimiter=delimiter)
        rows = list(reader)

    if not rows:
        raise RuntimeError("CSV boş ya da okunamadı.")

    return rows


def calibrate_scale(rows):
    """
    CSV'deki ardışık torque değişimlerinden (abs delta) p99 hesaplar,
    UI threshold = 0.45 olacak şekilde SCALE döndürür.
    """
    pooled_diffs = []
    prev = None

    take = min(len(rows), CALIB_ROWS)
    for i in range(take):
        r = rows[i]
        cur = [to_float(r.get(k)) for k in JOINT_KEYS]
        if any(v is None for v in cur):
            continue
        if prev is not None:
            for a, b in zip(cur, prev):
                pooled_diffs.append(abs(a - b))
        prev = cur

    pooled_diffs = [d for d in pooled_diffs if d is not None and math.isfinite(d)]
    pooled_diffs.sort()

    p = quantile(pooled_diffs, Q)
    if p is None or p <= 0:
        # fallback: scale = 1 (yine de çalışsın)
        return 1.0, 0.0

    scale = p / TARGET_THR
    return scale, p
def main():
    CSV_PATH = pick_csv_from_user()
    rows = load_rows(CSV_PATH)

    # 🔽 CSV header'a göre joint kolonlarını otomatik algıla
    global JOINT_KEYS
    JOINT_KEYS = detect_joint_keys(rows[0].keys())
    JOINTS = len(JOINT_KEYS)

    print(f"[producer] Selected CSV: {CSV_PATH}")
    print(f"[producer] Using torque columns: {JOINT_KEYS}")

    # 🔽 SCALE kalibrasyonu
    scale, p99 = calibrate_scale(rows)
    print(
        f"[producer] Calibrated SCALE={scale:.4f} "
        f"from p99(diff)={p99:.4f} to keep thr={TARGET_THR}"
    )

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))

    frame = 0
    idx = 0

    ideal = [0.0] * JOINTS

    while True:
        r = rows[idx]
        ts = r.get("timestamp") or datetime.now().isoformat()

        actual_raw = [to_float(r.get(k)) for k in JOINT_KEYS]
        if any(v is None for v in actual_raw):
            idx = (idx + 1) % len(rows)
            continue

        actual = [v / scale for v in actual_raw]

        pkt = {
            "frame_no": frame,
            "timestamp": ts,
            "torque_ideal": ideal,
            "torque_actual": actual
        }

        sock.sendall((json.dumps(pkt) + "\n").encode())
        print(f"sent frame {frame}")

        frame += 1
        idx += 1
        if idx >= len(rows):
            idx = 0

        time.sleep(SEND_INTERVAL)


if __name__ == "__main__":
    main()
