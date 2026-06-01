import socket, json, time, csv, re, math
from datetime import datetime
import argparse
import os
from typing import Optional

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



def pick_csv_from_user(cli_path: Optional[str] = None):
    if cli_path and os.path.exists(cli_path):
        return cli_path

    def _csv_has_axis_act(path: str) -> bool:
        try:
            with open(path, "r", encoding="utf-8") as f:
                first_line = f.readline()
                if not first_line:
                    return False
                if ";" in first_line and "," not in first_line:
                    delimiter = ";"
                elif "," in first_line:
                    delimiter = ","
                else:
                    delimiter = ";"
            with open(path, newline="", encoding="utf-8") as f:
                reader = csv.reader(f, delimiter=delimiter)
                headers = next(reader, [])
            norm = {h.strip().lower() for h in headers if isinstance(h, str)}
            return "axis_act" in norm
        except Exception:
            return False

    possible_paths = [
        # Canonical injected 900s dataset (15:16-15:31) — keeps live ON aligned
        # with static RF / PCA / AE results that were all regenerated from it.
        "data/kuka_log_900scnd_injected.csv",
        "../data/kuka_log_900scnd_injected.csv",
        # Legacy canta dataset (14:33-14:43) — kept only as a fallback.
        "data/kuka_log600_scnd_20hz(canta)_injected.csv",
        "../data/kuka_log600_scnd_20hz(canta)_injected.csv",
        "data/kuka_log600_scnd_20hz(canta).csv",
        "../data/kuka_log600_scnd_20hz(canta).csv",
        # Optional: larger raw dataset (may yield different calibration scale).
        "autoencoder/Data_for_Train/kuka_log_900scnd_100hz(=5,8hz).csv",
        "../autoencoder/Data_for_Train/kuka_log_900scnd_100hz(=5,8hz).csv",
    ]
    for p in possible_paths:
        if os.path.exists(p):
            return p
    # Try searching the data directory
    for data_dir in ["data", "../data", "mindtwin/data", "../mindtwin/data"]:
        if os.path.exists(data_dir):
            files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith(".csv")]
            if files:
                files.sort(key=lambda fp: (not _csv_has_axis_act(fp), fp))
                return files[0]
    # Fallback to tkinter if available
    try:
        from tkinter import Tk
        from tkinter.filedialog import askopenfilename
        root = Tk()
        root.withdraw()
        path = askopenfilename(
            title="Torque CSV seç (Google Drive içinden)",
            filetypes=[("CSV files", "*.csv")]
        )
        if path:
            return path
    except Exception:
        pass
    raise RuntimeError("Torque CSV file could not be found automatically.")

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

def detect_axis_act_column(headers) -> Optional[str]:
    norm = {k.lower(): k for k in headers if isinstance(k, str)}
    return norm.get("axis_act")


def detect_vel_axis_act_cols(headers) -> Optional[list]:
    """VEL_AXIS_ACT_A1..A6 kolonlarını tespit et. Tümü yoksa None döndür."""
    norm = {k.lower(): k for k in headers if isinstance(k, str)}
    cols = []
    for i in range(1, 7):
        key = f"vel_axis_act_a{i}"
        if key in norm:
            cols.append(norm[key])
        else:
            return None
    return cols


def detect_curr_cols(headers) -> Optional[list]:
    """CURR_A1..A6 kolonlarını tespit et. Tümü yoksa None döndür."""
    norm = {k.lower(): k for k in headers if isinstance(k, str)}
    cols = []
    for i in range(1, 7):
        key = f"curr_a{i}"
        if key in norm:
            cols.append(norm[key])
        else:
            return None
    return cols


def parse_axis_act(raw: str) -> Optional[list[float]]:
    """Parse 'A1 45.23 A2 -12.34 ...' → 6 joint angles in radians. Returns None on failure."""
    tokens = re.findall(r'A(\d)\s+([-\d.]+(?:[Ee][-+]?\d+)?)', raw)
    q = [0.0] * 6
    found = 0
    for idx_str, val_str in tokens:
        idx = int(idx_str) - 1
        if 0 <= idx < 6:
            import math as _m
            q[idx] = float(val_str) * (_m.pi / 180.0)  # deg → rad
            found += 1
    return q if found > 0 else None


def _parse_dt(rows) -> float:
    """Timestamp'lardan medyan dt (saniye) hesapla. Başarısızsa 0.01 (100 Hz) döndür."""
    from datetime import datetime
    ts_list = []
    for r in rows:
        raw = r.get("timestamp") or r.get("Timestamp") or ""
        try:
            ts_list.append(datetime.fromisoformat(raw.replace(" ", "T")))
        except Exception:
            break
    if len(ts_list) == len(rows) and len(ts_list) > 1:
        import numpy as _np
        diffs = [(ts_list[i+1] - ts_list[i]).total_seconds() for i in range(len(ts_list)-1)]
        dt = float(_np.median(diffs))
        return dt if dt > 0 else 0.01
    return 0.01  # 100 Hz fallback


def precompute_kinematics(rows, axis_act_col: str, vel_cols=None):
    """
    q (rad), q_dot (rad/s) ve q_ddot (rad/s²) ön-hesaplar.

    vel_cols verilirse: VEL_AXIS_ACT (deg/s → rad/s) kullanılır, tek türev yeterli.
    verilmezse: AXIS_ACT'tan SG + double-gradient (eski yöntem, fallback).
    """
    try:
        import numpy as np
        from scipy.signal import savgol_filter
    except ImportError:
        print("[producer] scipy not installed — joint kinematics will not be sent")
        return None, None, None

    q_all = []
    for r in rows:
        raw = r.get(axis_act_col, "") or ""
        parsed = parse_axis_act(raw)
        q_all.append(parsed if parsed is not None else [0.0] * 6)

    q_arr = np.array(q_all, dtype=float)  # (N, 6)
    win = min(7, len(q_arr) if len(q_arr) % 2 == 1 else len(q_arr) - 1)
    q_smooth = savgol_filter(q_arr, window_length=win, polyorder=2, axis=0) if win >= 3 else q_arr

    dt = _parse_dt(rows)

    if vel_cols is not None:
        # Ölçülmüş hız: deg/s → rad/s, tek türev → ivme
        vel_raw = np.array(
            [[to_float(r.get(c)) or 0.0 for c in vel_cols] for r in rows],
            dtype=float
        ) * (3.141592653589793 / 180.0)
        q_dot_arr  = savgol_filter(vel_raw, window_length=win, polyorder=2, axis=0) if win >= 3 else vel_raw
        q_ddot_arr = np.gradient(q_dot_arr, dt, axis=0)
    else:
        # Fallback: pozisyondan çift türev
        q_dot_arr  = np.gradient(q_smooth, dt, axis=0)
        q_ddot_arr = np.gradient(q_dot_arr, dt, axis=0)

    return q_smooth.tolist(), q_dot_arr.tolist(), q_ddot_arr.tolist()  # q_smooth: eğitimle tutarlı


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
    parser = argparse.ArgumentParser(description="MindTwin torque producer (sends torque + optional kinematics for RF)")
    parser.add_argument("--csv", default=None, help="CSV file path. If omitted, auto-detects or opens file dialog.")
    args = parser.parse_args()

    CSV_PATH = pick_csv_from_user(args.csv)
    rows = load_rows(CSV_PATH)

    # CSV header'a göre joint kolonlarını otomatik algıla
    global JOINT_KEYS
    JOINT_KEYS = detect_joint_keys(rows[0].keys())
    JOINTS = len(JOINT_KEYS)

    print(f"[producer] Selected CSV: {CSV_PATH}")
    print(f"[producer] Using torque columns: {JOINT_KEYS}")

    # SCALE kalibrasyonu
    scale, p99 = calibrate_scale(rows)
    print(
        f"[producer] Calibrated SCALE={scale:.4f} "
        f"from p99(diff)={p99:.4f} to keep thr={TARGET_THR}"
    )

    # Kinematik ön-hesaplama (graybox RF için — AXIS_ACT varsa)
    axis_act_col = detect_axis_act_column(rows[0].keys())
    q_list = None
    q_dot_list = None
    q_ddot_list = None
    if axis_act_col:
        vel_cols = detect_vel_axis_act_cols(rows[0].keys())
        if vel_cols:
            print(f"[producer] VEL_AXIS_ACT kolonları bulundu — ölçülmüş hız kullanılıyor (daha doğru q_ddot)")
        else:
            print(f"[producer] VEL_AXIS_ACT yok — AXIS_ACT'tan çift türev kullanılıyor")
        print(f"[producer] AXIS_ACT kolonu: '{axis_act_col}' — kinematik hesaplanıyor ...")
        q_list, q_dot_list, q_ddot_list = precompute_kinematics(rows, axis_act_col, vel_cols=vel_cols)
        if q_list is not None:
            print("[producer] q, q_dot ve q_ddot hazır — graybox RF (ters dinamik) aktif")
        else:
            print("[producer] q hesabı başarısız — graybox RF devre dışı")
    else:
        print("[producer] AXIS_ACT kolonu yok — graybox RF bu CSV için devre dışı")

    curr_cols = detect_curr_cols(rows[0].keys())
    if curr_cols:
        print(f"[producer] CURR kolonları bulundu — motor akımı gönderilecek: {curr_cols}")
    else:
        print("[producer] CURR kolonları yok — motor akımı gönderilmeyecek")

    # BRAKE_SIG filtresi — eğitimde sadece operasyonel (BRAKE_SIG==63) frame'ler kullanıldı
    norm_keys = {k.lower(): k for k in rows[0].keys()}
    brake_col = norm_keys.get("brake_sig")
    if brake_col is not None:
        print(f"[producer] BRAKE_SIG kolonu bulundu ('{brake_col}') — yalnızca BRAKE_SIG==63 frame'leri gönderilecek")
    else:
        print("[producer] BRAKE_SIG kolonu yok — tüm frame'ler gönderilecek (idle filtresi uygulanamıyor)")

    # Consumer henüz hazır olmayabilir — en fazla 10 kez dene
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    for _attempt in range(10):
        try:
            sock.connect((HOST, PORT))
            print(f"[producer] Consumer'a bağlandı ({HOST}:{PORT})")
            break
        except OSError:
            if _attempt < 9:
                print(f"[producer] Bağlantı bekleniyor ({_attempt+1}/10) — consumer hazır değil, 3s sonra tekrar...")
                time.sleep(3)
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            else:
                raise RuntimeError(f"[producer] Consumer'a bağlanılamadı ({HOST}:{PORT}). Consumer çalışıyor mu?")

    frame = 0
    idx = 0
    ideal = [0.0] * JOINTS

    while True:
        r = rows[idx]

        # Boşta (idle) frame'leri atla — model yalnızca operasyonel frame'lerle eğitildi
        if brake_col is not None:
            bv = to_float(r.get(brake_col))
            if bv is not None and bv != 63:
                idx = (idx + 1) % len(rows)
                continue

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
            "torque_actual": actual,
            # Send raw torque + calibration scale so consumer can normalize per-model consistently.
            "torque_actual_raw": actual_raw,
            "producer_scale": scale,
        }

        # Eklem açıları, hızları ve ivmeleri — graybox RF (ters dinamik) için
        if q_list is not None and q_dot_list is not None:
            pkt["q"]      = q_list[idx]
            pkt["q_dot"]  = q_dot_list[idx]
            if q_ddot_list is not None:
                pkt["q_ddot"] = q_ddot_list[idx]

        # Motor akımları — CURR özelliği için
        if curr_cols:
            pkt["curr"] = [to_float(r.get(c)) or 0.0 for c in curr_cols]

        sock.sendall((json.dumps(pkt) + "\n").encode())
        print(f"sent frame {frame}")

        frame += 1
        idx += 1
        if idx >= len(rows):
            idx = 0

        time.sleep(SEND_INTERVAL)


if __name__ == "__main__":
    main()
