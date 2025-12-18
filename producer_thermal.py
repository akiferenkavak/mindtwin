# producer.py
# Thermal camera capture_summary.txt dosyasını okuyup
# FramePacket JSON satırlarını TCP soketi üzerinden gönderir.
import os, re, json, time, socket
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional, Iterator

# ==== CONFIG ====
HOST = "127.0.0.1"
PORT = 8765
BASE_DIR = r"thermal_capture/thermal_capture"  
SUMMARY_PATH = os.path.join(BASE_DIR, "capture_summary.txt")
IMAGE_PATTERN = "frame_{:04d}.jpg"  # frame_0001.jpg gibi
STRICT_IMAGE_CHECK = True  # True ise dosya yoksa image_path=None

# ==== DATA MODEL ====
@dataclass
class FramePacket:
    image_path: Optional[str]
    timestamp: str         # ISO 8601 (e.g., "2025-11-10T13:16:25.141000")
    t_min: float
    t_max: float
    t_mean: float
    frame_no: int

# ==== PARSER ====
PAT_FRAME = re.compile(r"\bFrame[:\s]+(\d+)\b", re.IGNORECASE)
PAT_DATETIME = re.compile(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)")
PAT_MIN = re.compile(r"Min[:\s]+(-?\d+(?:\.\d+)?)", re.IGNORECASE)
PAT_MAX = re.compile(r"Max[:\s]+(-?\d+(?:\.\d+)?)", re.IGNORECASE)
PAT_MEAN = re.compile(r"Mean[:\s]+(-?\d+(?:\.\d+)?)", re.IGNORECASE)

def parse_summary(path: str) -> list[tuple[int, datetime, float, float, float]]:
    """capture_summary.txt içinden (frame_no, ts, min, max, mean) listesi döndürür"""
    results = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()

    # Her 'Frame:' blokunu kaba şekilde böl
    blocks = re.split(r"(?=Frame[:\s]+\d+)", text)
    for b in blocks:
        if not b.strip():
            continue
        m_f = PAT_FRAME.search(b)
        m_t = PAT_DATETIME.search(b)
        m_n = PAT_MIN.search(b)
        m_x = PAT_MAX.search(b)
        m_m = PAT_MEAN.search(b)
        if not (m_f and m_t and m_n and m_x and m_m):
            # eksik alan varsa bu bloğu atla
            continue
        frame_no = int(m_f.group(1))
        # "YYYY-MM-DD 13:16:25.141" formatını oku
        ts_str = f"{m_t.group(1)} {m_t.group(2)}"
        ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f") if "." in ts_str \
             else datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        tmin = float(m_n.group(1))
        tmax = float(m_x.group(1))
        tmean = float(m_m.group(1))
        results.append((frame_no, ts, tmin, tmax, tmean))

    # Zaman sırasına göre sırala (log karışıksa)
    results.sort(key=lambda x: x[1])
    return results

def packets_from_summary(rows: list[tuple[int, datetime, float, float, float]]) -> Iterator[FramePacket]:
    for frame_no, ts, tmin, tmax, tmean in rows:
        img_path = os.path.join(BASE_DIR, IMAGE_PATTERN.format(frame_no))
        if STRICT_IMAGE_CHECK and not os.path.isfile(img_path):
            img_path = None
        yield FramePacket(
            image_path=img_path,
            timestamp=ts.isoformat(),
            t_min=tmin,
            t_max=tmax,
            t_mean=tmean,
            frame_no=frame_no,
        )

# ==== REAL-TIME SENDER ====
def send_stream(packets: list[FramePacket]) -> None:
    if not packets:
        print("Gönderilecek paket bulunamadı.")
        return

    # TCP bağlantısı
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    print(f"[producer] bağlı: {HOST}:{PORT} | {len(packets)} paket gönderilecek")

    # 1x gerçek zamanlı: ardışık zaman farkına göre bekle
    prev_dt = datetime.fromisoformat(packets[0].timestamp)
    for i, p in enumerate(packets):
        cur_dt = datetime.fromisoformat(p.timestamp)
        wait = (cur_dt - prev_dt).total_seconds()
        if wait < 0:
            wait = 0.0
        if i > 0:
            time.sleep(wait)  # 1× hız

        line = json.dumps(asdict(p), ensure_ascii=False) + "\n"
        sock.sendall(line.encode("utf-8"))
        prev_dt = cur_dt
        print(f"[producer] sent frame={p.frame_no} t={p.timestamp} wait={wait:.3f}s")

    sock.shutdown(socket.SHUT_WR)
    sock.close()
    print("[producer] bitti.")

if __name__ == "__main__":
    rows = parse_summary(SUMMARY_PATH)
    pkts = list(packets_from_summary(rows))
    send_stream(pkts)
