# consumer.py  (canlı yazdırmalı sürüm)
# Thermal producer (TCP:8765) + Torque producer (TCP:8766) dinler
# FastAPI ile UI'ya websocket yayınlar:
#   Thermal WS: /ws
#   Torque  WS: /ws/torque

import json
import socket
import sys
import threading
import asyncio
import os
import time
from collections import deque
from typing import Optional

from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import uvicorn


HOST = "127.0.0.1"

PORT_THERMAL_TCP = 8765
PORT_TORQUE_TCP = 8766

EVENTS_LOG_FILE = "events.log"

ERROR_COOLDOWN = 5.0  # saniye
FRAME_HISTORY_SIZE = 200

# Torque threshold (UI ile uyumlu tut)
TORQUE_THRESHOLD = 0.47

# Thermal thresholds: UI bozulmasın diye frame'leri DÖNÜŞTÜRMÜYORUZ.
# Sadece anomaly/event eşiğini doğru birime göre hesaplıyoruz.
THERMAL_THRESHOLD_C = 30.0
THERMAL_WARNING_C = 30.0
THERMAL_CRITICAL_C = 33.0
KELVIN_OFFSET = 273.15


latest_frame = None          # thermal latest packet
latest_torque = None         # torque latest packet

frame_history = deque(maxlen=FRAME_HISTORY_SIZE)

error_log = []
last_error_time = {}

if os.path.exists(EVENTS_LOG_FILE):
    with open(EVENTS_LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            try:
                error_log.append(json.loads(line))
            except Exception:
                pass


def is_kelvin_value(t: float) -> bool:
    # KUKA loglarında 295-310 gibi değerler Kelvin olur.
    # Celsius için tipik değerler 0-100 bandında.
    return t is not None and t > 120.0


def thr_in_same_unit(sample_t: float, thr_c: float) -> float:
    # sample Kelvin ise threshold'u Kelvin'e çevir
    return (thr_c + KELVIN_OFFSET) if is_kelvin_value(sample_t) else thr_c


def interpret_temp(t_max_raw: float) -> str:
    thr_raw = thr_in_same_unit(t_max_raw, THERMAL_THRESHOLD_C)
    return "HIGH TEMPERATURE ALERT!" if t_max_raw >= thr_raw else "Temperature Normal."



class Sink:
    def __init__(self) -> None:
        self.timestamps: list[str] = []
        self.mins: list[float] = []
        self.maxs: list[float] = []
        self.means: list[float] = []
        self.image_paths: list[Optional[str]] = []
        self.frame_nos: list[int] = []

    def add(self, obj: dict) -> None:
        self.timestamps.append(obj["timestamp"])
        self.mins.append(float(obj["t_min"]))
        self.maxs.append(float(obj["t_max"]))
        self.means.append(float(obj["t_mean"]))
        self.image_paths.append(obj.get("image_path"))
        self.frame_nos.append(int(obj.get("frame_no", -1)))


def handle_thermal_client(conn: socket.socket, sink: Sink) -> None:
    global latest_frame

    with conn:
        buf = b""
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            buf += chunk

            # NDJSON (satır bazlı)
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if not line.strip():
                    continue

                try:
                    obj = json.loads(line.decode("utf-8"))

                    sink.add(obj)
                    latest_frame = obj
                    frame_history.append(obj)

                    i = len(sink.mins) - 1

                    # CANLI LOG
                    print(
                        f"[live] #{sink.frame_nos[i]:04d} | "
                        f"ts={sink.timestamps[i]} | "
                        f"min={sink.mins[i]:.2f}°C max={sink.maxs[i]:.2f}°C mean={sink.means[i]:.2f}°C | "
                        f"path={'None' if sink.image_paths[i] is None else sink.image_paths[i]}"
                    )

                    # THERMAL EVENT DETECTION (AUTO UNIT)
                    t_max = sink.maxs[i]
                    comment = interpret_temp(t_max)

                    thr_raw = thr_in_same_unit(t_max, THERMAL_THRESHOLD_C)
                    warn_raw = thr_in_same_unit(t_max, THERMAL_WARNING_C)
                    crit_raw = thr_in_same_unit(t_max, THERMAL_CRITICAL_C)

                    if t_max >= thr_raw:
                        key = ("THERMAL", sink.frame_nos[i])
                        now = time.time()

                        if key not in last_error_time or now - last_error_time[key] > ERROR_COOLDOWN:
                            # SEVERITY
                            if t_max > crit_raw:
                                severity = "CRITICAL"
                            elif t_max > warn_raw:
                                severity = "WARNING"
                            else:
                                severity = "INFO"

                            event = {
                                "timestamp": obj["timestamp"],
                                "type": "THERMAL",
                                "severity": severity,
                                "message": "High temperature detected",
                                "meta": {
                                    "t_max": t_max,
                                    "threshold": thr_raw,
                                    "frame_no": sink.frame_nos[i],
                                },
                            }

                            error_log.append(event)
                            last_error_time[key] = now

                            with open(EVENTS_LOG_FILE, "a", encoding="utf-8") as f:
                                f.write(json.dumps(event) + "\n")

                    print(f"                        ↳ {comment}")
                    sys.stdout.flush()

                except Exception as e:
                    print("[consumer] parse error:", e)
                    sys.stdout.flush()


def run_thermal_server() -> None:
    sink = Sink()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT_THERMAL_TCP))
    srv.listen(1)

    print(f"[consumer] listening on {HOST}:{PORT_THERMAL_TCP} ...")
    sys.stdout.flush()

    try:
        conn, addr = srv.accept()
        print(f"[consumer] connected from {addr}")
        sys.stdout.flush()
        handle_thermal_client(conn, sink)
    finally:
        srv.close()

    # Akış bittiğinde özet
    n = len(sink.mins)
    print("\n=== STREAM SUMMARY ===")
    print(f"count: {n}")
    if n:
        head = list(range(min(3, n)))
        tail = list(range(max(0, n - 3), n))

        def show(idx: int) -> None:
            print(
                f"[{idx}] frame={sink.frame_nos[idx]} ts={sink.timestamps[idx]} "
                f"min={sink.mins[idx]:.2f} max={sink.maxs[idx]:.2f} mean={sink.means[idx]:.2f} "
                f"path={'None' if sink.image_paths[idx] is None else sink.image_paths[idx]}"
            )

        print("\n-- first --")
        for i in head:
            show(i)
        print("\n-- last --")
        for i in tail:
            show(i)


def detect_torque_anomaly(actual, ideal):
    diffs = [abs(a - i) for a, i in zip(actual, ideal)]
    flags = [d > TORQUE_THRESHOLD for d in diffs]
    return diffs, flags


def run_torque_server() -> None:
    global latest_torque

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT_TORQUE_TCP))
    srv.listen(1)

    print(f"[torque] listening on {HOST}:{PORT_TORQUE_TCP} ...")
    sys.stdout.flush()

    conn, addr = srv.accept()
    print(f"[torque] connected from {addr}")
    sys.stdout.flush()

    buf = b""
    with conn:
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            buf += chunk

            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if not line.strip():
                    continue

                pkt = json.loads(line.decode("utf-8"))

                diffs, flags = detect_torque_anomaly(pkt["torque_actual"], pkt["torque_ideal"])
                pkt["diffs"] = diffs
                pkt["anomaly"] = any(flags)

                if pkt["anomaly"]:
                    for j, d in enumerate(pkt["diffs"]):
                        if d > TORQUE_THRESHOLD:
                            key = ("TORQUE", j + 1)
                            now = time.time()

                            if key not in last_error_time or now - last_error_time[key] > ERROR_COOLDOWN:
                                # SEVERITY
                                if d > 0.6:
                                    severity = "CRITICAL"
                                elif d > 0.3:
                                    severity = "WARNING"
                                else:
                                    severity = "INFO"

                                event = {
                                    "timestamp": pkt["timestamp"],
                                    "type": "TORQUE",
                                    "severity": severity,
                                    "message": f"Joint {j+1} torque exceeded threshold",
                                    "meta": {
                                        "joint": j + 1,
                                        "diff": d,
                                        "threshold": TORQUE_THRESHOLD,
                                        "frame_no": pkt["frame_no"],
                                    },
                                }

                                error_log.append(event)
                                with open(EVENTS_LOG_FILE, "a", encoding="utf-8") as f:
                                    f.write(json.dumps(event) + "\n")

                                last_error_time[key] = now

                latest_torque = pkt

                print(f"[torque] frame={pkt['frame_no']} anomaly={pkt['anomaly']} diffs={diffs}")
                sys.stdout.flush()



app = FastAPI()

# Static
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def root():
    return FileResponse(os.path.join("static", "index.html"))


@app.get("/thermal")
def thermal_page():
    return FileResponse(os.path.join("static", "thermal.html"))


@app.get("/torque")
def torque_page():
    return FileResponse(os.path.join("static", "torque.html"))


@app.get("/events")
def events_page():
    return FileResponse(os.path.join("static", "events.html"))


@app.get("/frames/latest")
def get_latest():
    if latest_frame is None:
        return {"status": "no data yet"}
    return latest_frame


@app.get("/frames/history")
def get_frame_history():
    return list(frame_history)


@app.get("/errors")
def get_errors():
    return error_log[-200:]


@app.websocket("/ws")
async def websocket_thermal(ws: WebSocket):
    await ws.accept()
    global latest_frame
    while True:
        if latest_frame is not None:
            try:
                await ws.send_json(latest_frame)
            except Exception as e:
                print("WebSocket send error:", e)
        await asyncio.sleep(0.5)


@app.websocket("/ws/torque")
async def websocket_torque(ws: WebSocket):
    await ws.accept()
    global latest_torque
    while True:
        if latest_torque is not None:
            try:
                await ws.send_json(latest_torque)
            except Exception as e:
                print("WebSocket send error:", e)
        await asyncio.sleep(0.5)


if __name__ == "__main__":
    # TCP listeners
    t1 = threading.Thread(target=run_thermal_server, daemon=True)
    t1.start()

    t2 = threading.Thread(target=run_torque_server, daemon=True)
    t2.start()

    # Web server
    uvicorn.run(app, host="0.0.0.0", port=8000)
