# consumer.py  (canlı yazdırmalı sürüm)
# producer.py tarafından gönderilen FramePacket JSON satırlarını TCP soketi üzerinden alır,
# ayrıştırır ve içeriği canlı olarak konsola yazdırır.

import json, socket, sys, threading, asyncio
from typing import Optional
from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles
import uvicorn

HOST = "127.0.0.1"
PORT = 8765
latest_frame = None # global variable to store the most recent frame

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

def handle_client(conn: socket.socket, sink: Sink):
    global latest_frame
    with conn:
        buf = b""
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            buf += chunk
            # NDJSON satır bazlı ayrıştırma
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line.decode("utf-8"))
                    sink.add(obj)
                    latest_frame = obj # store latest frame here

                    # === CANLI YAZDIRMA ===
                    i = len(sink.mins) - 1
                    print(
                        f"[live] #{sink.frame_nos[i]:04d} | "
                        f"ts={sink.timestamps[i]} | "
                        f"min={sink.mins[i]:.2f}°C max={sink.maxs[i]:.2f}°C mean={sink.means[i]:.2f}°C | "
                        f"path={'None' if sink.image_paths[i] is None else sink.image_paths[i]}"
                    )
                    comment = interpret_temp(sink.maxs[i])
                    print(f"                        ↳ {comment}");
                    sys.stdout.flush()

                    # (İsteğe bağlı)
                    # window = 10
                    # if len(sink.means) >= window:
                    #     ma = sum(sink.means[-window:]) / window
                    #     print(f"        ↳ running_mean({window}) = {ma:.2f}°C"); sys.stdout.flush()

                except Exception as e:
                    print("[consumer] parse error:", e); sys.stdout.flush()

def run_server():
    sink = Sink()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(1)
    print(f"[consumer] listening on {HOST}:{PORT} ..."); sys.stdout.flush()

    try:
        conn, addr = srv.accept()
        print(f"[consumer] connected from {addr}"); sys.stdout.flush()
        handle_client(conn, sink)
    finally:
        srv.close()

    # Akış bittiğinde kısa özet:
    n = len(sink.mins)
    print("\n=== STREAM SUMMARY ===")
    print(f"count: {n}")
    if n:
        head = list(range(min(3, n)))
        tail = list(range(max(0, n-3), n))
        def show(idx):
            print(f"[{idx}] frame={sink.frame_nos[idx]} ts={sink.timestamps[idx]} "
                  f"min={sink.mins[idx]:.2f} max={sink.maxs[idx]:.2f} mean={sink.means[idx]:.2f} "
                  f"path={'None' if sink.image_paths[idx] is None else sink.image_paths[idx]}")
        print("\n-- first --")
        for i in head: show(i)
        print("\n-- last --")
        for i in tail: show(i)

    # Diziler hazır: sink.mins, sink.maxs, sink.means, sink.timestamps, sink.image_paths, sink.frame_nos

# --- FastAPI setup ---
app = FastAPI()

# Serve static assets
app.mount("/static", StaticFiles(directory="static"), name="static")

# Serve index.html at root
from fastapi.responses import FileResponse
import os

@app.get("/")
def root():
    return FileResponse(os.path.join("static", "index.html"))

@app.get("/frames/latest")
def get_latest():
    if latest_frame is None:
        return {"status": "no data yet"}
    return latest_frame

from fastapi import WebSocket

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    global latest_frame
    while True:
        if latest_frame is not None:
            try:
                await ws.send_json(latest_frame)
            except Exception as e:
                print("WebSocket send error:", e)
        await asyncio.sleep(0.5)  # shorter interval for smoother updates



def interpret_temp(t_max: float,threshold: float=30.0) -> str:
    if t_max >= threshold:
        return "HIGH TEMPERATURE ALERT!"
    else:
        return "Temperature Normal."

if __name__ == "__main__":
    # Start the TCP listener (producer → consumer stream) in background
    t = threading.Thread(target=run_server, daemon=True)
    t.start()

    # Start FastAPI web server
    uvicorn.run(app, host="0.0.0.0", port=8000)
