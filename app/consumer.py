# consumer.py  (thermal threshold: UI'dan ayarlanır + kalıcı)
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
from pathlib import Path
import google.generativeai as genai
from dotenv import load_dotenv
from fastapi import Request
from collections import deque
from typing import Optional

from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import uvicorn

BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv("app/.env")

genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

gemini_model = genai.GenerativeModel("gemini-2.5-flash")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:
    from models.model_manager import TorqueModelManager
except Exception as _model_err:  # model artifacts optional
    TorqueModelManager = None
    _MODEL_IMPORT_ERROR = _model_err

HOST = "127.0.0.1"

PORT_THERMAL_TCP = 8765
PORT_TORQUE_TCP = 8766

EVENTS_LOG_FILE = str(BASE_DIR / "events.log")
SETTINGS_FILE = str(BASE_DIR / "settings.json")

ERROR_COOLDOWN = 5.0  # saniye
FRAME_HISTORY_SIZE = 200
MODEL_EVENT_COOLDOWN = 5.0

# ---------------------------
# TORQUE (DOKUNMADIM)
# ---------------------------
TORQUE_THRESHOLD = 2.0

# ---------------------------
# THERMAL (kalıcı ayar)
# ---------------------------
KELVIN_OFFSET = 273.15

DEFAULT_SETTINGS = {
    "thermal_threshold_c": 30.0,  # °C (UI'daki input ile değişecek)
    "thermal_warning_c": 30.0,
    "thermal_critical_c": 33.0,
}

_settings_lock = threading.Lock()
settings = DEFAULT_SETTINGS.copy()


def load_settings() -> None:
    global settings
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                merged = DEFAULT_SETTINGS.copy()
                merged.update({k: float(v) for k, v in data.items() if k in DEFAULT_SETTINGS})
                settings = merged
        except Exception:
            # bozuksa defaultla devam
            settings = DEFAULT_SETTINGS.copy()
    else:
        settings = DEFAULT_SETTINGS.copy()
        save_settings()  # ilk kez oluştur


def save_settings() -> None:
    with _settings_lock:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(settings, f, ensure_ascii=False, indent=2)


def is_kelvin_value(t: float) -> bool:
    # KUKA loglarında 295-310 gibi değerler Kelvin olur.
    return t is not None and t > 120.0


def thr_in_same_unit(sample_t: float, thr_c: float) -> float:
    # sample Kelvin ise threshold'u Kelvin'e çevir
    return (thr_c + KELVIN_OFFSET) if is_kelvin_value(sample_t) else thr_c


# ---------------------------
# GLOBAL STATE
# ---------------------------
latest_frame = None          # thermal latest packet
latest_torque = None         # torque latest packet
frame_history = deque(maxlen=FRAME_HISTORY_SIZE)

error_log = []
last_error_time = {}

# Load persisted settings + old events
load_settings()

if os.path.exists(EVENTS_LOG_FILE):
    with open(EVENTS_LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            try:
                error_log.append(json.loads(line))
            except Exception:
                pass

# ---------------------------
# TORQUE MODEL MANAGER (PCA + IForest)
# ---------------------------
torque_models = None
if TorqueModelManager is not None:
    try:
        torque_models = TorqueModelManager.load(artifacts_dir=str(BASE_DIR / "artifacts"))
        if torque_models.enabled():
            print(
                "[torque] model manager loaded:"
                f" PCA={'yes' if torque_models.pca else 'no'},"
                f" IForest={'yes' if torque_models.iforest else 'no'}"
            )
        else:
            print("[torque] model manager: artifacts not found, skipping")
    except Exception as e:
        print("[torque] model manager load error:", e)
        torque_models = None
else:
    print("[torque] model manager unavailable:", _MODEL_IMPORT_ERROR)


# ---------------------------
# THERMAL TCP CONSUMER
# ---------------------------
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
                    t_max = sink.maxs[i]

                    # live log
                    with _settings_lock:
                        thr_c = float(settings["thermal_threshold_c"])
                        warn_c = float(settings["thermal_warning_c"])
                        crit_c = float(settings["thermal_critical_c"])

                    thr_raw = thr_in_same_unit(t_max, thr_c)
                    warn_raw = thr_in_same_unit(t_max, warn_c)
                    crit_raw = thr_in_same_unit(t_max, crit_c)

                    # console info (raw + celsius)
                    t_max_c = (t_max - KELVIN_OFFSET) if is_kelvin_value(t_max) else t_max
                    print(
                        f"[live] #{sink.frame_nos[i]:04d} | ts={sink.timestamps[i]} | "
                        f"t_max_raw={t_max:.2f} | t_max_c={t_max_c:.2f}°C | "
                        f"thr_c={thr_c:.2f}°C"
                    )

                    # EVENT DETECTION
                    if t_max >= thr_raw:
                        key = ("THERMAL", sink.frame_nos[i])
                        now = time.time()

                        if key not in last_error_time or now - last_error_time[key] > ERROR_COOLDOWN:
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
                                    "t_max": t_max,             # raw
                                    "t_max_c": t_max_c,         # celsius (events.html düzgün gösterecek)
                                    "threshold": thr_raw,       # raw threshold (unit matched)
                                    "threshold_c": thr_c,       # celsius threshold (UI için)
                                    "frame_no": sink.frame_nos[i],
                                },
                            }

                            error_log.append(event)
                            last_error_time[key] = now

                            with open(EVENTS_LOG_FILE, "a", encoding="utf-8") as f:
                                f.write(json.dumps(event) + "\n")

                    sys.stdout.flush()

                except Exception as e:
                    print("[consumer] thermal parse error:", e)
                    sys.stdout.flush()


def run_thermal_server() -> None:
    sink = Sink()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT_THERMAL_TCP))
    srv.listen(1)

    print(f"[consumer] listening on {HOST}:{PORT_THERMAL_TCP} ...")
    sys.stdout.flush()

    while True:
        conn, addr = srv.accept()
        print(f"[consumer] thermal connected from {addr}")
        sys.stdout.flush()
        try:
            handle_thermal_client(conn, sink)
        except Exception as e:
            print("[consumer] thermal client error:", e)
        finally:
            try:
                conn.close()
            except Exception:
                pass


# ---------------------------
# TORQUE TCP CONSUMER (DOKUNMADIM)
# ---------------------------
def detect_torque_anomaly(actual, ideal):
    diffs = [abs(a - i) for a, i in zip(actual, ideal)]
    flags = [d > TORQUE_THRESHOLD for d in diffs]
    return diffs, flags


def _model_severity(score: float, threshold: float) -> str:
    if threshold <= 0:
        return "INFO"
    ratio = score / threshold
    if ratio >= 1.6:
        return "CRITICAL"
    if ratio >= 1.3:
        return "WARNING"
    return "INFO"


def log_torque_model_event(event_type: str, score: float, threshold: float, frame_no: int, ts: str) -> None:
    key = (event_type,)
    now = time.time()
    if key in last_error_time and now - last_error_time[key] <= MODEL_EVENT_COOLDOWN:
        return

    severity = _model_severity(score, threshold)
    label = event_type.replace("TORQUE_", "")

    event = {
        "timestamp": ts,
        "type": event_type,
        "severity": severity,
        "message": f"{label} anomaly score exceeded threshold",
        "meta": {
            "model": label,
            "score": score,
            "threshold": threshold,
            "frame_no": frame_no,
        },
    }

    error_log.append(event)
    with open(EVENTS_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")

    last_error_time[key] = now


def run_torque_server() -> None:
    global latest_torque

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT_TORQUE_TCP))
    srv.listen(1)

    print(f"[torque] listening on {HOST}:{PORT_TORQUE_TCP} ...")
    sys.stdout.flush()

    while True:
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
                    ideal = pkt.get("torque_ideal")
                    if torque_models is not None and torque_models.pca is not None:
                        try:
                            recon = torque_models.reconstruct(pkt["torque_actual"])
                            if recon is not None:
                                ideal = recon
                                pkt["torque_ideal"] = recon
                                pkt["ideal_source"] = "pca_reconstruct"
                        except Exception as e:
                            print("[torque] PCA reconstruct error:", e)

                    if ideal is None:
                        ideal = [0.0] * len(pkt["torque_actual"])
                        pkt["ideal_source"] = "zero"

                    diffs, flags = detect_torque_anomaly(pkt["torque_actual"], ideal)
                    pkt["diffs"] = diffs
                    pkt["anomaly"] = any(flags)

                    if pkt["anomaly"]:
                        for j, d in enumerate(pkt["diffs"]):
                            if d > TORQUE_THRESHOLD:
                                key = ("TORQUE", j + 1)
                                now = time.time()

                                if key not in last_error_time or now - last_error_time[key] > ERROR_COOLDOWN:
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

                    # PCA + IForest scores
                    model_payload = {}
                    if torque_models is not None and torque_models.enabled():
                        try:
                            model_payload = torque_models.score(pkt["torque_actual"])
                            pkt.update(model_payload)
                        except Exception as e:
                            print("[torque] model scoring error:", e)

                    if model_payload.get("pca_anomaly"):
                        log_torque_model_event(
                            "TORQUE_PCA",
                            model_payload["pca_score"],
                            model_payload["pca_threshold"],
                            pkt["frame_no"],
                            pkt["timestamp"],
                        )

                    if model_payload.get("iforest_anomaly"):
                        log_torque_model_event(
                            "TORQUE_IFOREST",
                            model_payload["iforest_score"],
                            model_payload["iforest_threshold"],
                            pkt["frame_no"],
                            pkt["timestamp"],
                        )

                    latest_torque = pkt
                    print(
                        f"[torque] frame={pkt['frame_no']} anomaly={pkt['anomaly']}"
                        f" model={pkt.get('model_anomaly')} diffs={diffs}"
                    )
                    sys.stdout.flush()


# ---------------------------
# FASTAPI (UI)
# ---------------------------
app = FastAPI()
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.get("/")
def root():
    return FileResponse(str(BASE_DIR / "static" / "index.html"))


@app.get("/thermal")
def thermal_page():
    return FileResponse(str(BASE_DIR / "static" / "thermal.html"))


@app.get("/torque")
def torque_page():
    return FileResponse(str(BASE_DIR / "static" / "torque.html"))


@app.get("/events")
def events_page():
    return FileResponse(str(BASE_DIR / "static" / "events.html"))


@app.get("/frames/latest")
def get_latest():
    if latest_frame is None:
        return {"status": "no data yet"}
    return latest_frame


@app.get("/errors")
def get_errors():
    return error_log[-200:]


class ChatRequest(BaseModel):
    message: str


@app.post("/chat")
async def chat(req: ChatRequest):
    thermal_data = latest_frame if latest_frame else {}
    torque_data = latest_torque if latest_torque else {}
    recent_events = error_log[-20:]
    recent_frames = list(frame_history)[-20:]

    prompt = f"""
You are MindTwin AI, an intelligent assistant for a KUKA robotic arm monitoring dashboard.

Your responsibilities:
- Analyze thermal, torque, PCA and Isolation Forest anomaly data.
- Explain anomalies clearly.
- Suggest possible engineering causes.
- Recommend maintenance actions when needed.
- Use only provided data.
- Do not hallucinate nonexistent anomalies.

Response style rules:
- Be concise by default.
- Use short monitoring-style responses for dashboard questions.
- Use more detailed explanations only if the user explicitly asks for details.
- Keep most responses under 2 sentences.
- Sound like an industrial monitoring dashboard.
Language rules:
- Use simple engineering language.
- Avoid overly academic or corporate wording.
- Sound direct and practical.

Maintenance rules:
- Give practical maintenance suggestions when anomalies appear.
- Keep recommendations short and actionable.
- Prefer commands like:
  "Check", "Inspect", "Monitor", "Reduce load".
- Maximum 3 recommendation lines.

Latest thermal data:
{thermal_data}

Latest torque data:
{torque_data}

Recent events:
{recent_events}

Recent thermal frames:
{recent_frames}

User question:
{req.message}
"""

    try:
        response = gemini_model.generate_content(prompt)

        reply = (
            response.text
            if response.text
            else "No AI response generated."
        )

        return {
            "reply": reply
        }

    except Exception as e:
        print("CHAT ERROR:", e)

        return {
            "reply": f"Gemini error: {str(e)}"
        }


# ---- SETTINGS API (kalıcı threshold) ----



# ---- SETTINGS API (kalıcı threshold) ----
class ThermalSettingsIn(BaseModel):
    thermal_threshold_c: float


@app.get("/settings/thermal")
def get_thermal_settings():
    with _settings_lock:
        return {"thermal_threshold_c": float(settings["thermal_threshold_c"])}


@app.post("/settings/thermal")
def set_thermal_settings(payload: ThermalSettingsIn):
    with _settings_lock:
        settings["thermal_threshold_c"] = float(payload.thermal_threshold_c)
    save_settings()
    return {"ok": True, "thermal_threshold_c": float(payload.thermal_threshold_c)}


@app.websocket("/ws")
async def websocket_thermal(ws: WebSocket):
    await ws.accept()
    global latest_frame
    while True:
        if latest_frame is not None:
            try:
                await ws.send_json(latest_frame)
            except Exception as e:
                print("WebSocket thermal send error:", e)
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
                print("WebSocket torque send error:", e)
        await asyncio.sleep(0.5)


if __name__ == "__main__":
    t1 = threading.Thread(target=run_thermal_server, daemon=True)
    t1.start()

    t2 = threading.Thread(target=run_torque_server, daemon=True)
    t2.start()

    uvicorn.run(app, host="0.0.0.0", port=8000)
