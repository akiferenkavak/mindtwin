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
from fastapi.middleware.gzip import GZipMiddleware
from collections import deque
from typing import Optional

from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import uvicorn

BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / "app" / ".env")   # absolute path – her zaman bulunur

# API key: .env'den oku, yoksa doğrudan kullan
_API_KEY = os.getenv("GOOGLE_API_KEY") or "AIzaSyB7n4hUvikR9uU6AEXXci7Wp0tJmcNDnY4"
genai.configure(api_key=_API_KEY)

gemini_model = genai.GenerativeModel("gemini-2.5-flash")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# ---------------------------
# LOAD KNOWLEDGE BASE FROM JSON FILES
# ---------------------------
def _load_json_safe(path: Path) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

ARTIFACTS_DIR = BASE_DIR / "artifacts"
KB_EVAL_REPORT     = _load_json_safe(ARTIFACTS_DIR / "eval_report.json")
KB_PCA_THRESHOLDS  = _load_json_safe(ARTIFACTS_DIR / "pca_thresholds.json")
KB_IFOREST_THR     = _load_json_safe(ARTIFACTS_DIR / "iforest_threshold.json")
KB_AUTOENCODER     = _load_json_safe(ARTIFACTS_DIR / "autoencoder_results.json")
KB_PCA_RESULTS     = _load_json_safe(ARTIFACTS_DIR / "pca_results.json")

# ---------------------------
# AUTOENCODER LOOKUPS (FRAME-INDEX BASED — döngüsel, timestamp bağımsız)
# ---------------------------
# AE verisi aynı CSV'den türetildiği için sıra numarası (frame_no % len) eşleşir.
ae_multiple_list  = KB_AUTOENCODER.get("multiple", [])   # ordered list
ae_multiple_thr   = KB_AUTOENCODER.get("meta", {}).get("multiple_threshold", None)
ae_single_thr     = KB_AUTOENCODER.get("meta", {}).get("single_threshold", None)

# single: feature → ordered list (her feature için ayrı liste)
ae_single_by_feat = KB_AUTOENCODER.get("single", {})

# Map timestamp to frame index (0 to 3222) in the chronological multiple list
ae_time_to_frame = {row.get("timestamp"): idx for idx, row in enumerate(ae_multiple_list) if row.get("timestamp")}

# Pre-map single anomalies to their actual frame index using timestamp mapping
ae_single_by_frame_and_feat = {}
for feat, items in ae_single_by_feat.items():
    ae_single_by_frame_and_feat[feat] = {}
    if isinstance(items, list):
        for item in items:
            ts = item.get("timestamp") or item.get("time") or ""
            f_no = ae_time_to_frame.get(ts)
            if f_no is None:
                f_no = item.get("frame_no") or item.get("frame")
            if f_no is not None:
                ae_single_by_frame_and_feat[feat][f_no] = item


def translate_to_english(text: str) -> str:
    if not text:
        return text
    # Convert Turkish expressions to clean English descriptions
    text = text.replace("normalden dusuk", "below normal")
    text = text.replace("normalden yuksek", "above normal")
    text = text.replace("Normal calisma", "Normal operation")
    text = text.replace("Normal çalışma", "Normal operation")
    return text


def translate_ae_data(data: dict) -> dict:
    if not isinstance(data, dict):
        return data
    
    # Translate multiple list
    if "multiple" in data and isinstance(data["multiple"], list):
        for item in data["multiple"]:
            if "explanation" in item and isinstance(item["explanation"], str):
                item["explanation"] = translate_to_english(item["explanation"])
            if "root_cause" in item and isinstance(item["root_cause"], str):
                item["root_cause"] = translate_to_english(item["root_cause"])
                
    # Translate single dictionary
    if "single" in data and isinstance(data["single"], dict):
        for feat, items in data["single"].items():
            if isinstance(items, list):
                for item in items:
                    if "explanation" in item and isinstance(item["explanation"], str):
                        item["explanation"] = translate_to_english(item["explanation"])
                    if "direction" in item and isinstance(item["direction"], str):
                        item["direction"] = "high" if item.get("direction") == "high" else "low"
                        
    return data


def get_ae_for_frame(frame_no: int) -> dict:
    """Return the AE multiple-row for this frame (cyclic wrap)."""
    if not ae_multiple_list:
        return {}
    row = ae_multiple_list[frame_no % len(ae_multiple_list)]
    return row



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

# Load persisted settings - clean events.log for a fresh session
load_settings()

try:
    with open(EVENTS_LOG_FILE, "w", encoding="utf-8") as f:
        f.truncate(0)
    print("[consumer] events.log cleared successfully for fresh session.")
except Exception as e:
    print("[consumer] Error clearing events.log:", e)

# ---------------------------
# TORQUE MODEL MANAGER (PCA + IForest)
# ---------------------------
torque_models = None
_rf_reload_attempted = False
if TorqueModelManager is not None:
    try:
        torque_models = TorqueModelManager.load(artifacts_dir=str(BASE_DIR / "artifacts"))
        if torque_models.enabled():
            print(
                "[torque] model manager loaded:"
                f" PCA={'yes' if torque_models.pca else 'no'},"
                f" IForest={'yes' if torque_models.iforest else 'no'},"
                f" RF={'yes' if getattr(torque_models, 'graybox_rf', None) else 'no'}"
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


def log_torque_model_event(
    event_type: str, score: float, threshold: float,
    frame_no: int, ts: str,
    extra_meta: dict = None,
    severity: str = None
) -> None:
    key_joint = None
    if extra_meta and "worst_joint" in extra_meta:
        key_joint = extra_meta.get("worst_joint")
    key = (event_type, key_joint)
    now = time.time()
    if key in last_error_time and now - last_error_time[key] <= MODEL_EVENT_COOLDOWN:
        return

    if severity is None:
        severity = _model_severity(score, threshold)
    label = event_type.replace("TORQUE_", "")

    meta = {
        "model": label,
        "score": score,
        "threshold": threshold,
        "frame_no": frame_no,
    }
    if extra_meta:
        meta.update(extra_meta)

    # Build concise human-readable message
    worst = meta.get("worst_joint", "?")
    if event_type == "TORQUE_AUTOENCODER":
        msg = f"Autoencoder Torque Error: A{worst}"
    elif event_type == "TORQUE_PCA":
        msg = f"PCA Torque Error: A{worst}"
    elif event_type == "TORQUE_COMBINED":
        models = meta.get("models") or ["PCA", "AE"]
        msg = f"DOUBLE ANOMALY ({' + '.join(models)}): A{worst}"
    elif event_type == "TORQUE_TRIPLE_COMBINED":
        msg = f"TRIPLE ANOMALY (PCA + AE + RF): A{worst}"
    elif event_type == "TORQUE_RF":
        msg = f"Random Forest Torque Residual: A{worst}"
    else:
        msg = f"{label} anomaly score exceeded threshold"
        if "worst_joint" in meta:
            msg += f" | Highest error: A{meta['worst_joint']}"

    event = {
        "timestamp": ts,
        "type": event_type,
        "severity": severity,
        "message": msg,
        "meta": meta,
    }

    error_log.append(event)
    with open(EVENTS_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")

    last_error_time[key] = now


def run_torque_server() -> None:
    global latest_torque
    global torque_models
    global _rf_reload_attempted

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
                    # Normalize torque input per detector scale if raw torque + producer scale are provided.
                    # This keeps PCA/IForest and RF aligned even if the producer calibrates with a different CSV.
                    torque_raw = pkt.get("torque_actual_raw")
                    producer_scale = pkt.get("producer_scale")
                    if (
                        torque_models is not None
                        and torque_raw is not None
                        and producer_scale
                        and isinstance(torque_raw, list)
                    ):
                        try:
                            scale_f = float(producer_scale) if float(producer_scale) != 0 else None
                            if scale_f:
                                pca_scale = None
                                try:
                                    pca_scale = float((torque_models.pca.meta or {}).get("producer_scale") or 0) if torque_models.pca else None
                                except Exception:
                                    pca_scale = None
                                rf_scale = None
                                try:
                                    rf_scale = float(getattr(getattr(torque_models, "graybox_rf", None), "producer_scale", 0) or 0)
                                except Exception:
                                    rf_scale = None

                                if pca_scale and pca_scale > 0:
                                    pkt["torque_actual"] = [(float(v) / pca_scale) for v in torque_raw]
                                # Always keep the producer-normalized values available for RF and other consumers.
                                pkt["torque_actual_producer"] = [(float(v) / scale_f) for v in torque_raw]
                                if rf_scale and rf_scale > 0:
                                    pkt["torque_actual_rf"] = [(float(v) / rf_scale) for v in torque_raw]
                        except Exception:
                            pass
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

                    # Raw torque threshold checks removed as requested

                    # PCA + IForest scores
                    model_payload = {}
                    if torque_models is not None and torque_models.enabled():
                        try:
                            if (
                                not _rf_reload_attempted
                                and pkt.get("q") is not None
                                and pkt.get("q_dot") is not None
                                and getattr(torque_models, "graybox_rf", None) is None
                            ):
                                _rf_reload_attempted = True
                                try:
                                    torque_models = TorqueModelManager.load(artifacts_dir=str(BASE_DIR / "artifacts"))
                                    print("[torque] model manager reloaded (RF auto-train may have run).")
                                except Exception as _e:
                                    print("[torque] model manager reload error:", _e)

                            model_payload = torque_models.score(
                                pkt.get("torque_actual") or pkt["torque_actual"],
                                q=pkt.get("q"),
                                q_dot=pkt.get("q_dot"),
                                q_ddot=pkt.get("q_ddot"),
                                curr=pkt.get("curr"),
                            )
                            pkt.update(model_payload)
                        except Exception as e:
                            print("[torque] model scoring error:", e)

                    if model_payload.get("pca_anomaly"):
                        # Find which joint has the highest diff for PCA context
                        worst_j = int(pkt["diffs"].index(max(pkt["diffs"]))) + 1 if pkt.get("diffs") else None
                        log_torque_model_event(
                            "TORQUE_PCA",
                            model_payload["pca_score"],
                            model_payload["pca_threshold"],
                            pkt["frame_no"],
                            pkt["timestamp"],
                            extra_meta={"worst_joint": worst_j} if worst_j else None,
                        )

                    # RF scoring uses its own normalization scale; if provided, override with torque_actual_rf.
                    rf_payload = model_payload
                    if (
                        torque_models is not None
                        and getattr(torque_models, "graybox_rf", None) is not None
                        and pkt.get("torque_actual_rf") is not None
                        and pkt.get("q") is not None
                        and pkt.get("q_dot") is not None
                    ):
                        try:
                            rf_payload = torque_models.graybox_rf.score(
                                pkt.get("q"),
                                pkt.get("q_dot"),
                                pkt.get("torque_actual_rf"),
                                q_ddot=pkt.get("q_ddot"),
                                curr=pkt.get("curr"),
                            )
                            # Keep rf_* keys consistent in pkt for downstream UI.
                            pkt.update(rf_payload)
                        except Exception as _e:
                            rf_payload = model_payload

                    rf_anomaly_joints = list(rf_payload.get("rf_anomaly_joints") or [])
                    if rf_anomaly_joints:
                        rf_scores = rf_payload.get("rf_scores") or {}
                        rf_thresholds = rf_payload.get("rf_thresholds") or {}

                        # Emit per-axis events so the torque monitor can reflect RF anomalies per joint.
                        for joint in rf_anomaly_joints:
                            score_j = float(rf_scores.get(f"A{joint}", 0.0) or 0.0)
                            thr_j = float(rf_thresholds.get(f"A{joint}", 0.0) or 0.0)
                            log_torque_model_event(
                                "TORQUE_RF",
                                score_j,
                                thr_j,
                                pkt["frame_no"],
                                pkt["timestamp"],
                                extra_meta={
                                    "worst_joint": int(joint),
                                    "anomaly_joints": rf_anomaly_joints,
                                    "rf_scores": rf_scores,
                                },
                            )



                    # -----------------------------------------------
                    # AUTOENCODER injection (frame-index döngüsel eşleştirme)
                    # -----------------------------------------------
                    ae_idx = pkt["frame_no"] % len(ae_multiple_list) if ae_multiple_list else pkt["frame_no"]
                    ae_row = get_ae_for_frame(pkt["frame_no"])
                    ae_score     = float(ae_row.get("error", 0))
                    ae_thr       = float(ae_row.get("threshold", ae_multiple_thr or 9.385))
                    ae_raw_is_anomaly = bool(ae_row.get("is_anomaly", ae_score > ae_thr))
                    ae_root_cause = ae_row.get("root_cause", "")

                    pkt["ae_score"]      = round(ae_score, 4)
                    pkt["ae_threshold"]  = round(ae_thr, 4)
                    pkt["ae_root_cause"] = translate_to_english(ae_root_cause)

                    # Single Autoencoder torque anomaly check (events.html and events.log use this)
                    single_torque_anomalies = []
                    worst_single_ae_score = 0.0
                    worst_single_ae_joint = None
                    worst_single_ae_expl = ""
                    single_thr = ae_single_thr or 7.20

                    for i in range(1, 7):
                        feat_name = f"TORQUE_A{i}"
                        feat_dict = ae_single_by_frame_and_feat.get(feat_name, {})
                        row = feat_dict.get(ae_idx)
                        if row:
                            err = float(row.get("error", 0))
                            is_anom = bool(row.get("is_anomaly", False))
                            if is_anom:
                                single_torque_anomalies.append((i, err, row.get("explanation", "")))
                            if err > worst_single_ae_score:
                                worst_single_ae_score = err
                                worst_single_ae_joint = i
                                worst_single_ae_expl = row.get("explanation", "")

                    ae_torque_is_anomaly = len(single_torque_anomalies) > 0

                    if ae_torque_is_anomaly:
                        worst_j_ae = worst_single_ae_joint
                        # If error is very high (ratio >= 1.4), set severity to CRITICAL (red)
                        ratio = worst_single_ae_score / single_thr
                        severity = "CRITICAL" if ratio >= 1.4 else "WARNING"

                        log_torque_model_event(
                            "TORQUE_AUTOENCODER",
                            worst_single_ae_score,
                            single_thr,
                            pkt["frame_no"],
                            pkt["timestamp"],
                            extra_meta={
                                "root_cause": f"TORQUE_A{worst_j_ae}",
                                "worst_joint": worst_j_ae,
                                "explanation": translate_to_english(worst_single_ae_expl),
                            },
                            severity=severity
                        )

                    # COMBINED (any 2-of-3) and TRIPLE (3-of-3) model anomalies
                    pca_anom = bool(model_payload.get("pca_anomaly"))
                    # rf_payload contains the actual RF scores (scored separately from model_payload)
                    rf_anom = bool(rf_payload.get("rf_anomaly"))
                    
                    # Update pkt["ae_anomaly"] and ae_is_anomaly to represent torque anomalies only
                    pkt["ae_anomaly"] = ae_torque_is_anomaly
                    ae_is_anomaly = ae_torque_is_anomaly
                    ae_anom = bool(ae_is_anomaly)
                    models_hit = [m for m, ok in (("PCA", pca_anom), ("AE", ae_anom), ("RF", rf_anom)) if ok]
                    worst_joint_combined = (
                        int(pkt["diffs"].index(max(pkt["diffs"]))) + 1
                        if pkt.get("diffs")
                        else (model_payload.get("rf_worst_joint") or worst_single_ae_joint or 1)
                    )

                    if len(models_hit) >= 3:
                        log_torque_model_event(
                            "TORQUE_TRIPLE_COMBINED",
                            max(ae_score, float(model_payload.get("pca_score") or 0.0), float(model_payload.get("rf_max_residual") or 0.0)),
                            0,
                            pkt["frame_no"],
                            pkt["timestamp"],
                            extra_meta={
                                "models": models_hit,
                                "pca_score": model_payload.get("pca_score"),
                                "ae_score": ae_score,
                                "rf_max_residual": model_payload.get("rf_max_residual"),
                                "worst_joint": worst_joint_combined,
                            },
                            severity="CRITICAL",
                        )
                    elif len(models_hit) == 2:
                        log_torque_model_event(
                            "TORQUE_COMBINED",
                            max(ae_score, float(model_payload.get("pca_score") or 0.0), float(model_payload.get("rf_max_residual") or 0.0)),
                            0,
                            pkt["frame_no"],
                            pkt["timestamp"],
                            extra_meta={
                                "models": models_hit,
                                "pca_score": model_payload.get("pca_score"),
                                "ae_score": ae_score,
                                "rf_max_residual": model_payload.get("rf_max_residual"),
                                "worst_joint": worst_joint_combined,
                            },
                            severity="WARNING",
                        )


                    # -----------------------------------------------
                    # TORQUE_TRIPLE_THREAT:
                    # Fiziksel tork eşiği AYNI ANDA modelden de geçerse
                    # (aynı frame'de hem sensör hem model anomali dedi)
                    # -----------------------------------------------
                    any_model_anomaly = (
                        model_payload.get("pca_anomaly")
                        or pkt.get("ae_anomaly", False)
                    )
                    if pkt.get("anomaly") and any_model_anomaly:
                        key_tt = ("TORQUE_TRIPLE_THREAT",)
                        now_tt = time.time()
                        if key_tt not in last_error_time or now_tt - last_error_time[key_tt] > MODEL_EVENT_COOLDOWN:
                            max_diff   = max(pkt["diffs"])
                            worst_joint = pkt["diffs"].index(max_diff) + 1
                            models_active = []
                            if model_payload.get("pca_anomaly"):
                                models_active.append(f"PCA={model_payload['pca_score']:.4f}")
                            if pkt.get("ae_anomaly"):
                                models_active.append(f"AE={pkt['ae_score']:.4f}")
                            event_tt = {
                                "timestamp": pkt["timestamp"],
                                "type": "TORQUE_TRIPLE_THREAT",
                                "severity": "CRITICAL",
                                "message": (
                                    f"SENSOR + MODEL ANOMALY IN SAME FRAME! "
                                    f"Joint {worst_joint} Δ={max_diff:.3f} | "
                                    + ", ".join(models_active)
                                ),
                                "meta": {
                                    "frame_no":    pkt["frame_no"],
                                    "worst_joint": worst_joint,
                                    "max_diff":    max_diff,
                                    "torque_thr":  TORQUE_THRESHOLD,
                                    "models":      models_active,
                                    "pca_score":   model_payload.get("pca_score"),
                                    "ae_score":    pkt.get("ae_score"),
                                },
                            }
                            error_log.append(event_tt)
                            with open(EVENTS_LOG_FILE, "a", encoding="utf-8") as f:
                                f.write(json.dumps(event_tt) + "\n")
                            last_error_time[key_tt] = now_tt
                            print(f"[torque] ⚡ TRIPLE THREAT frame={pkt['frame_no']} joint={worst_joint}")



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
app.add_middleware(GZipMiddleware, minimum_size=1000)  # compress responses > 1KB
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


@app.get("/autoencoder")
def autoencoder_page():
    return FileResponse(str(BASE_DIR / "static" / "autoencoder.html"))


@app.get("/pca")
def pca_page():
    return FileResponse(str(BASE_DIR / "static" / "pca.html"))


@app.get("/rf")
def rf_page():
    return FileResponse(str(BASE_DIR / "static" / "rf.html"))


@app.get("/api/autoencoder/results")
def autoencoder_results():
    """Serve the pre-computed autoencoder anomaly results JSON."""
    results_path = BASE_DIR / "artifacts" / "autoencoder_results.json"
    if results_path.exists():
        try:
            with open(results_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            data = translate_ae_data(data)
            return JSONResponse(
                content=data,
                headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"}
            )
        except Exception as e:
            print("[autoencoder] Error reading results JSON:", e)
            return JSONResponse(
                content={"error": str(e), "multiple": [], "single": {}},
                status_code=500
            )
    else:
        return JSONResponse(content={
            "meta": {"note": "Run autoencoder/export_results.py to generate results"},
            "multiple": [],
            "single": {}
        })


@app.get("/api/pca/results")
def pca_results():
    """Serve the pre-computed PCA anomaly results JSON."""
    results_path = BASE_DIR / "artifacts" / "pca_results.json"
    if results_path.exists():
        try:
            with open(results_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return JSONResponse(
                content=data,
                headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"}
            )
        except Exception as e:
            print("[pca] Error reading results JSON:", e)
            return JSONResponse(
                content={"error": str(e), "overall": [], "per_feature": {}},
                status_code=500
            )
    else:
        return JSONResponse(content={
            "meta": {"note": "Run models/stats/export_pca_results.py to generate results"},
            "overall": [],
            "per_feature": {}
        })


@app.get("/api/rf/meta")
def rf_meta():
    """Serve graybox RF thresholds and model metadata."""
    meta_path = BASE_DIR / "artifacts" / "graybox_rf_thresholds.json"
    if meta_path.exists():
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return JSONResponse(
                content=data,
                headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"},
            )
        except Exception as e:
            return JSONResponse(content={"error": str(e)}, status_code=500)
    return JSONResponse(
        content={"error": "graybox_rf_thresholds.json not found — run train_graybox_rf.py first"},
        status_code=404,
    )


@app.get("/api/rf/results")
def rf_results():
    """Serve the pre-computed Random Forest anomaly results JSON."""
    results_path = BASE_DIR / "artifacts" / "rf_results.json"
    if results_path.exists():
        try:
            with open(results_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return JSONResponse(
                content=data,
                headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"}
            )
        except Exception as e:
            print("[rf] Error reading results JSON:", e)
            return JSONResponse(
                content={"error": str(e), "overall": [], "per_feature": {}},
                status_code=500
            )
    else:
        return JSONResponse(content={
            "meta": {"note": "Run models/stats/export_rf_results.py to generate results"},
            "overall": [],
            "per_feature": {}
        })



# ---------------------------
# ANOMALY HELPERS (backend-side)
# ---------------------------

def _extract_second_from_ts(ts: str) -> Optional[str]:
    """Extract HH:mm:ss from various timestamp formats."""
    if not ts:
        return None
    import re
    m = re.search(r'(\d{2}:\d{2}:\d{2})', ts)
    if m:
        return m.group(1)
    try:
        from datetime import datetime
        d = datetime.fromisoformat(ts.replace('Z', '+00:00'))
        return d.strftime('%H:%M:%S')
    except Exception:
        pass
    return None


@app.get("/api/anomaly/combined")
def anomaly_combined():
    """
    Return Combined anomalies: same HH:mm:ss + same feature
    detected by both PCA (per_feature) and Autoencoder (single).
    Combined list is NOT extra-counted — it's a cross-reference.
    """
    try:
        pca_path = BASE_DIR / "artifacts" / "pca_results.json"
        ae_path  = BASE_DIR / "artifacts" / "autoencoder_results.json"

        if not pca_path.exists() or not ae_path.exists():
            return JSONResponse(content={"combined": [], "count": 0})

        with open(pca_path, "r", encoding="utf-8") as f:
            pca_data = json.load(f)
        with open(ae_path, "r", encoding="utf-8") as f:
            ae_data = json.load(f)

        # Collect PCA anomalies with secondKey
        pca_anomalies = []
        per_feature = pca_data.get("per_feature", {})
        for feat, items in per_feature.items():
            if not isinstance(items, list):
                continue
            for row in items:
                if not row.get("is_anomaly"):
                    continue
                ts = row.get("timestamp") or row.get("time") or row.get("DateTime") or ""
                second_key = _extract_second_from_ts(str(ts))
                pca_anomalies.append({
                    "feature": row.get("feature") or feat,
                    "frame": row.get("frame_no"),
                    "timestamp": ts,
                    "secondKey": second_key,
                    "pca_score": row.get("pca_score"),
                    "feat_threshold": row.get("feat_threshold"),
                    "severity_ratio": row.get("severity_ratio"),
                })

        # Collect AE single anomalies with secondKey
        ae_anomalies = []
        ae_single = ae_data.get("single", {})
        for feat, items in ae_single.items():
            if not isinstance(items, list):
                continue
            for row in items:
                if not row.get("is_anomaly"):
                    continue
                ts = row.get("timestamp") or row.get("time") or ""
                second_key = _extract_second_from_ts(str(ts))
                ae_anomalies.append({
                    "feature": row.get("feature") or feat,
                    "timestamp": ts,
                    "secondKey": second_key,
                    "error": row.get("error"),
                    "threshold": row.get("threshold"),
                    "severity_ratio": row.get("severity_ratio"),
                })

        # Match: same second + same feature
        combined = []
        for pca in pca_anomalies:
            if not pca["secondKey"] or not pca["feature"]:
                continue
            for ae in ae_anomalies:
                if not ae["secondKey"] or not ae["feature"]:
                    continue
                if pca["secondKey"] == ae["secondKey"] and pca["feature"] == ae["feature"]:
                    combined.append({
                        "time": pca["secondKey"],
                        "feature": pca["feature"],
                        "pcaScore": pca["pca_score"],
                        "aeError": ae["error"],
                        "pcaFrame": pca["frame"],
                        "pcaTimestamp": pca["timestamp"],
                        "aeTimestamp": ae["timestamp"],
                        "matchReason": "same second and same feature",
                    })

        return JSONResponse(content={"combined": combined, "count": len(combined)})

    except Exception as e:
        print("[anomaly/combined] error:", e)
        return JSONResponse(content={"combined": [], "count": 0, "error": str(e)}, status_code=500)


@app.get("/api/anomaly/torque-counts")
def anomaly_torque_counts():
    """
    Return anomaly counts per TORQUE_A1-A6 from PCA and Autoencoder results.
    Combined records are NOT counted separately.
    """
    TORQUE_FEATURES = [
        "TORQUE_A1","TORQUE_A2","TORQUE_A3",
        "TORQUE_A4","TORQUE_A5","TORQUE_A6"
    ]
    counts = {f: {"pca": 0, "ae": 0, "total": 0} for f in TORQUE_FEATURES}

    try:
        pca_path = BASE_DIR / "artifacts" / "pca_results.json"
        ae_path  = BASE_DIR / "artifacts" / "autoencoder_results.json"

        if pca_path.exists():
            with open(pca_path, "r", encoding="utf-8") as f:
                pca_data = json.load(f)
            per_feature = pca_data.get("per_feature", {})
            for feat, items in per_feature.items():
                if feat not in TORQUE_FEATURES:
                    continue
                if not isinstance(items, list):
                    continue
                for row in items:
                    if row.get("is_anomaly"):
                        counts[feat]["pca"] += 1

        if ae_path.exists():
            with open(ae_path, "r", encoding="utf-8") as f:
                ae_data = json.load(f)
            ae_single = ae_data.get("single", {})
            for feat, items in ae_single.items():
                if feat not in TORQUE_FEATURES:
                    continue
                if not isinstance(items, list):
                    continue
                for row in items:
                    if row.get("is_anomaly"):
                        counts[feat]["ae"] += 1

        for feat in TORQUE_FEATURES:
            counts[feat]["total"] = counts[feat]["pca"] + counts[feat]["ae"]

        return JSONResponse(content={"counts": counts})

    except Exception as e:
        print("[anomaly/torque-counts] error:", e)
        return JSONResponse(content={"counts": counts, "error": str(e)}, status_code=500)


@app.get("/frames/latest")
def get_latest():
    if latest_frame is None:
        return {"status": "no data yet"}
    return latest_frame


@app.get("/errors")
def get_errors():
    return error_log[-200:]


# ---------------------------
# LATEST 20 EVENTS HELPER
# ---------------------------
def get_latest_20_events() -> list:
    """
    Return the 20 newest anomaly events, sorted by timestamp descending.
    Uses in-memory error_log; falls back to reading events.log if empty.
    """
    events = list(error_log)
    if not events:
        # Fallback: read from disk (useful after page reload with no live data yet)
        try:
            with open(EVENTS_LOG_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            events.append(json.loads(line))
                        except Exception:
                            pass
        except Exception:
            pass

    # Sort by timestamp descending (newest first) and take top 20
    def _ts_key(e):
        ts = e.get("timestamp", "")
        try:
            from datetime import datetime
            return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            return ts  # string compare fallback

    try:
        events_sorted = sorted(events, key=_ts_key, reverse=True)
    except Exception:
        events_sorted = events[::-1]  # reverse order if sort fails

    return events_sorted[:20]
class ChatRequest(BaseModel):
    message: str

@app.post("/chat")
async def chat(req: ChatRequest):

    thermal_data   = latest_frame  if latest_frame  else {}
    torque_data    = latest_torque if latest_torque else {}
    recent_events  = get_latest_20_events()

    # Autoencoder summary: only REAL anomalies (is_anomaly == True)
    all_multiple = KB_AUTOENCODER.get("multiple", [])
    real_anomalies = [a for a in all_multiple if a.get("is_anomaly") == True]
    real_anomalies = sorted(real_anomalies, key=lambda x: x.get("error", 0), reverse=True)[:5]
    ae_meta = KB_AUTOENCODER.get("meta", {})

    prompt = f"""
You are MindTwin AI — a specialized robot health assistant embedded in a KUKA robotic arm
digital-twin monitoring dashboard.

You ONLY answer questions strictly related to:
  - Robot health monitoring (KUKA robot arm)
  - Thermal anomalies (motor temperatures)
  - Torque anomalies (joint torques A1–A6)
  - PCA / Isolation Forest / Random Forest / Autoencoder anomaly detection
  - Predictive maintenance and actionable maintenance suggestions

If the user asks ANYTHING unrelated to these topics (general knowledge, coding,
personal questions, weather, math, etc.), respond EXACTLY with this message and nothing else:
  "I can only answer questions related to robot health monitoring and anomaly detection."

RULES:
- Answer ONLY in English.
- Keep answers to 1-2 sentences maximum.
- Never use markdown, bullet points, or headings.
- Always base your answer on the latest 20 anomaly events provided below.
- If system looks normal, say so briefly.
- If no live data, say "No live data available yet."
- Give actionable maintenance suggestions when anomalies are present.
- Do NOT use phrases like "Based on the data", "The robot's health", "Overall status".

── KNOWLEDGE BASE ────────────────────────────────────────────────────────────

[PCA Thresholds]: {json.dumps(KB_PCA_THRESHOLDS)}
[IForest Threshold]: {json.dumps(KB_IFOREST_THR)}
[Autoencoder Meta]: {json.dumps(ae_meta)}
[Top 5 AE Anomalies]: {json.dumps(real_anomalies)}

── LIVE DATA ─────────────────────────────────────────────────────────────────

[Latest Thermal Frame]: {json.dumps(thermal_data)}
[Latest Torque Frame]: {json.dumps(torque_data)}
[Latest 20 Anomaly Events]: {json.dumps(recent_events)}

── USER QUESTION ─────────────────────────────────────────────────────────────
{req.message}
"""

    try:
        response = gemini_model.generate_content(prompt)
        return {"reply": response.text}

    except Exception as e:
        print("CHAT ERROR:", e)
        return {"reply": f"AI error: {str(e)}"}



# ---------------------------
# GRAPH EXPLANATION API
# ---------------------------

_PAGE_PROMPTS = {
    "thermal": (
        "You are analyzing the Thermal Monitor graph of a KUKA robot arm. "
        "The graph shows live temperature readings (T_min, T_mean, T_max in °C) over recent frames. "
    ),
    "torque": (
        "You are analyzing the Torque Monitor graph of a KUKA robot arm. "
        "The graph shows torque anomaly levels (0=normal, 1=single anomaly, 2=combined, 3=triple) for joints A1-A6. "
    ),
    "autoencoder": (
        "You are analyzing the Autoencoder Anomaly Detection graph of a KUKA robot arm. "
        "The graph shows reconstruction error vs threshold for torque sensor data. When error exceeds threshold, it signals an anomaly. "
    ),
    "pca": (
        "You are analyzing the PCA Anomaly Detection graph of a KUKA robot arm. "
        "The graph shows PCA reconstruction error vs threshold for joint torque data. Spikes above the threshold indicate anomalies. "
    ),
    "rf": (
        "You are analyzing the Random Forest (Grey-box Bagging Regressor) Anomaly Detection graph of a KUKA robot arm. "
        "The graph shows RF residuals per joint (A1-A6) vs their 3σ thresholds. Points above threshold indicate torque anomalies. "
    ),
}

_PAGE_EVENT_TYPES = {
    "thermal":     ["THERMAL"],
    "torque":      ["TORQUE_PCA", "TORQUE_AUTOENCODER", "TORQUE_RF", "TORQUE_COMBINED", "TORQUE_TRIPLE_COMBINED", "TORQUE_TRIPLE_THREAT"],
    "autoencoder": ["TORQUE_AUTOENCODER"],
    "pca":         ["TORQUE_PCA"],
    "rf":          ["TORQUE_RF"],
}

_NO_EVENTS_MSG = "No recent anomaly events found. Continue monitoring the system."


@app.get("/api/explain/{page}")
async def explain_page(page: str):
    """
    Return a short AI-generated explanation (2-4 sentences) for the given page's graph.
    Based ONLY on the latest 20 anomaly/event records.
    """
    page = page.lower()
    if page not in _PAGE_PROMPTS:
        return JSONResponse(content={"explanation": _NO_EVENTS_MSG}, status_code=200)

    recent_events = get_latest_20_events()
    # Filter to relevant event types for this page
    event_types = _PAGE_EVENT_TYPES.get(page, [])
    relevant = [e for e in recent_events if e.get("type", "") in event_types] if event_types else recent_events

    if not relevant and not (page == "thermal" and latest_frame) and not (page in ("torque", "rf", "autoencoder", "pca") and latest_torque):
        return JSONResponse(content={"explanation": _NO_EVENTS_MSG})

    thermal_ctx = json.dumps(latest_frame) if latest_frame else "No thermal data"
    torque_ctx  = json.dumps(latest_torque) if latest_torque else "No torque data"

    base_prompt = _PAGE_PROMPTS[page]

    prompt = f"""{base_prompt}

CONTEXT:
- Latest 20 anomaly events (filtered for this page): {json.dumps(relevant)}
- Latest thermal frame: {thermal_ctx}
- Latest torque frame: {torque_ctx}

TASK: Write a plain English explanation of what the current graph shows. Follow ALL rules below:
- Answer in English only.
- Maximum 2-4 sentences. No more.
- State clearly whether the system looks NORMAL, WARNING, or CRITICAL.
- Mention the most relevant component or joint if applicable (e.g., "Joint A3", "thermal sensor").
- End with ONE short action sentence such as:
  "Monitor the temperature.", "Inspect Joint A3.", "Reduce robot load.", "Continue monitoring."
- Do NOT use markdown, bullet points, headers, or any formatting.
- Do NOT start with "Based on" or "Overall".
- If no anomaly events exist, output exactly: "{_NO_EVENTS_MSG}"
"""

    try:
        response = gemini_model.generate_content(prompt)
        explanation = response.text.strip()
        return JSONResponse(content={"explanation": explanation})
    except Exception as e:
        print(f"[explain/{page}] Gemini error:", e)
        return JSONResponse(content={"explanation": _NO_EVENTS_MSG})


@app.get("/api/ai-events-summary")
async def ai_events_summary():
    """
    Return an AI-generated summary of the latest 20 anomaly events for the Events page.
    """
    recent_events = get_latest_20_events()

    if not recent_events:
        return JSONResponse(content={
            "summary": _NO_EVENTS_MSG,
            "most_frequent_type": "None",
            "most_affected_joint": "N/A",
            "highest_severity": "None",
            "thermal_status": "normal",
            "torque_status": "normal",
            "model_status": "normal",
            "recommended_action": "Continue monitoring the system."
        })

    # Compute stats from latest 20 events
    from collections import Counter

    type_counts = Counter(e.get("type", "UNKNOWN") for e in recent_events)
    most_frequent_type = type_counts.most_common(1)[0][0] if type_counts else "Unknown"

    joint_counts = Counter()
    for e in recent_events:
        wj = e.get("meta", {}).get("worst_joint")
        if wj:
            joint_counts[f"A{wj}"] += 1
    most_affected_joint = joint_counts.most_common(1)[0][0] if joint_counts else "N/A"

    severities = [e.get("severity", "INFO") for e in recent_events]
    highest_severity = "CRITICAL" if "CRITICAL" in severities else ("WARNING" if "WARNING" in severities else "INFO")

    thermal_events = [e for e in recent_events if e.get("type") == "THERMAL"]
    torque_events  = [e for e in recent_events if e.get("type", "").startswith("TORQUE_")]
    thermal_status = "critical" if any(e.get("severity") == "CRITICAL" for e in thermal_events) else ("warning" if thermal_events else "normal")
    torque_status  = "critical" if any(e.get("severity") == "CRITICAL" for e in torque_events) else ("warning" if torque_events else "normal")
    model_events   = [e for e in recent_events if e.get("type") in ("TORQUE_PCA", "TORQUE_AUTOENCODER", "TORQUE_RF", "TORQUE_COMBINED", "TORQUE_TRIPLE_COMBINED")]
    model_status   = "critical" if any(e.get("severity") == "CRITICAL" for e in model_events) else ("warning" if model_events else "normal")

    prompt = f"""You are analyzing the latest 20 anomaly events from a KUKA robot arm digital-twin dashboard.

Latest 20 events: {json.dumps(recent_events)}

Summary statistics computed:
- Most frequent anomaly type: {most_frequent_type}
- Most affected joint: {most_affected_joint}
- Highest severity: {highest_severity}
- Thermal status: {thermal_status}
- Torque status: {torque_status}
- Model anomaly status: {model_status}

Write a concise AI Events Summary in English. It must:
1. Be 2-3 sentences maximum.
2. Mention the most frequent anomaly type and most affected joint.
3. State the overall system health (normal / warning / critical).
4. End with one short recommended maintenance action.
5. No markdown, no bullet points, no headers.
6. Do NOT start with "Based on the data".
"""

    try:
        response = gemini_model.generate_content(prompt)
        ai_text = response.text.strip()
    except Exception as e:
        print("[ai-events-summary] Gemini error:", e)
        ai_text = f"Latest {len(recent_events)} events analyzed. Most frequent: {most_frequent_type}, most affected joint: {most_affected_joint}, highest severity: {highest_severity}."

    return JSONResponse(content={
        "summary": ai_text,
        "most_frequent_type": most_frequent_type,
        "most_affected_joint": most_affected_joint,
        "highest_severity": highest_severity,
        "thermal_status": thermal_status,
        "torque_status": torque_status,
        "model_status": model_status,
        "recommended_action": "Inspect " + most_affected_joint + " and monitor closely." if most_affected_joint != "N/A" else "Continue monitoring the system.",
        "event_count": len(recent_events),
    })


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
