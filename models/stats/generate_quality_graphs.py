"""
Model kalite grafikleri üretir (AE, RF, PCA).

Her model için 2 grafik (toplam 6 PNG) üretir:
  - timeseries: skor/zaman + threshold + anomaly işaretleri
  - summary   : (AE/PCA için root-cause bar chart, RF için per-axis quantile vs threshold)

Örnek:
  python -X utf8 models/stats/generate_quality_graphs.py \\
      --out artifacts/quality_graphs \\
      --rf-csv data/kuka_log600_scnd_20hz(canta)_injected.csv
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Optional

import joblib
import numpy as np

# Matplotlib cache path can be unwritable in sandboxed environments.
os.environ.setdefault("MPLCONFIGDIR", str(Path("/tmp/matplotlib-cache").resolve()))
# Fontconfig cache errors are noisy when HOME isn't writable; route caches to /tmp.
os.environ.setdefault("XDG_CACHE_HOME", str(Path("/tmp").resolve()))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ── path setup ────────────────────────────────────────────────────────────────
_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here))
sys.path.insert(0, str(_here.parents[0]))  # models/

try:
    from feature_builder import detect_torque_columns, load_csv_rows
except Exception as e:
    raise SystemExit(
        "[graphs] HATA: feature_builder import edilemedi. 'mindtwin/' dizininden çalıştır."
    ) from e

try:
    from urdf_dynamics import compute_inverse_dynamics_batch
except Exception:
    compute_inverse_dynamics_batch = None


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _safe_float(x, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if np.isfinite(v) else default
    except Exception:
        return default


def _parse_time_like(ts: str) -> Optional[float]:
    """
    '14:33:59.698' gibi timestamp'leri saniye cinsine çevirir.
    ISO format varsa datetime ile çözer. Çözülemezse None.
    """
    if not ts:
        return None
    ts = str(ts).strip()

    # ISO / datetime-ish
    try:
        dt = datetime.fromisoformat(ts.replace(" ", "T"))
        # absolute epoch seconds
        return dt.timestamp()
    except Exception:
        pass

    # HH:MM:SS(.mmm)
    m = re.match(r"^(?P<h>\d{1,2}):(?P<m>\d{2}):(?P<s>\d{2})(?:\.(?P<ms>\d{1,6}))?$", ts)
    if not m:
        return None
    h = int(m.group("h"))
    mi = int(m.group("m"))
    s = int(m.group("s"))
    ms = m.group("ms") or "0"
    msf = float(f"0.{ms}")
    return h * 3600 + mi * 60 + s + msf


def _normalize_x(xs: list[Optional[float]]) -> np.ndarray:
    """
    Eğer değerler epoch ise relative'e çevirir, değilse index döndürür.
    """
    arr = np.array([x if x is not None else np.nan for x in xs], dtype=float)
    if np.all(np.isnan(arr)):
        return np.arange(len(xs), dtype=float)

    # If most values are large (epoch), make relative.
    finite = arr[np.isfinite(arr)]
    if finite.size == 0:
        return np.arange(len(xs), dtype=float)
    if np.median(finite) > 10_000:  # heuristic: epoch seconds
        t0 = float(finite[0])
        arr = arr - t0
    # Fill NaNs with index for continuity
    nan_idx = np.where(~np.isfinite(arr))[0]
    if nan_idx.size:
        arr[nan_idx] = nan_idx.astype(float)
    return arr


def _plot_timeseries_with_threshold(
    *,
    out_path: Path,
    x: np.ndarray,
    y: np.ndarray,
    threshold: float | np.ndarray,
    anomaly_mask: Optional[np.ndarray] = None,
    title: str,
    y_label: str,
    x_label: str = "sample",
    secondary: Optional[dict] = None,
) -> None:
    plt.figure(figsize=(12, 4), dpi=180)
    ax = plt.gca()

    ax.plot(x, y, lw=1.2, label=y_label, color="#1f77b4")
    if isinstance(threshold, np.ndarray):
        ax.plot(x, threshold, lw=1.2, label="threshold", color="#ff7f0e", alpha=0.9)
    else:
        ax.axhline(float(threshold), lw=1.2, label="threshold", color="#ff7f0e", alpha=0.9)

    if anomaly_mask is not None and anomaly_mask.any():
        ax.scatter(x[anomaly_mask], y[anomaly_mask], s=10, color="#d62728", label="anomaly", zorder=3)

    if secondary is not None:
        ax2 = ax.twinx()
        ax2.plot(
            x,
            secondary["y"],
            lw=1.0,
            label=secondary.get("label", "secondary"),
            color=secondary.get("color", "#2ca02c"),
            alpha=0.9,
        )
        thr2 = secondary.get("threshold")
        if thr2 is not None:
            if isinstance(thr2, np.ndarray):
                ax2.plot(x, thr2, lw=1.0, color=secondary.get("thr_color", "#9467bd"), alpha=0.9, label="threshold2")
            else:
                ax2.axhline(float(thr2), lw=1.0, color=secondary.get("thr_color", "#9467bd"), alpha=0.9, label="threshold2")
        ax2.set_ylabel(secondary.get("y_label", ""))

        # legend merge
        lines = ax.get_lines() + ax2.get_lines()
        labels = [l.get_label() for l in lines]
        if anomaly_mask is not None and anomaly_mask.any():
            # scatter not included in get_lines
            handles, hlbl = ax.get_legend_handles_labels()
            ax.legend(handles, hlbl, loc="upper right", frameon=False)
        else:
            ax.legend(lines, labels, loc="upper right", frameon=False)
    else:
        ax.legend(loc="upper right", frameon=False)

    ax.set_title(title)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.grid(True, alpha=0.25)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def _plot_bar_counts(
    *,
    out_path: Path,
    counts: Counter,
    title: str,
    x_label: str,
    y_label: str = "count",
    top_k: int = 12,
) -> None:
    items = counts.most_common(top_k)
    labels = [k for k, _ in items]
    values = [v for _, v in items]

    plt.figure(figsize=(10, 4.8), dpi=180)
    ax = plt.gca()
    ax.barh(labels[::-1], values[::-1], color="#1f77b4", alpha=0.9)
    ax.set_title(title)
    ax.set_xlabel(y_label)
    ax.set_ylabel(x_label)
    ax.grid(True, axis="x", alpha=0.25)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def _ae_graphs(ae_json: Path, out_dir: Path) -> list[Path]:
    data = json.loads(ae_json.read_text(encoding="utf-8"))
    meta = data.get("meta", {}) if isinstance(data, dict) else {}
    multiple = data.get("multiple", []) if isinstance(data, dict) else []

    xs = [_parse_time_like(r.get("timestamp")) for r in multiple]
    x = _normalize_x(xs)

    y = np.array([_safe_float(r.get("error")) for r in multiple], dtype=float)
    thr = float(meta.get("multiple_threshold") or (multiple[0].get("threshold") if multiple else 0.0) or 0.0)
    anomaly = np.array([bool(r.get("is_anomaly")) for r in multiple], dtype=bool)

    out1 = out_dir / "ae_timeseries.png"
    _plot_timeseries_with_threshold(
        out_path=out1,
        x=x,
        y=y,
        threshold=thr,
        anomaly_mask=anomaly,
        title="AE — Reconstruction Error vs Threshold (Multiple)",
        y_label="reconstruction error",
        x_label="time (s)" if np.nanmax(x) > 0 else "sample",
    )

    # Root-cause summary (only for anomalies)
    root_causes = [r.get("root_cause") for r in multiple if r.get("is_anomaly") and r.get("root_cause")]
    counts = Counter(root_causes)
    out2 = out_dir / "ae_root_cause_counts.png"
    _plot_bar_counts(
        out_path=out2,
        counts=counts if counts else Counter({"no_anomaly": 0}),
        title="AE — Root Cause Counts (Anomalies)",
        x_label="root_cause",
        y_label="count",
        top_k=12,
    )
    return [out1, out2]


def _pca_graphs(pca_json: Path, out_dir: Path) -> list[Path]:
    data = json.loads(pca_json.read_text(encoding="utf-8"))
    meta = data.get("meta", {}) if isinstance(data, dict) else {}
    overall = data.get("overall", []) if isinstance(data, dict) else []

    xs = [_parse_time_like(r.get("timestamp")) for r in overall]
    x = _normalize_x(xs)

    pca_score = np.array([_safe_float(r.get("pca_score")) for r in overall], dtype=float)
    pca_thr = np.array([_safe_float(r.get("pca_threshold", meta.get("threshold", 0.0))) for r in overall], dtype=float)

    if_score = np.array([_safe_float(r.get("iforest_score")) for r in overall], dtype=float)
    if_thr = np.array([_safe_float(r.get("iforest_threshold", meta.get("iforest_threshold", 0.0))) for r in overall], dtype=float)

    anomaly = np.array([bool(r.get("model_anomaly")) for r in overall], dtype=bool)

    out1 = out_dir / "pca_timeseries_dual_axis.png"
    _plot_timeseries_with_threshold(
        out_path=out1,
        x=x,
        y=pca_score,
        threshold=pca_thr,
        anomaly_mask=anomaly,
        title="PCA — Scores with Thresholds (Dual Axis)",
        y_label="pca_score",
        x_label="time (s)" if np.nanmax(x) > 0 else "sample",
        secondary={
            "y": if_score,
            "threshold": if_thr,
            "label": "iforest_score",
            "color": "#2ca02c",
            "thr_color": "#9467bd",
            "y_label": "iforest_score",
        },
    )

    worst_features = [r.get("worst_feature") for r in overall if r.get("model_anomaly") and r.get("worst_feature")]
    counts = Counter(worst_features)
    out2 = out_dir / "pca_worst_feature_counts.png"
    _plot_bar_counts(
        out_path=out2,
        counts=counts if counts else Counter({"no_anomaly": 0}),
        title="PCA — Worst Feature Counts (Anomalies)",
        x_label="worst_feature",
        y_label="count",
        top_k=12,
    )
    return [out1, out2]


def _detect_col(headers: list[str], name: str) -> Optional[str]:
    norm = {k.lower(): k for k in headers if isinstance(k, str)}
    return norm.get(name.lower())


def _detect_vel_cols(headers: list[str]) -> Optional[list[str]]:
    norm = {k.lower(): k for k in headers if isinstance(k, str)}
    cols = []
    for i in range(1, 7):
        c = norm.get(f"vel_axis_act_a{i}")
        if c:
            cols.append(c)
        else:
            return None
    return cols


def _detect_curr_cols(headers: list[str]) -> Optional[list[str]]:
    norm = {k.lower(): k for k in headers if isinstance(k, str)}
    cols = []
    for i in range(1, 7):
        c = norm.get(f"curr_a{i}")
        if c:
            cols.append(c)
        else:
            return None
    return cols


def _parse_axis_act(raw: str) -> Optional[list[float]]:
    tokens = re.findall(r"A(\d)\s+([-\d.]+(?:[Ee][-+]?\d+)?)", raw or "")
    q = [0.0] * 6
    found = 0
    for s, v in tokens:
        idx = int(s) - 1
        if 0 <= idx < 6:
            q[idx] = float(v) * (np.pi / 180.0)
            found += 1
    return q if found > 0 else None


def _parse_relative_seconds_from_rows(rows: list[dict]) -> np.ndarray:
    ts = []
    for r in rows:
        raw = r.get("timestamp") or r.get("Timestamp") or ""
        try:
            ts.append(datetime.fromisoformat(str(raw).replace(" ", "T")))
        except Exception:
            # fallback: monotonic sample index at ~10Hz
            return np.arange(len(rows), dtype=float) * 0.1
    if len(ts) > 1:
        t0 = ts[0]
        return np.array([(t - t0).total_seconds() for t in ts], dtype=float)
    return np.arange(len(rows), dtype=float)


def _rf_scores_from_csv(
    *,
    rows: list[dict],
    rf_model_pkl: Path,
    rf_thresholds_json: Path,
    urdf_path: Optional[str],
    limit: Optional[int],
) -> dict:
    bundle = joblib.load(str(rf_model_pkl))
    models = bundle.get("models") or []
    for mdl in models:
        if hasattr(mdl, "n_jobs"):
            try:
                mdl.n_jobs = 1
            except Exception:
                pass

    meta = json.loads(rf_thresholds_json.read_text(encoding="utf-8"))
    thresholds = np.array(meta["thresholds"], dtype=float)
    producer_scale = float(meta.get("producer_scale", 1.0) or 1.0)
    use_inv_dyn = bool(meta.get("inverse_dynamics_used", False))
    curr_used = bool(meta.get("curr_used", False))

    if limit is not None:
        rows = rows[:limit]

    headers = list(rows[0].keys())
    axis_act_col = _detect_col(headers, "axis_act")
    vel_cols = _detect_vel_cols(headers)
    curr_cols = _detect_curr_cols(headers) if curr_used else None
    torque_cols = detect_torque_columns(headers)[:6]

    # q (rad)
    q_all = np.zeros((len(rows), 6), dtype=float)
    for i, r in enumerate(rows):
        raw = r.get(axis_act_col or "", "") if axis_act_col else ""
        parsed = _parse_axis_act(str(raw))
        if parsed:
            q_all[i] = np.array(parsed, dtype=float)

    # time + dt
    t = _parse_relative_seconds_from_rows(rows)
    dt = float(np.median(np.diff(t))) if len(t) > 1 else float(meta.get("dt_median", 0.01))
    if not np.isfinite(dt) or dt <= 0:
        dt = float(meta.get("dt_median", 0.01) or 0.01)

    # velocity source
    if vel_cols:
        vel_raw = np.array(
            [[_safe_float(r.get(c)) for c in vel_cols] for r in rows],
            dtype=float,
        ) * (np.pi / 180.0)
        q_dot = vel_raw
        q_ddot = np.gradient(q_dot, dt, axis=0)
        vel_source = "VEL_AXIS_ACT"
    else:
        q_dot = np.gradient(q_all, dt, axis=0)
        q_ddot = np.gradient(q_dot, dt, axis=0)
        vel_source = "AXIS_ACT türevi"

    # torque (producer scaled)
    tau = np.zeros((len(rows), 6), dtype=float)
    for j, col in enumerate(torque_cols):
        tau[:, j] = np.array([_safe_float(r.get(col)) for r in rows], dtype=float)
    tau = tau / producer_scale

    # base features: [q, qd, (curr)]
    if curr_cols:
        curr = np.array(
            [[_safe_float(r.get(c)) for c in curr_cols] for r in rows],
            dtype=float,
        )
        X_base = np.hstack([q_all, q_dot, curr])
    else:
        X_base = np.hstack([q_all, q_dot])

    # inverse dynamics feature: tau_model_Aj
    tau_model = None
    if use_inv_dyn and urdf_path and compute_inverse_dynamics_batch is not None:
        try:
            tau_model = compute_inverse_dynamics_batch(str(urdf_path), q_all, q_dot, q_ddot)
        except Exception:
            tau_model = None

    # predict residuals for each axis
    residuals = np.zeros((len(rows), 6), dtype=float)
    for j, mdl in enumerate(models[:6]):
        X = np.hstack([X_base, tau_model[:, j : j + 1]]) if tau_model is not None else X_base
        pred = mdl.predict(X)
        residuals[:, j] = np.abs(tau[:, j] - pred)

    worst_axis = np.argmax(residuals, axis=1)
    max_residual = residuals[np.arange(len(rows)), worst_axis]
    worst_thr = thresholds[worst_axis]
    anomaly = (residuals > thresholds.reshape(1, -1)).any(axis=1)

    return {
        "t": t,
        "vel_source": vel_source,
        "residuals": residuals,
        "thresholds": thresholds,
        "max_residual": max_residual,
        "worst_axis": worst_axis,
        "worst_thr": worst_thr,
        "anomaly": anomaly,
    }


def _rf_graphs(
    rf_csv: Path,
    rf_model_pkl: Path,
    rf_thresholds_json: Path,
    out_dir: Path,
    limit: Optional[int],
) -> list[Path]:
    rows = load_csv_rows(str(rf_csv))
    meta = json.loads(rf_thresholds_json.read_text(encoding="utf-8"))
    urdf_path = meta.get("urdf_path")

    scores = _rf_scores_from_csv(
        rows=rows,
        rf_model_pkl=rf_model_pkl,
        rf_thresholds_json=rf_thresholds_json,
        urdf_path=urdf_path,
        limit=limit,
    )

    t = scores["t"]
    x = t if np.nanmax(t) > 0 else np.arange(len(t), dtype=float)

    out1 = out_dir / "rf_timeseries_max_residual.png"
    _plot_timeseries_with_threshold(
        out_path=out1,
        x=x,
        y=scores["max_residual"],
        threshold=scores["worst_thr"],
        anomaly_mask=scores["anomaly"],
        title=f"RF — Max Residual vs Threshold (vel={scores['vel_source']})",
        y_label="max_residual",
        x_label="time (s)" if np.nanmax(x) > 0 else "sample",
    )

    # summary: per-axis 95th percentile vs threshold
    res = scores["residuals"]
    thresholds = scores["thresholds"]
    q95 = np.quantile(res, 0.95, axis=0)

    labels = [f"A{i}" for i in range(1, 7)]
    plt.figure(figsize=(10, 4.8), dpi=180)
    ax = plt.gca()
    ax.bar(labels, q95, color="#1f77b4", alpha=0.9, label="residual q95")
    ax.plot(labels, thresholds, color="#ff7f0e", lw=2.0, marker="o", label="threshold")
    ax.set_title("RF — Residual Quantile (q95) vs Threshold")
    ax.set_xlabel("axis")
    ax.set_ylabel("residual")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend(frameon=False, loc="upper right")
    plt.tight_layout()
    out2 = out_dir / "rf_residual_q95_vs_threshold.png"
    plt.savefig(out2)
    plt.close()
    return [out1, out2]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="artifacts/quality_graphs", help="output directory")

    ap.add_argument("--ae-json", default="artifacts/autoencoder_results.json", help="AE results json")
    ap.add_argument("--pca-json", default="artifacts/pca_results.json", help="PCA results json")

    ap.add_argument("--rf-csv", default=None, help="RF scoring için CSV (örn: data/..._injected.csv)")
    ap.add_argument("--rf-model", default="artifacts/graybox_rf_models.pkl", help="RF model pkl")
    ap.add_argument("--rf-thresholds", default="artifacts/graybox_rf_thresholds.json", help="RF thresholds json")

    ap.add_argument("--limit", type=int, default=None, help="ilk N sample ile sınırla (debug)")
    args = ap.parse_args()

    out_dir = Path(args.out)
    _ensure_dir(out_dir)

    produced: list[Path] = []

    ae_json = Path(args.ae_json)
    if ae_json.exists():
        produced += _ae_graphs(ae_json, out_dir)
    else:
        print(f"[graphs] AE json bulunamadi: {ae_json}")

    pca_json = Path(args.pca_json)
    if pca_json.exists():
        produced += _pca_graphs(pca_json, out_dir)
    else:
        print(f"[graphs] PCA json bulunamadi: {pca_json}")

    if args.rf_csv:
        rf_csv = Path(args.rf_csv)
        rf_model = Path(args.rf_model)
        rf_thr = Path(args.rf_thresholds)
        if not rf_csv.exists():
            print(f"[graphs] RF csv bulunamadi: {rf_csv}")
        elif not rf_model.exists():
            print(f"[graphs] RF model bulunamadi: {rf_model}")
        elif not rf_thr.exists():
            print(f"[graphs] RF thresholds bulunamadi: {rf_thr}")
        else:
            produced += _rf_graphs(rf_csv, rf_model, rf_thr, out_dir, args.limit)
    else:
        print("[graphs] RF grafikleri icin --rf-csv verin (ornek: data/..._injected.csv)")

    print(f"[graphs] OK: {len(produced)} grafik → {out_dir}")
    for p in produced:
        print(f"  - {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
