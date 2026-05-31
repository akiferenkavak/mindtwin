import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import numpy as np

try:
    from ..feature_builder import (
        calibrate_scale,
        detect_torque_columns,
        load_csv_rows,
        to_float,
    )
    from ..model_manager import TorqueModelManager
except ImportError:  # script execution fallback (python models/stats/analyze_quartile_anomalies.py ...)
    from pathlib import Path
    import sys

    root = Path(__file__).resolve().parents[2]
    sys.path.append(str(root))
    from models.feature_builder import (  # noqa: E402
        calibrate_scale,
        detect_torque_columns,
        load_csv_rows,
        to_float,
    )
    from models.model_manager import TorqueModelManager  # noqa: E402


def parse_ratio(val: str) -> float:
    v = float(val)
    if v < 0 or v > 1:
        raise argparse.ArgumentTypeError("ratio 0-1 araliginda olmali")
    return v


def _norm_headers(headers) -> dict:
    return {str(k).lower(): k for k in headers if isinstance(k, str)}


def _detect_axis_act_col(headers) -> Optional[str]:
    return _norm_headers(headers).get("axis_act")


def _detect_vel_axis_act_cols(headers) -> Optional[list[str]]:
    norm = _norm_headers(headers)
    cols = []
    for i in range(1, 7):
        key = f"vel_axis_act_a{i}"
        if key in norm:
            cols.append(norm[key])
        else:
            return None
    return cols


def _detect_curr_cols(headers) -> Optional[list[str]]:
    norm = _norm_headers(headers)
    cols = []
    for i in range(1, 7):
        key = f"curr_a{i}"
        if key in norm:
            cols.append(norm[key])
        else:
            return None
    return cols


def _parse_axis_act(raw: str) -> Optional[list[float]]:
    """
    Parse AXIS_ACT string like:
      "{E6AXIS: A1 -6.06, A2 -136.97, ...}"  (deg)
    -> [q1..q6] in radians.
    """
    if not raw:
        return None
    tokens = re.findall(r"A(\d)\s+([-\d.]+(?:[Ee][-+]?\d+)?)", raw)
    q = [0.0] * 6
    found = 0
    for idx_str, val_str in tokens:
        idx = int(idx_str) - 1
        if 0 <= idx < 6:
            q[idx] = float(val_str) * (np.pi / 180.0)  # deg -> rad
            found += 1
    return q if found > 0 else None


def _build_feature_matrix_with_index(rows, torque_cols, limit=None):
    feats = []
    idxs = []
    timestamps = []

    take = len(rows) if limit is None else min(len(rows), limit)
    for i in range(take):
        r = rows[i]
        vals = [to_float(r.get(k)) for k in torque_cols]
        if any(v is None for v in vals):
            continue
        if any(not np.isfinite(v) for v in vals):
            continue
        feats.append([float(v) for v in vals])
        idxs.append(i)
        timestamps.append(r.get("timestamp") or r.get("Timestamp") or "")

    if not feats:
        raise RuntimeError("Feature matrisi bos. Torque kolonlarini kontrol et.")

    return np.asarray(feats, dtype=float), idxs, timestamps


def _inject_window_mask(n: int, inject_ratio: float) -> np.ndarray:
    if inject_ratio <= 0 or n <= 0:
        return np.zeros(n, dtype=bool)
    win_len = max(1, int(n * inject_ratio))
    start = (n - win_len) // 2
    end = start + win_len
    mask = np.zeros(n, dtype=bool)
    mask[start:end] = True
    return mask


def _quartile_ids(n: int) -> np.ndarray:
    if n <= 0:
        return np.zeros(0, dtype=int)
    edges = [0, int(n * 0.25), int(n * 0.50), int(n * 0.75), n]
    qid = np.zeros(n, dtype=int)
    for q in range(4):
        qid[edges[q] : edges[q + 1]] = q + 1
    return qid


@dataclass
class ModelFlags:
    pca: bool
    iforest: bool
    rf: bool
    any_model: bool


def _rate(mask: np.ndarray) -> float:
    if mask.size == 0:
        return 0.0
    return float(np.mean(mask))


def _pct(x: float) -> float:
    return float(x) * 100.0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute spike vs non-spike anomaly rates and quartile anomaly distribution."
    )
    parser.add_argument(
        "--csv",
        default=None,
        help="CSV yolu. Varsayılan: data/kuka_log600_scnd_20hz(canta)_injected.csv (varsa).",
    )
    parser.add_argument("--output-dir", default="artifacts", help="Cikis klasoru")
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument(
        "--no-producer-scale",
        action="store_true",
        help="Producer kalibrasyonunu uygulama (ham torque).",
    )
    parser.add_argument(
        "--inject-ratio",
        type=parse_ratio,
        default=0.5,
        help="Spike penceresi oranı (0-1). (Default: 0.5 => orta %50; Q2+Q3'e denk gelir)",
    )
    parser.add_argument(
        "--include-rf",
        action="store_true",
        help="RF için q/q_dot/curr parse edip skorla (CSV uygun olmalı).",
    )
    args = parser.parse_args()

    default_csv = os.path.join("data", "kuka_log600_scnd_20hz(canta)_injected.csv")
    csv_path = args.csv or (default_csv if os.path.exists(default_csv) else None)
    if not csv_path:
        raise SystemExit("CSV yolu bulunamadi. --csv ver.")

    rows = load_csv_rows(csv_path)
    headers = rows[0].keys()
    torque_cols = detect_torque_columns(headers)

    axis_act_col = _detect_axis_act_col(headers) if args.include_rf else None
    vel_cols = _detect_vel_axis_act_cols(headers) if args.include_rf else None
    curr_cols = _detect_curr_cols(headers) if args.include_rf else None

    X, row_idxs, timestamps = _build_feature_matrix_with_index(
        rows, torque_cols, limit=args.max_rows
    )

    scale = 1.0
    p99 = 0.0
    if not args.no_producer_scale:
        scale, p99 = calibrate_scale(rows, torque_cols)
        if scale and scale != 1.0:
            X = X / scale

    manager = TorqueModelManager.load()
    if not manager.enabled():
        raise RuntimeError("Model artifacts bulunamadi. Once train etmelisin.")

    rf_detector = getattr(manager, "graybox_rf", None) if args.include_rf else None
    if args.include_rf and rf_detector is None:
        print("[quartile] NOTE: --include-rf set but graybox RF artifacts not loaded.")

    inject_mask = _inject_window_mask(len(X), args.inject_ratio)
    quartile_ids = _quartile_ids(len(X))

    flags_pca = np.zeros(len(X), dtype=bool)
    flags_if = np.zeros(len(X), dtype=bool)
    flags_rf = np.zeros(len(X), dtype=bool)

    rf_parse_total = 0
    rf_parse_ok = 0

    for i, x in enumerate(X):
        q = None
        q_dot = None
        curr = None

        if args.include_rf and axis_act_col and vel_cols:
            rf_parse_total += 1
            r = rows[row_idxs[i]]
            q_raw = r.get(axis_act_col) or ""
            q = _parse_axis_act(q_raw)
            if q is not None:
                try:
                    q_dot = [
                        float(r.get(vel_cols[j], 0.0) or 0.0) * (np.pi / 180.0)
                        for j in range(6)
                    ]  # deg/s -> rad/s
                except Exception:
                    q_dot = None
            if q is not None and q_dot is not None:
                rf_parse_ok += 1

            if curr_cols:
                try:
                    curr = [float(r.get(curr_cols[j], 0.0) or 0.0) for j in range(6)]
                except Exception:
                    curr = None

        # PCA / IForest / (optional) RF via manager
        # RF is computed separately (with its own producer_scale normalization) to avoid
        # mixing scale assumptions between detectors.
        res = manager.score(x)
        flags_pca[i] = bool(res.get("pca_anomaly"))
        flags_if[i] = bool(res.get("iforest_anomaly"))

        if rf_detector is not None and q is not None and q_dot is not None:
            try:
                rf_scale = float(getattr(rf_detector, "producer_scale", 1.0) or 1.0)
                # Reconstruct raw torque (before producer calibration), then normalize
                # using the RF detector's own producer_scale (the one used during RF training).
                x_raw = np.asarray(x, dtype=float) * float(scale or 1.0)
                x_rf = (x_raw / rf_scale).tolist()
                rf_res = rf_detector.score(q, q_dot, x_rf, curr=curr)
                flags_rf[i] = bool(rf_res.get("rf_anomaly"))
            except Exception:
                flags_rf[i] = False

    flags_any = flags_pca | flags_if | flags_rf

    def split_rate(flags: np.ndarray) -> dict:
        if inject_mask.sum() == 0:
            return {"all": _rate(flags)}
        return {
            "all": _rate(flags),
            "spiked": _rate(flags[inject_mask]),
            "non_spiked": _rate(flags[~inject_mask]),
        }

    summary_rates = {
        "pca": split_rate(flags_pca),
        "iforest": split_rate(flags_if),
        "rf": split_rate(flags_rf) if args.include_rf else {"note": "rf not requested"},
        "any_model": split_rate(flags_any),
    }

    # Quartile tables
    def quartile_table(flags: np.ndarray) -> list[dict]:
        total_anoms = int(flags.sum())
        table = []
        for q in range(1, 5):
            mask_q = quartile_ids == q
            n_q = int(mask_q.sum())
            an_q = int(flags[mask_q].sum())
            sp_q = int((inject_mask & mask_q).sum())
            table.append(
                {
                    "quartile": f"Q{q}",
                    "frames": n_q,
                    "spike_frames": sp_q,
                    "anomalies": an_q,
                    "anomaly_rate": float(an_q / n_q) if n_q else 0.0,
                    "relative_share": float(an_q / total_anoms) if total_anoms else 0.0,
                }
            )
        return table

    quartiles = {
        "pca": quartile_table(flags_pca),
        "iforest": quartile_table(flags_if),
        "rf": quartile_table(flags_rf) if args.include_rf else [],
        "any_model": quartile_table(flags_any),
    }

    # Human-readable sentence metrics (requested "yuzde x / yuzde y")
    spike_metrics = {
        k: {
            "spiked_pct": _pct(v.get("spiked", 0.0)) if isinstance(v, dict) else None,
            "non_spiked_pct": _pct(v.get("non_spiked", 0.0)) if isinstance(v, dict) else None,
        }
        for k, v in summary_rates.items()
        if isinstance(v, dict) and "spiked" in v
    }

    report = {
        "generated_at": datetime.now().isoformat(),
        "csv": csv_path,
        "samples": int(len(X)),
        "feature_columns": torque_cols,
        "producer_scale": float(scale),
        "producer_scale_p99": float(p99),
        "spike_window": {
            "inject_ratio": float(args.inject_ratio),
            "start": int(np.where(inject_mask)[0][0]) if inject_mask.any() else None,
            "end": int(np.where(inject_mask)[0][-1]) if inject_mask.any() else None,
        },
        "rates": summary_rates,
        "requested_percentages": spike_metrics,
        "quartiles": quartiles,
        "rf_parse": {
            "enabled": bool(args.include_rf),
            "axis_act_col": axis_act_col,
            "vel_cols": vel_cols,
            "curr_cols": curr_cols,
            "parsed_ok": int(rf_parse_ok),
            "parsed_total": int(rf_parse_total),
        },
    }

    os.makedirs(args.output_dir, exist_ok=True)

    out_json = os.path.join(args.output_dir, "quartile_anomaly_report.json")
    out_md = os.path.join(args.output_dir, "quartile_anomaly_report.md")

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    def _md_table(title: str, table: list[dict]) -> str:
        if not table:
            return f"## {title}\n\n(no data)\n"
        lines = [
            f"## {title}",
            "",
            "| Quartile | Frames | Spike Frames | Anomalies | Anomaly Rate | Relative Share |",
            "|---:|---:|---:|---:|---:|---:|",
        ]
        for row in table:
            lines.append(
                "| {quartile} | {frames} | {spike_frames} | {anomalies} | {anomaly_rate:.2%} | {relative_share:.2%} |".format(
                    **row
                )
            )
        lines.append("")
        return "\n".join(lines)

    md_parts = [
        "# Quartile Anomaly Report",
        "",
        f"- Generated: `{report['generated_at']}`",
        f"- CSV: `{csv_path}`",
        f"- Samples used: `{len(X)}`",
        f"- Spike window (middle): ratio `{args.inject_ratio}` (start `{report['spike_window']['start']}`, end `{report['spike_window']['end']}`)",
        "",
        "## Spike vs Non-spike (requested percentages)",
        "",
    ]

    for model_key, vals in spike_metrics.items():
        md_parts.append(
            f"- `{model_key}`: spiked area anomalies in **{vals['spiked_pct']:.2f}%** of frames, non-spiked area in **{vals['non_spiked_pct']:.2f}%** of frames."
        )

    md_parts.append("")
    md_parts.append(_md_table("Any Model", quartiles["any_model"]))
    md_parts.append(_md_table("PCA", quartiles["pca"]))
    md_parts.append(_md_table("IForest", quartiles["iforest"]))
    if args.include_rf:
        md_parts.append(_md_table("Random Forest", quartiles["rf"]))

    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(md_parts))

    print("[quartile] done")
    print(f"[quartile] report: {out_json}")
    print(f"[quartile] table : {out_md}")
    print("[quartile] spike vs non-spike:")
    for model_key, vals in spike_metrics.items():
        print(
            f"  - {model_key}: spiked={vals['spiked_pct']:.2f}%  non_spiked={vals['non_spiked_pct']:.2f}%"
        )


if __name__ == "__main__":
    main()
