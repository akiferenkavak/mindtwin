"""
Export per-frame PCA (and IForest) scores to pca_results.json.

Usage:
  python models/stats/export_pca_results.py \
    --csv "data/kuka_log600_scnd_20hz(canta).csv" \
    --output-dir artifacts

The resulting pca_results.json has the shape:
{
  "meta": {
    "generated_at": "...",
    "threshold": <float>,
    "iforest_threshold": <float>,
    "n_components": <int>,
    "explained_variance_ratio": [...],
    "feature_columns": [...],
    "training_samples": <int>
  },
  "overall": [
    {
      "frame_no": 0,
      "timestamp": "...",
      "pca_score": <float>,
      "pca_threshold": <float>,
      "pca_anomaly": true/false,
      "severity_ratio": <float>,
      "threshold_distance": <float>,
      "iforest_score": <float>,
      "iforest_threshold": <float>,
      "iforest_anomaly": true/false,
      "model_anomaly": true/false,
      "worst_feature": "TORQUE_A3",
      "explanation": "..."
    },
    ...
  ],
  "per_feature": {
    "TORQUE_A1": [
      {
        "frame_no": 0,
        "timestamp": "...",
        "pca_score": <float>,
        "pca_threshold": <float>,
        "is_anomaly": true/false,
        "feature_error": <float>,
        "severity_ratio": <float>,
        "direction": "high" | "low",
        "explanation": "..."
      },
      ...
    ],
    ...
  }
}
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np

# ---------------------------------------------------------------------------
# Path bootstrap so script can be run from repo root or directly
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve()
_MODELS_DIR = _HERE.parents[1]   # mindtwin/models/
_ROOT_DIR   = _HERE.parents[2]   # mindtwin/

for _p in [str(_MODELS_DIR), str(_ROOT_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from feature_builder import (
        build_feature_matrix,
        calibrate_scale,
        detect_torque_columns,
        load_csv_rows,
        to_float,
    )
    from model_manager import TorqueModelManager
except ImportError:
    from models.feature_builder import (  # noqa: E402
        build_feature_matrix,
        calibrate_scale,
        detect_torque_columns,
        load_csv_rows,
        to_float,
    )
    from models.model_manager import TorqueModelManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SEVERITY_LABELS = {
    "CRITICAL": (3.0, float("inf")),
    "HIGH":     (1.5, 3.0),
    "MEDIUM":   (1.0, 1.5),
    "LOW":      (0.0, 1.0),
}


def severity_label(ratio: float) -> str:
    if ratio >= 3.0:
        return "CRITICAL"
    if ratio >= 1.5:
        return "HIGH"
    if ratio >= 1.0:
        return "MEDIUM"
    return "LOW"


def build_explanation(score: float, threshold: float, worst_feat: str, ratio: float) -> str:
    if ratio < 1.0:
        return "Normal operation"
    sev = severity_label(ratio)
    return (
        f"{sev}: Reconstruction error {score:.4f} exceeds threshold {threshold:.4f} "
        f"({ratio:.2f}×). Highest deviation in {worst_feat}."
    )


def build_feature_explanation(feat: str, feat_err: float, mean_err: float,
                               threshold: float, direction: str) -> str:
    ratio = feat_err / threshold if threshold > 0 else 0.0
    if ratio < 1.0:
        return f"{feat}: within normal range (err={feat_err:.4f})"
    dir_str = "above normal" if direction == "high" else "below normal"
    sev = severity_label(ratio)
    return (
        f"{sev}: {feat} reconstruction error {feat_err:.4f} is {dir_str}. "
        f"Severity ratio {ratio:.2f}×."
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Export PCA per-frame results to JSON")
    parser.add_argument("--csv", required=True, help="CSV path")
    parser.add_argument("--output-dir", default="artifacts", help="Output directory")
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument(
        "--no-producer-scale",
        action="store_true",
        help="Skip producer calibration",
    )
    args = parser.parse_args()

    # ── Load data ──────────────────────────────────────────────────────────
    rows = load_csv_rows(args.csv)
    torque_cols = detect_torque_columns(rows[0].keys())

    limit = args.max_rows
    X_list, timestamps, row_idxs = [], [], []
    take = len(rows) if limit is None else min(len(rows), limit)
    for i in range(take):
        r = rows[i]
        vals = [to_float(r.get(k)) for k in torque_cols]
        if any(v is None for v in vals) or any(not np.isfinite(v) for v in vals):
            continue
        X_list.append([float(v) for v in vals])
        timestamps.append(r.get("timestamp", ""))
        row_idxs.append(i)

    X = np.asarray(X_list, dtype=float)

    scale = 1.0
    if not args.no_producer_scale:
        scale, _ = calibrate_scale(rows, torque_cols)
        if scale != 1.0:
            X = X / scale

    # ── Load model manager ─────────────────────────────────────────────────
    manager = TorqueModelManager.load(artifacts_dir=args.output_dir)
    if not manager.enabled():
        print("[export_pca] ERROR: No model artifacts found. Train models first.")
        sys.exit(1)

    pca_det   = manager.pca
    if_det    = manager.iforest

    if pca_det is None:
        print("[export_pca] ERROR: PCA model not found.")
        sys.exit(1)

    pca_thr    = float(pca_det.threshold)
    if_thr     = float(if_det.threshold) if if_det is not None else None

    # Per-feature standard deviations (for direction inference)
    # Work in scaled space
    Xs = pca_det.scaler.transform(X)
    Xp = pca_det.model.transform(Xs)
    Xr = pca_det.model.inverse_transform(Xp)
    X_rec_orig = pca_det.scaler.inverse_transform(Xr)

    per_sample_errors = np.mean((Xs - Xr) ** 2, axis=1)   # MSE in scaled space (matches score())
    per_feat_errors   = (Xs - Xr) ** 2                    # shape (N, F)

    # feature-level means for direction estimation
    feat_means = np.mean(X, axis=0)

    # Pre-compute Isolation Forest scores in a single vectorized call to prevent loop overhead (spawning threads 3000+ times)
    if_scores = None
    if if_det is not None:
        try:
            X_if = if_det.scaler.transform(X) if if_det.scaler is not None else X
            if_scores = -if_det.model.score_samples(X_if)
            print(f"[export_pca] Vectorized IForest scoring complete for {len(X)} samples.")
        except Exception as e:
            print(f"[export_pca] Warning: Vectorized IForest scoring failed ({e}), falling back to loop.", file=sys.stderr)

    # ── Build overall list ─────────────────────────────────────────────────
    overall = []
    for idx in range(len(X_list)):
        score      = float(per_sample_errors[idx])
        ratio      = score / pca_thr if pca_thr > 0 else 0.0
        dist       = score - pca_thr
        is_pca     = score >= pca_thr

        # Worst feature
        feat_errs  = per_feat_errors[idx]
        worst_feat_idx = int(np.argmax(feat_errs))
        worst_feat = torque_cols[worst_feat_idx]

        # IForest
        if_score   = None
        if_anomaly = False
        if if_scores is not None:
            if_score   = float(if_scores[idx])
            if_anomaly = if_score >= if_thr
        elif if_det is not None:
            # Fallback slow loop
            if_score   = float(if_det.score(X_list[idx]))
            if_anomaly = if_score >= if_thr

        model_anomaly = is_pca or if_anomaly

        overall.append({
            "frame_no":          idx,
            "timestamp":         timestamps[idx],
            "pca_score":         round(score, 6),
            "pca_threshold":     round(pca_thr, 6),
            "is_anomaly":        is_pca,
            "pca_anomaly":       is_pca,
            "severity_ratio":    round(ratio, 4),
            "threshold_distance": round(dist, 6),
            "iforest_score":     round(if_score, 6) if if_score is not None else None,
            "iforest_threshold": round(if_thr, 6)   if if_thr   is not None else None,
            "iforest_anomaly":   if_anomaly,
            "model_anomaly":     model_anomaly,
            "worst_feature":     worst_feat,
            "explanation":       build_explanation(score, pca_thr, worst_feat, ratio),
        })

    # ── Build per-feature list ─────────────────────────────────────────────
    per_feature = {}
    for fi, feat in enumerate(torque_cols):
        feat_list = []
        # Per-feature threshold: use overall PCA threshold as proxy
        # (reconstruct error for single feature)
        feat_errs_col = per_feat_errors[:, fi]   # (N,)
        feat_thr = float(np.quantile(feat_errs_col, 0.985))   # 98.5th percentile
        if feat_thr <= 0:
            feat_thr = pca_thr / len(torque_cols)

        for idx in range(len(X_list)):
            ferr   = float(feat_errs_col[idx])
            ratio  = ferr / feat_thr if feat_thr > 0 else 0.0
            is_an  = ferr >= feat_thr

            # Direction: original value vs reconstructed
            orig_val = X_list[idx][fi]
            recon_val = float(X_rec_orig[idx, fi])
            direction = "high" if orig_val > recon_val else "low"

            feat_list.append({
                "frame_no":      idx,
                "timestamp":     timestamps[idx],
                "feature":       feat,
                "pca_score":     round(per_sample_errors[idx], 6),
                "feature_error": round(ferr, 6),
                "pca_threshold": round(pca_thr, 6),
                "feat_threshold": round(feat_thr, 6),
                "is_anomaly":    is_an,
                "severity_ratio": round(ratio, 4),
                "direction":     direction if is_an else None,
                "explanation":   build_feature_explanation(
                    feat, ferr, float(feat_errs_col.mean()), feat_thr, direction
                ) if is_an else "Normal operation",
            })

        per_feature[feat] = feat_list

    # ── Meta ───────────────────────────────────────────────────────────────
    evr = [float(v) for v in pca_det.model.explained_variance_ratio_]
    meta = {
        "generated_at":           datetime.now().isoformat(),
        "threshold":              round(pca_thr, 6),
        "iforest_threshold":      round(if_thr, 6) if if_thr is not None else None,
        "n_components":           int(pca_det.model.n_components_),
        "explained_variance_ratio": evr,
        "total_variance_explained": round(sum(evr), 4),
        "feature_columns":        torque_cols,
        "training_samples":       int(pca_det.meta.get("training_samples", 0)),
        "quantile":               float(pca_det.meta.get("quantile", 0.985)),
        "feature_dim":            len(torque_cols),
        "samples_exported":       len(overall),
        "anomaly_count":          int(sum(1 for r in overall if r["pca_anomaly"])),
    }

    result = {
        "meta":        meta,
        "overall":     overall,
        "per_feature": per_feature,
    }

    os.makedirs(args.output_dir, exist_ok=True)
    out_path = os.path.join(args.output_dir, "pca_results.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False)

    n_anom = meta["anomaly_count"]
    print(f"[export_pca] done -> {out_path}")
    print(f"[export_pca] samples={meta['samples_exported']}  anomalies={n_anom}")
    print(f"[export_pca] threshold={pca_thr:.6f}  n_components={meta['n_components']}")


if __name__ == "__main__":
    main()
