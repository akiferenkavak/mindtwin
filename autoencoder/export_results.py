"""
export_results.py
=================
Orchestrator: train autoencoder → run both detection modes → write JSON for frontend.

Usage (from mindtwin/ root):
    python autoencoder/export_results.py

Outputs:
    artifacts/multiple_anomaly_results.json
    artifacts/single_anomaly_results.json
    artifacts/autoencoder_results.json  (merged, consumed by frontend)

All existing terminal prints in step3/step4 are preserved.
"""

import os
import sys
import json
from pathlib import Path

# ── path setup ─────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent   # mindtwin/
AE_DIR   = Path(__file__).resolve().parent           # mindtwin/autoencoder/
ARTIFACTS_DIR = BASE_DIR / "artifacts"
ARTIFACTS_DIR.mkdir(exist_ok=True)

# Add autoencoder/ to sys.path so step*.py imports work
if str(AE_DIR) not in sys.path:
    sys.path.insert(0, str(AE_DIR))

# ── imports ─────────────────────────────────────────────────────────────────
from step3_autoencoder import train_autoencoder
from step4_anomoly_detection import run_anomaly_detection as run_multiple
from step4_single_feature_anomaly_detection import (
    run_anomaly_detection as run_single,
    load_test_data,
    prepare_test_features,
    feature_to_text,
)

import numpy as np


# ── helpers ─────────────────────────────────────────────────────────────────
TIMESTAMP_COL = "timestamp"


def _fmt_ts(ts):
    """Format a pandas Timestamp to HH:MM:SS.mmm"""
    try:
        return ts.strftime("%H:%M:%S.%f")[:-3]
    except Exception:
        return str(ts)


def build_multiple_json(result_df, anomaly_idx, threshold):
    """
    Build the 'multiple' list for the frontend JSON.
    All rows are included (both normal and anomaly) so the frontend
    can draw the full continuous error line.
    """
    rows = []
    for i in range(len(result_df)):
        rec = result_df.iloc[i]
        ts = _fmt_ts(rec[TIMESTAMP_COL])
        err = float(rec.get("reconstruction_error", 0) or 0)
        thr = float(threshold)
        is_anom = bool(rec.get("is_anomaly", False))
        root    = str(rec.get("top_error_feature", "") or "")
        cause   = str(rec.get("root_cause_text", "") or "")

        rows.append({
            "timestamp":          ts,
            "error":              round(err, 6),
            "threshold":          round(thr, 6),
            "threshold_distance": round(err - thr, 6),
            "severity_ratio":     round(err / thr, 3) if thr else 0,
            "is_anomaly":         is_anom,
            "root_cause":         root,
            "explanation":        cause if cause else ("Normal operation" if not is_anom else f"{root} anomaly"),
        })
    return rows


def build_single_json(result_df, anomaly_idx, feat_cols, threshold):
    """
    Build the 'single' dict: { feature_name: [rows...] }
    Each feature only contains rows where that feature was the top-error feature.
    """
    per_feature = {f: [] for f in feat_cols}

    for i in range(len(result_df)):
        rec = result_df.iloc[i]
        feat = str(rec.get("top_error_feature", "") or "")
        if feat not in per_feature:
            continue

        ts   = _fmt_ts(rec[TIMESTAMP_COL])
        err  = float(rec.get("max_feature_error", 0) or 0)
        thr  = float(threshold)
        is_a = bool(rec.get("is_anomaly", False))
        cause = str(rec.get("root_cause_text", "") or "")
        signed = float(rec.get("top_feature_signed_diff", 0) or 0)

        # For single view we include ALL rows that had this feature as max-error,
        # not just anomalies — so the chart shows a continuous line.
        per_feature[feat].append({
            "timestamp":          ts,
            "error":              round(err, 6),
            "threshold":          round(thr, 6),
            "threshold_distance": round(err - thr, 6),
            "severity_ratio":     round(err / thr, 3) if thr else 0,
            "is_anomaly":         is_a,
            "feature":            feat,
            "direction":          "high" if signed > 0 else "low",
            "explanation":        cause if cause else ("Normal" if not is_a else f"{feat} anomaly"),
        })

    return per_feature


# ── main ────────────────────────────────────────────────────────────────────
def main():
    print("\n" + "="*60)
    print("AUTOENCODER EXPORT PIPELINE")
    print("="*60)

    # 1) Train
    print("\n[1/3] Training autoencoder…")
    model, scaler, feat_cols, fixed_threshold, single_feature_threshold = train_autoencoder(
        batch_size=16, epochs=40
    )
    print(f"      Fixed threshold  : {fixed_threshold:.6f}")
    print(f"      Single threshold : {single_feature_threshold:.6f}")
    print(f"      Features ({len(feat_cols)}): {feat_cols[:4]} …")

    # 2) Multiple-feature detection
    print("\n[2/3] Running multiple-feature anomaly detection…")
    multiple_result_df, multiple_anomaly_idx = run_multiple(
        model, scaler, feat_cols, fixed_threshold
    )
    multiple_json = build_multiple_json(
        multiple_result_df, multiple_anomaly_idx, fixed_threshold
    )
    print(f"      Rows: {len(multiple_json)}  |  Anomalies: {len(multiple_anomaly_idx)}")

    # 3) Single-feature detection
    print("\n[3/3] Running single-feature anomaly detection…")
    single_result_df, single_anomaly_idx = run_single(
        model, scaler, feat_cols, single_feature_threshold
    )
    single_json = build_single_json(
        single_result_df, single_anomaly_idx, feat_cols, single_feature_threshold
    )
    total_single = sum(1 for f in single_json.values() for r in f if r["is_anomaly"])
    print(f"      Features with rows: {sum(1 for f in single_json.values() if f)}")
    print(f"      Total single anomalies: {total_single}")

    # ── Write JSON files ────────────────────────────────────────────────────

    # Individual files
    m_path = ARTIFACTS_DIR / "multiple_anomaly_results.json"
    s_path = ARTIFACTS_DIR / "single_anomaly_results.json"
    a_path = ARTIFACTS_DIR / "autoencoder_results.json"

    with open(m_path, "w", encoding="utf-8") as f:
        json.dump(multiple_json, f, ensure_ascii=False, indent=2)
    print(f"\n✔  Written: {m_path}")

    with open(s_path, "w", encoding="utf-8") as f:
        json.dump(single_json, f, ensure_ascii=False, indent=2)
    print(f"✔  Written: {s_path}")

    # Merged (consumed by frontend)
    merged = {
        "meta": {
            "generated_at": str(np.datetime64("now")),
            "multiple_threshold":   round(float(fixed_threshold), 6),
            "single_threshold":     round(float(single_feature_threshold), 6),
            "total_multiple_anomalies": int(len(multiple_anomaly_idx)),
            "total_single_anomalies":   int(total_single),
            "features": feat_cols,
        },
        "multiple": multiple_json,
        "single":   single_json,
    }

    with open(a_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    print(f"✔  Written: {a_path}")

    print("\n" + "="*60)
    print("EXPORT COMPLETE")
    print(f"  Multiple anomalies : {len(multiple_anomaly_idx)}")
    print(f"  Single anomalies   : {total_single}")
    print(f"  Output dir         : {ARTIFACTS_DIR}")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
