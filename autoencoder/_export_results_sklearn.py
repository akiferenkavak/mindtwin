"""
_export_results_sklearn.py
==========================
TensorFlow-free replacement for export_results.py.

Replicates the Keras 16-8-16 autoencoder using sklearn's MLPRegressor so the
pipeline can run on machines without TensorFlow. The feature engineering,
threshold logic (99th percentile of train reconstruction / max-feature error)
and output JSON schema are identical to the original pipeline, so the frontend
consumes the result unchanged.

Train  : clean 900s mix (via step2_prepare_features.prepare_features)
Detect : injected 900s CSV (via step4_single_feature_anomaly_detection.load_test_data)
Output : artifacts/autoencoder_results.json (+ multiple/single individual files)

Usage:  python autoencoder/_export_results_sklearn.py
"""

import os
import sys
import json
from pathlib import Path

import numpy as np

BASE_DIR = Path(__file__).resolve().parent.parent      # mindtwin/
AE_DIR = Path(__file__).resolve().parent               # mindtwin/autoencoder/
ARTIFACTS_DIR = BASE_DIR / "artifacts"
ARTIFACTS_DIR.mkdir(exist_ok=True)

if str(AE_DIR) not in sys.path:
    sys.path.insert(0, str(AE_DIR))

# TF-free imports only (step2 / step4_single import no tensorflow)
from step2_prepare_features import prepare_features
from step4_single_feature_anomaly_detection import (
    load_test_data,
    prepare_test_features,
    feature_to_text,
)

from sklearn.neural_network import MLPRegressor

TIMESTAMP_COL = "timestamp"


def _fmt_ts(ts):
    try:
        return ts.strftime("%H:%M:%S.%f")[:-3]
    except Exception:
        return str(ts)


def train_autoencoder():
    """Train an MLP autoencoder (16-8-16) on the clean 900s mix."""
    print("[train] preparing features…")
    df_all, X, X_scaled, scaler, feat_cols = prepare_features()

    print(f"[train] training MLPRegressor on shape {X_scaled.shape}")
    model = MLPRegressor(
        hidden_layer_sizes=(16, 8, 16),
        activation="relu",
        solver="adam",
        batch_size=16,
        max_iter=400,
        shuffle=False,
        random_state=42,
        early_stopping=False,
    )
    # Autoencoder: reconstruct the (scaled) input
    model.fit(X_scaled, X_scaled)

    train_pred = model.predict(X_scaled)
    train_error_matrix = (X_scaled - train_pred) ** 2

    train_reconstruction_error = np.mean(train_error_matrix, axis=1)
    train_max_feature_error = np.max(train_error_matrix, axis=1)

    fixed_threshold = float(np.percentile(train_reconstruction_error, 99))
    single_feature_threshold = float(np.percentile(train_max_feature_error, 99))

    print(f"[train] fixed_threshold           = {fixed_threshold:.6f}")
    print(f"[train] single_feature_threshold  = {single_feature_threshold:.6f}")

    return model, scaler, feat_cols, fixed_threshold, single_feature_threshold


def score_test(model, scaler, feat_cols):
    """Score the injected 900s test CSV; return df + error matrices."""
    df_all = load_test_data()
    df_all, X, X_scaled = prepare_test_features(df_all, feat_cols, scaler)

    X_pred = model.predict(X_scaled)
    error_matrix = (X_scaled - X_pred) ** 2
    signed_diff = X_scaled - X_pred

    reconstruction_error = np.mean(error_matrix, axis=1)
    max_feature_error = np.max(error_matrix, axis=1)
    top_feature_idx = np.argmax(error_matrix, axis=1)
    top_feature_names = [feat_cols[i] for i in top_feature_idx]
    top_feature_signed_diff = signed_diff[np.arange(len(signed_diff)), top_feature_idx]

    return (
        df_all,
        reconstruction_error,
        max_feature_error,
        top_feature_names,
        top_feature_signed_diff,
    )


def build_multiple_json(df_all, reconstruction_error, top_feature_names,
                        top_feature_signed_diff, threshold):
    rows = []
    for i in range(len(df_all)):
        err = float(reconstruction_error[i])
        thr = float(threshold)
        is_anom = err > thr
        root = top_feature_names[i]
        signed = float(top_feature_signed_diff[i])
        if is_anom:
            direction = "high" if signed > 0 else "low"
            cause = feature_to_text(root, direction)
        else:
            cause = "Normal operation"
        rows.append({
            "timestamp":          _fmt_ts(df_all[TIMESTAMP_COL].iloc[i]),
            "error":              round(err, 6),
            "threshold":          round(thr, 6),
            "threshold_distance": round(err - thr, 6),
            "severity_ratio":     round(err / thr, 3) if thr else 0,
            "is_anomaly":         is_anom,
            "root_cause":         root,
            "explanation":        cause,
        })
    return rows


def build_single_json(df_all, max_feature_error, top_feature_names,
                     top_feature_signed_diff, feat_cols, threshold):
    per_feature = {f: [] for f in feat_cols}
    for i in range(len(df_all)):
        feat = top_feature_names[i]
        if feat not in per_feature:
            continue
        err = float(max_feature_error[i])
        thr = float(threshold)
        is_a = err > thr
        signed = float(top_feature_signed_diff[i])
        direction = "high" if signed > 0 else "low"
        cause = feature_to_text(feat, direction) if is_a else "Normal"
        per_feature[feat].append({
            "timestamp":          _fmt_ts(df_all[TIMESTAMP_COL].iloc[i]),
            "error":              round(err, 6),
            "threshold":          round(thr, 6),
            "threshold_distance": round(err - thr, 6),
            "severity_ratio":     round(err / thr, 3) if thr else 0,
            "is_anomaly":         is_a,
            "feature":            feat,
            "direction":          direction,
            "explanation":        cause,
        })
    return per_feature


def main():
    print("\n" + "=" * 60)
    print("AUTOENCODER EXPORT PIPELINE (sklearn / TF-free)")
    print("=" * 60)

    model, scaler, feat_cols, fixed_threshold, single_feature_threshold = train_autoencoder()

    (df_all, reconstruction_error, max_feature_error,
     top_feature_names, top_feature_signed_diff) = score_test(model, scaler, feat_cols)

    multiple_json = build_multiple_json(
        df_all, reconstruction_error, top_feature_names,
        top_feature_signed_diff, fixed_threshold
    )
    total_multiple = sum(1 for r in multiple_json if r["is_anomaly"])

    single_json = build_single_json(
        df_all, max_feature_error, top_feature_names,
        top_feature_signed_diff, feat_cols, single_feature_threshold
    )
    total_single = sum(1 for rows in single_json.values() for r in rows if r["is_anomaly"])

    m_path = ARTIFACTS_DIR / "multiple_anomaly_results.json"
    s_path = ARTIFACTS_DIR / "single_anomaly_results.json"
    a_path = ARTIFACTS_DIR / "autoencoder_results.json"

    with open(m_path, "w", encoding="utf-8") as f:
        json.dump(multiple_json, f, ensure_ascii=False, indent=2)
    with open(s_path, "w", encoding="utf-8") as f:
        json.dump(single_json, f, ensure_ascii=False, indent=2)

    merged = {
        "meta": {
            "generated_at": str(np.datetime64("now")),
            "multiple_threshold":       round(float(fixed_threshold), 6),
            "single_threshold":         round(float(single_feature_threshold), 6),
            "total_multiple_anomalies": int(total_multiple),
            "total_single_anomalies":   int(total_single),
            "features": feat_cols,
        },
        "multiple": multiple_json,
        "single":   single_json,
    }
    with open(a_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 60)
    print("EXPORT COMPLETE")
    print(f"  Rows                : {len(multiple_json)}")
    print(f"  Multiple anomalies  : {total_multiple}")
    print(f"  Single anomalies    : {total_single}")
    print(f"  Window start / end  : {_fmt_ts(df_all[TIMESTAMP_COL].iloc[0])} -> {_fmt_ts(df_all[TIMESTAMP_COL].iloc[-1])}")
    print(f"  Output              : {a_path}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
