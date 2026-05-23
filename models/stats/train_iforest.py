import argparse
import json
import os

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

"""
Ornek:
python models/stats/train_iforest.py --csv "data/kuka_log600_scnd_20hz(canta).csv" --output-dir artifacts --quantile 0.97

ya da (daha küçük sapmalar icin):
python models/stats/train_iforest.py --csv "data/kuka_log600_scnd_20hz(canta).csv" --output-dir artifacts --preset sensitive

Normal tork verisinden Isolation Forest modeli egitir
ve skorlarin belirli yuzdeligini anomali esigi yapar.
"""


try:
    from ..feature_builder import (
        build_feature_matrix,
        calibrate_scale,
        detect_torque_columns,
        load_csv_rows,
    )
except ImportError:  # script execution fallback
    from pathlib import Path
    import sys

    models_dir = Path(__file__).resolve().parents[1]
    sys.path.append(str(models_dir))
    from feature_builder import (  # noqa: E402
        build_feature_matrix,
        calibrate_scale,
        detect_torque_columns,
        load_csv_rows,
    )


def parse_contamination(raw: str):
    if raw == "auto":
        return "auto"
    val = float(raw)
    if val <= 0 or val >= 0.5:
        raise argparse.ArgumentTypeError("contamination 0-0.5 araliginda olmali")
    return val


def main():
    parser = argparse.ArgumentParser(description="Train Isolation Forest anomaly detector")
    parser.add_argument("--csv", required=True, help="Egitim CSV yolu")
    parser.add_argument("--output-dir", default="artifacts", help="Artifact cikis klasoru")
    parser.add_argument("--quantile", type=float, default=0.99, help="Threshold icin quantile")
    parser.add_argument("--contamination", type=parse_contamination, default="auto")
    parser.add_argument("--n-estimators", type=int, default=200)
    parser.add_argument(
        "--no-producer-scale",
        action="store_true",
        help="Producer kalibrasyonunu uygulama (ham torque ile calis)",
    )
    parser.add_argument("--max-rows", type=int, default=None, help="Maksimum satir")
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument(
        "--preset",
        choices=["sensitive", "balanced"],
        default=None,
        help="Hazir ayar: sensitive (daha hassas), balanced (varsayilan davranis)",
    )
    args = parser.parse_args()

    if args.preset == "sensitive":
        args.quantile = 0.95
    elif args.preset == "balanced":
        args.quantile = 0.99

    rows = load_csv_rows(args.csv)
    torque_cols = detect_torque_columns(rows[0].keys())
    X = build_feature_matrix(rows, torque_cols, limit=args.max_rows)

    scale = 1.0
    p99 = 0.0
    if not args.no_producer_scale:
        scale, p99 = calibrate_scale(rows, torque_cols)
        if scale != 1.0:
            X = X / scale

    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

    model = IsolationForest(
        n_estimators=args.n_estimators,
        contamination=args.contamination,
        random_state=args.random_state,
        n_jobs=-1,
    )
    model.fit(Xs)

    scores = -model.score_samples(Xs)
    threshold = float(np.quantile(scores, args.quantile))

    os.makedirs(args.output_dir, exist_ok=True)

    model_path = os.path.join(args.output_dir, "iforest.pkl")
    thr_path = os.path.join(args.output_dir, "iforest_threshold.json")

    joblib.dump({"model": model, "scaler": scaler}, model_path)

    meta = {
        "score_threshold": threshold,
        "quantile": args.quantile,
        "contamination": args.contamination,
        "n_estimators": int(args.n_estimators),
        "feature_columns": torque_cols,
        "producer_scale": float(scale),
        "producer_scale_p99": float(p99),
        "training_samples": int(X.shape[0]),
        "feature_dim": int(X.shape[1]),
    }

    with open(thr_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print("[iforest] trained")
    print(f"[iforest] samples={X.shape[0]} features={X.shape[1]}")
    print(f"[iforest] threshold={threshold:.6f} (q={args.quantile})")
    print(f"[iforest] saved: {model_path}, {thr_path}")


if __name__ == "__main__":
    main()
