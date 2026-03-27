import argparse
import json
import os

import joblib
import numpy as np
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

"""
Ornek:
python models/stats/train_pca.py --csv "data/kuka_log600_scnd_20hz(canta).csv" --output-dir artifacts --quantile 0.99 --n-components 3

ya da (daha kucuk sapmalar icin):
python models/stats/train_pca.py --csv "data/kuka_log600_scnd_20hz(canta).csv" --output-dir artifacts --preset sensitive

Normal tork verisinden PCA tabanli anomali modeli egitir,
rekonstruksiyon hatasina gore esik uretir.
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


def parse_n_components(raw: str):
    val = float(raw)
    if val <= 0:
        raise argparse.ArgumentTypeError("n_components > 0 olmali")
    if val <= 1:
        return val
    if not val.is_integer():
        raise argparse.ArgumentTypeError("n_components integer ya da (0,1] araliginda olmalidir")
    return int(val)


def main():
    parser = argparse.ArgumentParser(description="Train PCA anomaly detector")
    parser.add_argument("--csv", required=True, help="Egitim CSV yolu")
    parser.add_argument("--output-dir", default="artifacts", help="Artifact cikis klasoru")
    parser.add_argument("--quantile", type=float, default=0.99, help="Threshold icin quantile")
    parser.add_argument(
        "--n-components",
        type=parse_n_components,
        default="0.95",
        help="PCA n_components (int) ya da varyans orani (0,1]",
    )
    parser.add_argument(
        "--no-producer-scale",
        action="store_true",
        help="Producer kalibrasyonunu uygulama (ham torque ile calis)",
    )
    parser.add_argument("--max-rows", type=int, default=None, help="Maksimum satir")
    parser.add_argument(
        "--preset",
        choices=["sensitive", "balanced"],
        default=None,
        help="Hazir ayar: sensitive (daha hassas), balanced (varsayilan davranis)",
    )
    args = parser.parse_args()

    if args.preset == "sensitive":
        args.n_components = 2
        args.quantile = 0.97
    elif args.preset == "balanced":
        args.n_components = 3
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

    pca = PCA(n_components=args.n_components, svd_solver="full")
    Xp = pca.fit_transform(Xs)
    Xr = pca.inverse_transform(Xp)
    errors = np.mean((Xs - Xr) ** 2, axis=1)

    threshold = float(np.quantile(errors, args.quantile))

    os.makedirs(args.output_dir, exist_ok=True)

    model_path = os.path.join(args.output_dir, "pca_model.pkl")
    scaler_path = os.path.join(args.output_dir, "pca_scaler.pkl")
    thresholds_path = os.path.join(args.output_dir, "pca_thresholds.json")

    joblib.dump(pca, model_path)
    joblib.dump(scaler, scaler_path)

    meta = {
        "recon_error_threshold": threshold,
        "quantile": args.quantile,
        "n_components": int(pca.n_components_),
        "explained_variance_ratio": [float(v) for v in pca.explained_variance_ratio_],
        "feature_columns": torque_cols,
        "producer_scale": float(scale),
        "producer_scale_p99": float(p99),
        "training_samples": int(X.shape[0]),
        "feature_dim": int(X.shape[1]),
    }

    with open(thresholds_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print("[pca] trained")
    print(f"[pca] samples={X.shape[0]} features={X.shape[1]}")
    print(f"[pca] threshold={threshold:.6f} (q={args.quantile})")
    print(f"[pca] saved: {model_path}, {scaler_path}, {thresholds_path}")


if __name__ == "__main__":
    main()
