import argparse
import csv
import json
import os
from datetime import datetime

import numpy as np

"""
Ornek:
python models/eval_models.py --csv "data/kuka_log600_scnd_20hz(canta).csv" \\
  --inject 0.5 --inject-method spike --spike-factor 0.3 \\
  --plot-name eval_plot_lower_spike.png

ya da

python models/eval_models.py --csv "data/kuka_log600_scnd_20hz(canta).csv" \\
  --inject 0.5 --inject-method mix --spike-factor 0.3 --noise-std 0.2 \\
  --plot-name eval_plot_mix_low.png

CSV'nin orta %50'sine sentetik bozulma ekleyip (spike+noise+swap)
modellerin bu bolgeyi ne kadar anomali saydigini raporlar ve grafik uretir.
"""

try:
    from .feature_builder import (
        build_feature_matrix,
        calibrate_scale,
        detect_torque_columns,
        load_csv_rows,
        to_float,
    )
    from .model_manager import TorqueModelManager
except ImportError:  # script execution fallback
    from pathlib import Path
    import sys

    root = Path(__file__).resolve().parents[1]
    sys.path.append(str(root))
    from feature_builder import (  # noqa: E402
        build_feature_matrix,
        calibrate_scale,
        detect_torque_columns,
        load_csv_rows,
        to_float,
    )
    from model_manager import TorqueModelManager  # noqa: E402


def parse_ratio(val: str) -> float:
    v = float(val)
    if v < 0 or v > 1:
        raise argparse.ArgumentTypeError("ratio 0-1 araliginda olmali")
    return v


def build_feature_matrix_with_index(rows, torque_cols, limit=None):
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
        ts = r.get("timestamp") or ""
        timestamps.append(ts)

    if not feats:
        raise RuntimeError("Feature matrisi bos. Torque kolonlarini kontrol et.")

    return np.asarray(feats, dtype=float), idxs, timestamps


def inject_anomalies(
    X: np.ndarray,
    inject_ratio: float,
    method: str,
    spike_factor: float,
    noise_std: float,
    seed: int,
):
    if inject_ratio <= 0:
        return X, np.zeros(len(X), dtype=bool), None

    n = len(X)
    win_len = max(1, int(n * inject_ratio))
    start = (n - win_len) // 2
    end = start + win_len

    mask = np.zeros(n, dtype=bool)
    mask[start:end] = True

    rng = np.random.default_rng(seed)
    X_inj = X.copy()

    feat_std = np.std(X, axis=0)
    feat_std = np.where(feat_std == 0, 1.0, feat_std)

    for i in range(start, end):
        x = X_inj[i].copy()
        dim = x.shape[0]
        j = i % dim
        k = (j + 1) % dim

        if method in ("swap", "mix"):
            x[j], x[k] = x[k], x[j]

        if method in ("spike", "mix"):
            x[j] = x[j] * (1.0 + spike_factor)

        if method in ("noise", "mix"):
            noise = rng.normal(0.0, noise_std, size=dim) * feat_std
            x = x + noise

        X_inj[i] = x

    info = {
        "inject_ratio": inject_ratio,
        "inject_start": start,
        "inject_end": end,
        "method": method,
        "spike_factor": spike_factor,
        "noise_std": noise_std,
        "seed": seed,
    }

    return X_inj, mask, info


def compute_quantiles(arr, qs):
    if len(arr) == 0:
        return {str(q): None for q in qs}
    return {str(q): float(np.quantile(arr, q)) for q in qs}


def main():
    parser = argparse.ArgumentParser(description="Evaluate PCA + IForest on CSV")
    parser.add_argument("--csv", required=True, help="Test CSV yolu")
    parser.add_argument("--output-dir", default="artifacts", help="Cikis klasoru")
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument(
        "--no-producer-scale",
        action="store_true",
        help="Producer kalibrasyonunu uygulama (ham torque)",
    )
    parser.add_argument(
        "--inject",
        type=parse_ratio,
        default=0.0,
        help="Sentetik anomaly oranı (0-1). Orta pencereye uygular.",
    )
    parser.add_argument(
        "--inject-method",
        choices=["spike", "swap", "noise", "mix"],
        default="mix",
    )
    parser.add_argument("--spike-factor", type=float, default=0.6)
    parser.add_argument("--noise-std", type=float, default=0.35)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--plot-name",
        default="eval_plot.png",
        help="Plot dosya adi (output-dir altina kaydedilir)",
    )
    parser.add_argument(
        "--plot-normalized",
        action="store_true",
        help="Skorlari threshold'a bolerek normalize plot uret",
    )
    args = parser.parse_args()

    rows = load_csv_rows(args.csv)
    torque_cols = detect_torque_columns(rows[0].keys())

    X, row_idxs, timestamps = build_feature_matrix_with_index(
        rows, torque_cols, limit=args.max_rows
    )

    scale = 1.0
    p99 = 0.0
    if not args.no_producer_scale:
        scale, p99 = calibrate_scale(rows, torque_cols)
        if scale != 1.0:
            X = X / scale

    X_eval, inject_mask, inject_info = inject_anomalies(
        X,
        inject_ratio=args.inject,
        method=args.inject_method,
        spike_factor=args.spike_factor,
        noise_std=args.noise_std,
        seed=args.seed,
    )

    manager = TorqueModelManager.load()
    if not manager.enabled():
        raise RuntimeError("Model artifacts bulunamadi. Once train etmelisin.")

    pca_scores = []
    if_scores = []
    pca_flags = []
    if_flags = []
    model_flags = []

    for x in X_eval:
        res = manager.score(x)
        pca_scores.append(res.get("pca_score"))
        if_scores.append(res.get("iforest_score"))
        pca_flags.append(bool(res.get("pca_anomaly")))
        if_flags.append(bool(res.get("iforest_anomaly")))
        model_flags.append(bool(res.get("model_anomaly")))

    pca_scores_arr = np.array([s for s in pca_scores if s is not None], dtype=float)
    if_scores_arr = np.array([s for s in if_scores if s is not None], dtype=float)

    def rate(mask):
        if len(mask) == 0:
            return 0.0
        return float(np.mean(mask))

    def split_rate(flags):
        if inject_mask is None or inject_mask.sum() == 0:
            return {"all": rate(flags)}
        return {
            "all": rate(flags),
            "injected": rate(np.array(flags)[inject_mask]),
            "normal": rate(np.array(flags)[~inject_mask]),
        }

    summary = {
        "samples": int(len(X_eval)),
        "pca_anomaly_rate": split_rate(pca_flags),
        "iforest_anomaly_rate": split_rate(if_flags),
        "model_anomaly_rate": split_rate(model_flags),
        "pca_score_quantiles": compute_quantiles(pca_scores_arr, [0.5, 0.95, 0.99]),
        "iforest_score_quantiles": compute_quantiles(if_scores_arr, [0.5, 0.95, 0.99]),
    }

    top_k = 50
    rank_scores = np.zeros(len(X_eval), dtype=float)
    if manager.pca is not None:
        rank_scores += np.array([s if s is not None else 0.0 for s in pca_scores])
    if manager.iforest is not None:
        rank_scores += np.array([s if s is not None else 0.0 for s in if_scores])

    top_idx = np.argsort(rank_scores)[::-1][:top_k]

    os.makedirs(args.output_dir, exist_ok=True)

    report_path = os.path.join(args.output_dir, "eval_report.json")
    top_path = os.path.join(args.output_dir, "eval_top_anomalies.csv")
    plot_path = os.path.join(args.output_dir, args.plot_name)

    report = {
        "generated_at": datetime.now().isoformat(),
        "csv": args.csv,
        "feature_columns": torque_cols,
        "producer_scale": float(scale),
        "producer_scale_p99": float(p99),
        "inject": inject_info,
        "summary": summary,
        "model_meta": {
            "pca": manager.pca.meta if manager.pca is not None else None,
            "iforest": manager.iforest.meta if manager.iforest is not None else None,
        },
    }

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    with open(top_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "rank",
                "row_index",
                "timestamp",
                "pca_score",
                "iforest_score",
                "pca_anomaly",
                "iforest_anomaly",
                "model_anomaly",
                "injected",
            ]
        )
        for rank, idx in enumerate(top_idx, start=1):
            writer.writerow(
                [
                    rank,
                    row_idxs[idx],
                    timestamps[idx],
                    pca_scores[idx],
                    if_scores[idx],
                    int(pca_flags[idx]),
                    int(if_flags[idx]),
                    int(model_flags[idx]),
                    int(inject_mask[idx]) if inject_mask is not None else 0,
                ]
            )

    try:
        import matplotlib.pyplot as plt

        xs = np.arange(len(X_eval))
        fig, ax1 = plt.subplots(figsize=(12, 6))

        ax2 = ax1.twinx() if (manager.pca is not None and manager.iforest is not None) else None

        lines = []
        labels = []

        if manager.pca is not None:
            pca_vals = pca_scores
            pca_thr = manager.pca.threshold
            if args.plot_normalized and pca_thr:
                pca_vals = [v / pca_thr if v is not None else None for v in pca_scores]
                pca_thr = 1.0
            ln1 = ax1.plot(xs, pca_vals, label="PCA score", color="#4da6ff")
            ax1.axhline(pca_thr, color="#4da6ff", linestyle="--", alpha=0.7)
            ylabel = "PCA score/threshold" if args.plot_normalized else "PCA score"
            ax1.set_ylabel(ylabel, color="#4da6ff")
            ax1.tick_params(axis="y", labelcolor="#4da6ff")
            lines += ln1
            labels += [l.get_label() for l in ln1]

        if manager.iforest is not None:
            target_ax = ax2 if ax2 is not None else ax1
            if_vals = if_scores
            if_thr = manager.iforest.threshold
            if args.plot_normalized and if_thr:
                if_vals = [v / if_thr if v is not None else None for v in if_scores]
                if_thr = 1.0
            ln2 = target_ax.plot(xs, if_vals, label="IForest score", color="#f97316")
            target_ax.axhline(if_thr, color="#f97316", linestyle="--", alpha=0.7)
            ylabel = "IForest score/threshold" if args.plot_normalized else "IForest score"
            target_ax.set_ylabel(ylabel, color="#f97316")
            target_ax.tick_params(axis="y", labelcolor="#f97316")
            lines += ln2
            labels += [l.get_label() for l in ln2]

        if inject_mask is not None and inject_mask.sum() > 0:
            ax1.axvspan(
                np.where(inject_mask)[0][0],
                np.where(inject_mask)[0][-1],
                color="red",
                alpha=0.1,
                label="Injected window",
            )

        title = "Model Scores with Thresholds (Dual Axis)"
        if args.plot_normalized:
            title = "Model Scores (Normalized, Dual Axis)"
        ax1.set_title(title)
        ax1.set_xlabel("Sample index")

        if lines:
            ax1.legend(lines, labels, loc="upper right")

        fig.tight_layout()
        fig.savefig(plot_path, dpi=160)
        plt.close(fig)
        plot_saved = True
    except Exception:
        plot_saved = False

    print("[eval] done")
    print(f"[eval] report: {report_path}")
    print(f"[eval] top anomalies: {top_path}")
    if plot_saved:
        print(f"[eval] plot: {plot_path}")
    else:
        print("[eval] plot skipped (matplotlib missing)")


if __name__ == "__main__":
    main()
