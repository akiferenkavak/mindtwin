import argparse
import csv
from pathlib import Path
from typing import List

import numpy as np

try:
    from models.feature_builder import detect_torque_columns, load_csv_rows, to_float
except ImportError:
    from pathlib import Path as _Path
    import sys as _sys

    root = _Path(__file__).resolve().parents[1]
    _sys.path.append(str(root))
    from models.feature_builder import detect_torque_columns, load_csv_rows, to_float  # noqa: E402


def parse_ratio(val: str) -> float:
    v = float(val)
    if v < 0 or v > 1:
        raise argparse.ArgumentTypeError("ratio 0-1 araliginda olmali")
    return v


def inject_anomalies(
    X: np.ndarray,
    inject_ratio: float,
    method: str,
    spike_factor: float,
    noise_std: float,
    seed: int,
    fixed_joint: int = None,
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
        # fixed_joint (1-based) verilirse hep o ekseni boz; yoksa eksenler arasında dön
        if fixed_joint is not None:
            j = (fixed_joint - 1) % dim
        else:
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
        "fixed_joint": fixed_joint,
    }

    return X_inj, mask, info


def main():
    parser = argparse.ArgumentParser(description="Inject anomalies into a CSV and save new file")
    parser.add_argument("--csv", required=True, help="Input CSV yolu")
    parser.add_argument("--output", required=True, help="Output CSV yolu")
    parser.add_argument("--inject", type=parse_ratio, default=0.5, help="Inject oranı (0-1)")
    parser.add_argument(
        "--inject-method",
        choices=["spike", "swap", "noise", "mix"],
        default="spike",
    )
    parser.add_argument("--spike-factor", type=float, default=1.0)
    parser.add_argument("--noise-std", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--joint", type=int, default=None,
                        help="Sabit eksen (1-6). Verilirse hep o tork ekseni bozulur; yoksa eksenler arasında dönülür.")
    args = parser.parse_args()

    rows = load_csv_rows(args.csv)
    torque_cols = detect_torque_columns(rows[0].keys())

    # Build feature matrix (only torque columns)
    feats: List[List[float]] = []
    row_indices: List[int] = []
    for i, r in enumerate(rows):
        vals = [to_float(r.get(k)) for k in torque_cols]
        if any(v is None for v in vals):
            continue
        feats.append([float(v) for v in vals])
        row_indices.append(i)

    if not feats:
        raise RuntimeError("Feature matrisi bos. Torque kolonlarini kontrol et.")

    X = np.asarray(feats, dtype=float)
    X_inj, mask, info = inject_anomalies(
        X,
        inject_ratio=args.inject,
        method=args.inject_method,
        spike_factor=args.spike_factor,
        noise_std=args.noise_std,
        seed=args.seed,
        fixed_joint=args.joint,
    )

    # Apply injected values back to rows
    for local_idx, row_idx in enumerate(row_indices):
        for j, col in enumerate(torque_cols):
            rows[row_idx][col] = f"{X_inj[local_idx, j]:.6f}"

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Write CSV with same headers
    headers = list(rows[0].keys())
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    print("[inject] done")
    print(f"[inject] output: {out_path}")
    if info:
        print(f"[inject] window: {info['inject_start']}..{info['inject_end']} method={info['method']} spike={info['spike_factor']} noise={info['noise_std']}")


if __name__ == "__main__":
    main()
