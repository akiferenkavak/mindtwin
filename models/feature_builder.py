import csv
import math
from typing import Iterable, Sequence, Optional

import numpy as np


JOINT_COUNT = 6
TARGET_THR = 0.45
Q = 0.99
CALIB_ROWS = 400


def to_float(x):
    try:
        v = float(x)
        return v
    except Exception:
        return None


def detect_torque_columns(header_keys: Iterable[str]) -> list[str]:
    keys = [k for k in header_keys if isinstance(k, str)]
    norm = {k.lower(): k for k in keys}

    cand1 = [f"torque_a{i}" for i in range(1, JOINT_COUNT + 1)]
    if all(k in norm for k in cand1):
        return [norm[k] for k in cand1]

    cand2 = [f"avg_torque_a{i}" for i in range(1, JOINT_COUNT + 1)]
    if all(k in norm for k in cand2):
        return [norm[k] for k in cand2]

    cand3 = sorted(k for k in norm if k.startswith("avg_torque_a"))
    if cand3:
        return [norm[k] for k in cand3]

    raise RuntimeError(f"Torque kolonlari bulunamadi. Header: {keys}")


def _detect_delimiter(first_line: str) -> str:
    if ";" in first_line and "," not in first_line:
        return ";"
    if "," in first_line:
        return ","
    return ";"


def quantile(sorted_vals: list[float], q: float) -> Optional[float]:
    if not sorted_vals:
        return None
    if q <= 0:
        return sorted_vals[0]
    if q >= 1:
        return sorted_vals[-1]
    pos = (len(sorted_vals) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_vals[lo]
    frac = pos - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def calibrate_scale(
    rows: Sequence[dict],
    torque_cols: Sequence[str],
    target_thr: float = TARGET_THR,
    q: float = Q,
    calib_rows: int = CALIB_ROWS,
) -> tuple[float, float]:
    pooled_diffs: list[float] = []
    prev = None

    take = min(len(rows), calib_rows)
    for i in range(take):
        r = rows[i]
        cur = [to_float(r.get(k)) for k in torque_cols]
        if any(v is None for v in cur):
            continue
        if prev is not None:
            for a, b in zip(cur, prev):
                pooled_diffs.append(abs(a - b))
        prev = cur

    pooled_diffs = [d for d in pooled_diffs if d is not None and math.isfinite(d)]
    pooled_diffs.sort()

    p = quantile(pooled_diffs, q)
    if p is None or p <= 0:
        return 1.0, 0.0

    scale = p / target_thr
    return scale, p


def load_csv_rows(csv_path: str) -> list[dict]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        first_line = f.readline()
        f.seek(0)
        delimiter = _detect_delimiter(first_line)
        reader = csv.DictReader(f, delimiter=delimiter)
        rows = list(reader)

    if not rows:
        raise RuntimeError("CSV bos ya da okunamadi.")

    return rows


def build_feature_matrix(
    rows: Sequence[dict],
    torque_cols: Sequence[str],
    limit: Optional[int] = None,
) -> np.ndarray:
    feats: list[list[float]] = []
    take = len(rows) if limit is None else min(len(rows), limit)

    for i in range(take):
        r = rows[i]
        vals = [to_float(r.get(k)) for k in torque_cols]
        if any(v is None for v in vals):
            continue
        if any(not math.isfinite(v) for v in vals):
            continue
        feats.append([float(v) for v in vals])

    if not feats:
        raise RuntimeError("Feature matrisi bos. Torque kolonlarini kontrol et.")

    return np.asarray(feats, dtype=float)
