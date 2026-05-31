"""
Grey-box RF model başarı değerlendirmesi.

Metrikler:
  - Regresyon: R², MAE, RMSE (train CSV üzerinde, ters dinamik ile)
  - Anomali   : detection rate, per-axis hit rate (herhangi bir CSV üzerinde)
  - Karşılaştırma: iki CSV verilirse (normal vs. injected) oranları kıyaslar

Usage:
  cd mindtwin
  # Yalnızca eğitim verisi üzerinde regresyon kalitesi:
  python -X utf8 models/stats/evaluate_graybox_rf.py \\
      --csv "autoencoder/Data_for_Train/kuka_log_900scnd_100hz(=5,8hz).csv" \\
      --urdf robot.urdf

  # Normal vs. anomali karşılaştırması:
  python -X utf8 models/stats/evaluate_graybox_rf.py \\
      --csv  "autoencoder/Data_for_Train/kuka_log_900scnd_100hz(=5,8hz).csv" \\
      --csv2 "data/kuka_log600_scnd_20hz(canta)_injected.csv" \\
      --urdf robot.urdf
"""

import argparse
import json
import re
import sys
from pathlib import Path

import joblib
import numpy as np
from scipy.signal import savgol_filter

# ── path setup ────────────────────────────────────────────────────────────────
_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here))
sys.path.insert(0, str(_here.parents[0]))  # models/

try:
    from feature_builder import calibrate_scale, detect_torque_columns, load_csv_rows
except ImportError:
    sys.exit("[eval] HATA: feature_builder import edilemedi. 'mindtwin/' dizininden çalıştır.")

from urdf_dynamics import compute_inverse_dynamics_batch


# ── helpers ───────────────────────────────────────────────────────────────────

def _to_float(x):
    try: return float(x)
    except: return None


def _detect_col(headers, name):
    norm = {k.lower(): k for k in headers if isinstance(k, str)}
    return norm.get(name.lower())


def _detect_vel_cols(headers):
    norm = {k.lower(): k for k in headers if isinstance(k, str)}
    cols = []
    for i in range(1, 7):
        c = norm.get(f"vel_axis_act_a{i}")
        if c: cols.append(c)
        else: return None
    return cols


def _detect_curr_cols(headers):
    norm = {k.lower(): k for k in headers if isinstance(k, str)}
    cols = []
    for i in range(1, 7):
        c = norm.get(f"curr_a{i}")
        if c: cols.append(c)
        else: return None
    return cols


def _parse_axis_act(raw):
    tokens = re.findall(r'A(\d)\s+([-\d.]+(?:[Ee][-+]?\d+)?)', raw)
    q = [0.0] * 6
    found = 0
    for s, v in tokens:
        idx = int(s) - 1
        if 0 <= idx < 6:
            q[idx] = float(v) * (np.pi / 180.0)
            found += 1
    return q if found > 0 else None


def _parse_timestamps(rows):
    from datetime import datetime
    ts = []
    for r in rows:
        raw = r.get("timestamp") or r.get("Timestamp") or ""
        try: ts.append(datetime.fromisoformat(raw.replace(" ", "T")))
        except: return np.arange(len(rows)) * 0.01
    if len(ts) == len(rows) and len(ts) > 1:
        t0 = ts[0]
        return np.array([(t - t0).total_seconds() for t in ts])
    return np.arange(len(rows)) * 0.01


def _preprocess(rows, meta, urdf_path, no_brake_filter=False):
    """CSV → (X_base, tau_valid, tau_model_valid, idx_valid)"""
    headers = list(rows[0].keys())
    axis_act_col  = _detect_col(headers, "axis_act")
    brake_sig_col = _detect_col(headers, "brake_sig")
    vel_cols      = _detect_vel_cols(headers)
    curr_cols     = _detect_curr_cols(headers) if meta.get("curr_used") else None

    torque_cols = detect_torque_columns(headers)[:6]

    # q matrix
    q_all = np.zeros((len(rows), 6))
    for i, r in enumerate(rows):
        raw = r.get(axis_act_col or "", "") or ""
        parsed = _parse_axis_act(raw)
        if parsed: q_all[i] = parsed

    dt = float(np.median(np.diff(_parse_timestamps(rows))))

    q_smooth = savgol_filter(q_all, window_length=7, polyorder=2, axis=0)

    if vel_cols:
        vel_raw = np.array(
            [[_to_float(r.get(c)) or 0.0 for c in vel_cols] for r in rows]
        ) * (np.pi / 180.0)
        win = min(7, len(vel_raw) if len(vel_raw) % 2 == 1 else len(vel_raw) - 1)
        q_dot = savgol_filter(vel_raw, window_length=win, polyorder=2, axis=0)
        q_ddot = np.gradient(q_dot, dt, axis=0)
        vel_source = "VEL_AXIS_ACT"
    else:
        q_dot  = np.gradient(q_smooth, dt, axis=0)
        q_ddot = np.gradient(q_dot, dt, axis=0)
        vel_source = "AXIS_ACT türevi"

    # BRAKE_SIG filter
    if brake_sig_col and not no_brake_filter:
        brake_vals = np.array([_to_float(r.get(brake_sig_col)) for r in rows])
        idx_valid = np.where(brake_vals == 63)[0]
        if len(idx_valid) == 0:
            print("  [UYARI] BRAKE_SIG==63 filtresi sıfır satır bıraktı → filtre atlanıyor")
            idx_valid = np.arange(len(rows))
    else:
        idx_valid = np.arange(len(rows))

    q_v   = q_smooth[idx_valid]
    qd_v  = q_dot[idx_valid]
    qdd_v = q_ddot[idx_valid]

    # Torque
    scale = meta.get("producer_scale", 1.0)
    tau_v = np.zeros((len(idx_valid), 6))
    for j, col in enumerate(torque_cols):
        for ii, ri in enumerate(idx_valid):
            v = _to_float(rows[ri].get(col))
            tau_v[ii, j] = v if v is not None else 0.0
    tau_v /= scale

    # Inverse dynamics
    tau_model_v = None
    if meta.get("inverse_dynamics_used") and urdf_path:
        try:
            tau_model_v = compute_inverse_dynamics_batch(urdf_path, q_v, qd_v, qdd_v)
        except Exception as e:
            print(f"  [UYARI] ters dinamik başarısız: {e}")

    # CURR feature (motor currents)
    if curr_cols is not None:
        curr_v = np.array(
            [[_to_float(rows[ri].get(c)) or 0.0 for c in curr_cols] for ri in idx_valid],
            dtype=float
        )
        X_base = np.hstack([q_v, qd_v, curr_v])
    else:
        X_base = np.hstack([q_v, qd_v])
    return X_base, tau_v, tau_model_v, idx_valid, vel_source


# ── regression metrics ────────────────────────────────────────────────────────

def _predict_single(mdl, X):
    """n_jobs=1 ile predict — consumer çalışırken RAM'i korur."""
    orig = mdl.n_jobs
    mdl.n_jobs = 1
    try:
        return mdl.predict(X)
    finally:
        mdl.n_jobs = orig


def regression_metrics(models, thresholds, X_base, tau_v, tau_model_v):
    """Per-axis R², MAE, RMSE, anomaly rate."""
    results = []
    for j, mdl in enumerate(models):
        ax = j + 1
        if tau_model_v is not None:
            X = np.hstack([X_base, tau_model_v[:, j:j+1]])
        else:
            X = X_base

        Y = tau_v[:, j]
        Y_pred = _predict_single(mdl, X)
        residuals = np.abs(Y - Y_pred)
        ss_res = np.sum((Y - Y_pred) ** 2)
        ss_tot = np.sum((Y - np.mean(Y)) ** 2)
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else float('nan')

        anomaly_rate = float(np.mean(residuals > thresholds[j]))
        results.append({
            "axis":         f"A{ax}",
            "r2":           r2,
            "mae":          float(np.mean(residuals)),
            "rmse":         float(np.sqrt(np.mean((Y - Y_pred) ** 2))),
            "threshold":    thresholds[j],
            "mean_residual":float(np.mean(residuals)),
            "std_residual": float(np.std(residuals)),
            "anomaly_rate": anomaly_rate,
            "n_samples":    len(Y),
        })
    return results


# ── detection metrics (no ground truth) ──────────────────────────────────────

def detection_metrics(models, thresholds, X_base, tau_v, tau_model_v):
    """Per-frame anomaly decision → overall detection rate + per-axis hit rate."""
    n = len(tau_v)
    per_axis_anomaly = np.zeros((n, len(models)), dtype=bool)

    for j, mdl in enumerate(models):
        if tau_model_v is not None:
            X = np.hstack([X_base, tau_model_v[:, j:j+1]])
        else:
            X = X_base
        Y_pred = _predict_single(mdl, X)
        per_axis_anomaly[:, j] = np.abs(tau_v[:, j] - Y_pred) > thresholds[j]

    axes_fired     = per_axis_anomaly.sum(axis=1)   # kaç eksen tetiklendi (frame başına)
    frame_any      = axes_fired >= 1
    frame_multi    = axes_fired >= 2

    return {
        "total_frames":      n,
        "anomaly_frames_1":  int(frame_any.sum()),
        "anomaly_frames_2":  int(frame_multi.sum()),
        "detection_rate_1":  float(frame_any.mean()),
        "detection_rate_2":  float(frame_multi.mean()),
        "per_axis_hit_rate": {f"A{j+1}": float(per_axis_anomaly[:, j].mean()) for j in range(len(models))},
        "worst_axis":        f"A{1 + int(per_axis_anomaly.mean(axis=0).argmax())}",
    }


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv",        required=True, help="Birinci CSV (normal veri önerilir)")
    parser.add_argument("--csv2",       default=None,  help="İkinci CSV (anomali / injected)")
    parser.add_argument("--urdf",       default=None)
    parser.add_argument("--model-dir",  default="artifacts")
    parser.add_argument("--max-rows",       type=int, default=None)
    parser.add_argument("--no-brake-filter", action="store_true",
                        help="BRAKE_SIG==63 filtresini atla (test/injected veri için)")
    args = parser.parse_args()

    model_path = Path(args.model_dir) / "graybox_rf_models.pkl"
    thr_path   = Path(args.model_dir) / "graybox_rf_thresholds.json"

    if not model_path.exists():
        sys.exit(f"[eval] Model bulunamadı: {model_path}\nÖnce train_graybox_rf.py çalıştır.")

    with open(thr_path, encoding="utf-8") as f:
        meta = json.load(f)
    bundle  = joblib.load(model_path)
    models  = bundle["models"]
    thrs    = meta["thresholds"]

    if args.urdf:
        urdf_path = str(Path(args.urdf).resolve())
    else:
        candidate = Path(__file__).resolve().parents[2] / "robot.urdf"
        urdf_path = str(candidate) if candidate.exists() else None

    def evaluate_csv(csv_path, label):
        print(f"\n{'='*60}")
        print(f"  CSV: {csv_path}  [{label}]")
        print(f"{'='*60}")
        rows = load_csv_rows(csv_path)
        if args.max_rows:
            rows = rows[:args.max_rows]
        print(f"  Satır sayısı : {len(rows)}")

        X_base, tau_v, tau_model_v, idx_valid, vel_src = _preprocess(
            rows, meta, urdf_path, no_brake_filter=args.no_brake_filter
        )
        print(f"  Geçerli satır: {len(idx_valid)}  (BRAKE_SIG==63)")
        print(f"  q_dot kaynağı: {vel_src}")

        # Regresyon kalitesi
        reg = regression_metrics(models, thrs, X_base, tau_v, tau_model_v)
        print(f"\n  ── Regresyon Kalitesi (tüm veri) ──────────────────────")
        print(f"  {'Eksen':<6} {'R²':>7} {'MAE':>9} {'RMSE':>9} {'Eşik':>9} {'Anomali%':>9}")
        print(f"  {'-'*55}")
        for r in reg:
            flag = "⚠" if r['anomaly_rate'] > 0.10 else ""
            print(f"  {r['axis']:<6} {r['r2']:>7.4f} {r['mae']:>9.4f} {r['rmse']:>9.4f}"
                  f" {r['threshold']:>9.4f} {r['anomaly_rate']*100:>8.1f}% {flag}")

        r2_avg = float(np.mean([r['r2'] for r in reg]))
        print(f"\n  Ortalama R² : {r2_avg:.4f}  "
              f"({'İyi ≥0.95' if r2_avg >= 0.95 else 'Orta 0.8-0.95' if r2_avg >= 0.80 else 'Zayıf <0.80'})")

        # Anomali tespiti
        det = detection_metrics(models, thrs, X_base, tau_v, tau_model_v)
        print(f"\n  ── Anomali Tespiti ──────────────────────────────────")
        print(f"  Toplam frame       : {det['total_frames']}")
        print(f"  ≥1 eksen tetiklendi: {det['anomaly_frames_1']} ({det['detection_rate_1']*100:.1f}%)")
        print(f"  ≥2 eksen tetiklendi: {det['anomaly_frames_2']} ({det['detection_rate_2']*100:.1f}%)  ← aktif kural")
        print(f"  En kötü eksen      : {det['worst_axis']}")
        print(f"\n  Per-axis tetiklenme oranı:")
        for ax, rate in det['per_axis_hit_rate'].items():
            bar = '█' * int(rate * 40)
            print(f"    {ax}: {rate*100:5.1f}%  {bar}")

        return det

    det1 = evaluate_csv(args.csv, "birinci CSV")

    if args.csv2:
        det2 = evaluate_csv(args.csv2, "ikinci CSV (anomali)")
        print(f"\n{'='*60}")
        print(f"  KARŞILAŞTIRMA")
        print(f"{'='*60}")
        for rule, key in [("≥1 eksen", "detection_rate_1"), ("≥2 eksen (aktif)", "detection_rate_2")]:
            dr1 = det1[key]; dr2 = det2[key]
            lift = (dr2 / dr1) if dr1 > 0 else float('inf')
            tag = 'İyi ≥5×' if lift >= 5 else 'Orta 2-5×' if lift >= 2 else 'Zayıf <2×'
            print(f"  Kural {rule}:")
            print(f"    Normal : {dr1*100:.1f}%   Anomali: {dr2*100:.1f}%   Lift: {lift:.1f}×  ({tag})")

        print(f"\n  Per-axis karşılaştırma:")
        axes = list(det1['per_axis_hit_rate'].keys())
        print(f"  {'Eksen':<6} {'Normal%':>9} {'Anomali%':>10} {'Lift':>7}")
        print(f"  {'-'*36}")
        for ax in axes:
            r1 = det1['per_axis_hit_rate'].get(ax, 0)
            r2 = det2['per_axis_hit_rate'].get(ax, 0)
            lft = (r2 / r1) if r1 > 0 else float('inf')
            flag = "◀ iyi" if lft >= 3 else ""
            print(f"  {ax:<6} {r1*100:>8.1f}% {r2*100:>9.1f}%  {lft:>6.1f}×  {flag}")

    print()


if __name__ == "__main__":
    main()
