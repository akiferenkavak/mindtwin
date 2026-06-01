"""
Export per-frame Random Forest residuals to rf_results.json.

Bu script evaluate_graybox_rf.py'nin preprocess mantığını kullanarak
graybox_rf_models.pkl üzerinden her frame için:
  - max_residual (normalized)
  - per-axis residuals
  - is_anomaly
  değerlerini hesaplar ve artifacts/rf_results.json'a yazar.

Usage (mindtwin/ kök dizininden):
  python models/stats/export_rf_results.py ^
    --csv "autoencoder/Data_for_Train/kuka_log_900scnd_100hz(=5,8hz).csv" ^
    --output-dir artifacts

URDF zorunlu değil — ters dinamik yoksa 12/18-boyutlu (q+qd[+curr]) özellik vektörü kullanılır.
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
from scipy.signal import savgol_filter

# ── path bootstrap ─────────────────────────────────────────────────────────────
_HERE      = Path(__file__).resolve()
_STATS_DIR = _HERE.parent          # models/stats/
_ROOT_DIR  = _HERE.parents[2]      # mindtwin/

for _p in [str(_STATS_DIR), str(_STATS_DIR.parent), str(_ROOT_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)


# ── helpers ────────────────────────────────────────────────────────────────────

def _to_float(x):
    try:
        return float(x)
    except Exception:
        return None


def _detect_col(headers, name):
    norm = {k.lower(): k for k in headers if isinstance(k, str)}
    return norm.get(name.lower())


def _detect_vel_cols(headers):
    norm = {k.lower(): k for k in headers if isinstance(k, str)}
    cols = []
    for i in range(1, 7):
        c = norm.get(f"vel_axis_act_a{i}")
        if c:
            cols.append(c)
        else:
            return None
    return cols


def _detect_curr_cols(headers):
    norm = {k.lower(): k for k in headers if isinstance(k, str)}
    cols = []
    for i in range(1, 7):
        c = norm.get(f"curr_a{i}")
        if c:
            cols.append(c)
        else:
            return None
    return cols


def _detect_torque_cols(headers):
    norm = {k.lower(): k for k in headers if isinstance(k, str)}
    # try torque_a1..a6
    cand = [f"torque_a{i}" for i in range(1, 7)]
    if all(c in norm for c in cand):
        return [norm[c] for c in cand]
    # try avg_torque_a1..a6
    cand2 = [f"avg_torque_a{i}" for i in range(1, 7)]
    if all(c in norm for c in cand2):
        return [norm[c] for c in cand2]
    raise RuntimeError(f"Torque columns not found in header: {list(headers)[:20]}")


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
    from datetime import datetime as _dt
    ts_list = []
    for r in rows:
        raw = r.get("timestamp") or r.get("Timestamp") or ""
        try:
            ts_list.append(_dt.fromisoformat(raw.replace(" ", "T")))
        except Exception:
            return np.arange(len(rows)) * 0.01
    if len(ts_list) == len(rows) and len(ts_list) > 1:
        t0 = ts_list[0]
        return np.array([(t - t0).total_seconds() for t in ts_list])
    return np.arange(len(rows)) * 0.01


def load_csv_rows(csv_path: str):
    import csv
    with open(csv_path, newline="", encoding="utf-8") as f:
        first = f.readline(); f.seek(0)
        delim = ";" if (";" in first and "," not in first) else ","
        reader = csv.DictReader(f, delimiter=delim)
        rows = list(reader)
    if not rows:
        raise RuntimeError(f"CSV empty or unreadable: {csv_path}")
    return rows


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Export Grey-box RF per-frame residuals to rf_results.json")
    parser.add_argument("--csv",        required=True, help="CSV path (same CSV used for training)")
    parser.add_argument("--output-dir", default="artifacts")
    parser.add_argument("--max-rows",   type=int, default=None)
    parser.add_argument("--urdf",       default=None, help="URDF path (optional — enables inverse dynamics)")
    parser.add_argument("--no-brake-filter", action="store_true",
                        help="Skip BRAKE_SIG==63 filter")
    # ── Kontrollü arıza enjeksiyonu (sunum demosu için) ──
    parser.add_argument("--inject-joints", default=None,
                        help="Spike enjekte edilecek eksenler, örn '2' veya '2,4' (1-6)")
    parser.add_argument("--inject-factor", type=float, default=1.0,
                        help="Tork çarpanı: tau *= (1+factor). 1.0 = iki katı.")
    parser.add_argument("--inject-window", type=float, default=0.4,
                        help="Operasyonel frame'lerin ortadaki bu oranına enjekte edilir (0-1).")
    args = parser.parse_args()

    model_path = Path(args.output_dir) / "graybox_rf_models.pkl"
    thr_path   = Path(args.output_dir) / "graybox_rf_thresholds.json"

    if not model_path.exists():
        sys.exit(f"[export_rf] ERROR: {model_path} not found. Run train_graybox_rf.py first.")
    if not thr_path.exists():
        sys.exit(f"[export_rf] ERROR: {thr_path} not found.")

    print(f"[export_rf] Loading model from {model_path} ...")
    bundle    = joblib.load(model_path)
    models    = bundle["models"]     # list of 6 BaggingRegressor
    for mdl in models:
        mdl.n_jobs = 1

    with open(thr_path, encoding="utf-8") as f:
        meta = json.load(f)

    thresholds    = meta["thresholds"]          # list[6]
    producer_scale = float(meta.get("producer_scale", 1.0))
    use_inv_dyn   = bool(meta.get("inverse_dynamics_used", False))
    curr_used     = bool(meta.get("curr_used", False))

    # URDF resolution
    urdf_path = args.urdf
    if urdf_path is None:
        # try meta, then project-root
        stored = meta.get("urdf_path", "")
        if stored and Path(stored).exists():
            urdf_path = stored
        else:
            candidate = _ROOT_DIR / "robot.urdf"
            if candidate.exists():
                urdf_path = str(candidate)

    if use_inv_dyn and urdf_path:
        print(f"[export_rf] URDF: {urdf_path} — inverse dynamics ENABLED")
    else:
        if use_inv_dyn:
            print("[export_rf] WARNING: URDF not found — running WITHOUT inverse dynamics (12/18-feature mode)")
        use_inv_dyn = False

    # Load CSV
    print(f"[export_rf] Loading CSV: {args.csv}")
    rows = load_csv_rows(args.csv)
    if args.max_rows:
        rows = rows[:args.max_rows]
    print(f"[export_rf] Rows: {len(rows)}")

    headers = list(rows[0].keys())
    axis_act_col  = _detect_col(headers, "axis_act")
    brake_sig_col = _detect_col(headers, "brake_sig")
    vel_cols      = _detect_vel_cols(headers)
    torque_cols   = _detect_torque_cols(headers)

    curr_cols = None
    if curr_used:
        curr_cols = _detect_curr_cols(headers)
        if curr_cols:
            print(f"[export_rf] CURR columns found: {curr_cols}")
        else:
            print("[export_rf] WARNING: CURR not found in CSV — curr features will be zeros")

    if axis_act_col is None:
        sys.exit("[export_rf] ERROR: AXIS_ACT column not found — CSV missing kinematics.")

    print(f"[export_rf] AXIS_ACT: {axis_act_col}  VEL_AXIS_ACT: {'yes' if vel_cols else 'no'}")

    # ── Preprocess kinematics ─────────────────────────────────────────────────
    print("[export_rf] Computing kinematics ...")

    q_all = np.zeros((len(rows), 6))
    for i, r in enumerate(rows):
        raw = r.get(axis_act_col, "") or ""
        parsed = _parse_axis_act(raw)
        if parsed:
            q_all[i] = parsed

    dt = float(np.median(np.diff(_parse_timestamps(rows))))
    win = min(7, len(q_all) if len(q_all) % 2 == 1 else len(q_all) - 1)
    q_smooth = savgol_filter(q_all, window_length=win, polyorder=2, axis=0)

    if vel_cols:
        vel_raw = np.array(
            [[_to_float(r.get(c)) or 0.0 for c in vel_cols] for r in rows],
            dtype=float
        ) * (np.pi / 180.0)
        q_dot   = savgol_filter(vel_raw, window_length=win, polyorder=2, axis=0)
        q_ddot  = np.gradient(q_dot, dt, axis=0)
    else:
        q_dot   = np.gradient(q_smooth, dt, axis=0)
        q_ddot  = np.gradient(q_dot, dt, axis=0)

    # Torque
    tau_all = np.zeros((len(rows), 6))
    for j, col in enumerate(torque_cols):
        for i, r in enumerate(rows):
            v = _to_float(r.get(col))
            tau_all[i, j] = (v if v is not None else 0.0) / producer_scale

    # CURR
    curr_all = None
    if curr_cols:
        curr_all = np.array(
            [[_to_float(r.get(c)) or 0.0 for c in curr_cols] for r in rows],
            dtype=float
        )
    elif curr_used:
        curr_all = np.zeros((len(rows), 6))

    # BRAKE_SIG filter
    if brake_sig_col and not args.no_brake_filter:
        brake_vals = np.array([_to_float(r.get(brake_sig_col)) for r in rows])
        idx_valid = np.where(brake_vals == 63)[0]
        if len(idx_valid) == 0:
            print("  [WARN] BRAKE_SIG==63 filter left 0 rows — skipping filter")
            idx_valid = np.arange(len(rows))
    else:
        idx_valid = np.arange(len(rows))

    print(f"[export_rf] Valid rows: {len(idx_valid)}")

    # ── Operasyonel frame'ler için tork matrisi (injection buraya uygulanır) ──
    tau_valid = tau_all[idx_valid].copy()   # (N_valid, 6) — injection bu matris üzerinde

    # ── Kontrollü arıza enjeksiyonu ──────────────────────────────────────────
    injection_meta = None
    if args.inject_joints:
        inj_joints = [int(j.strip()) for j in args.inject_joints.split(",") if j.strip()]
        frac = max(0.0, min(1.0, args.inject_window))
        n_v  = len(idx_valid)
        win  = int(n_v * frac)
        i_start = (n_v - win) // 2
        i_end   = i_start + win
        for j in inj_joints:
            tau_valid[i_start:i_end, j - 1] *= (1.0 + args.inject_factor)
        # frame_no aralığı (original row index)
        fn_start = int(idx_valid[i_start]) if i_start < n_v else 0
        fn_end   = int(idx_valid[i_end - 1]) if i_end <= n_v else int(idx_valid[-1])
        injection_meta = {
            "joints":       inj_joints,
            "axes":         [f"A{j}" for j in inj_joints],
            "frame_start":  fn_start,
            "frame_end":    fn_end,
            "seq_start":    i_start,   # sequential index (for RF detection stats)
            "seq_end":      i_end,
            "factor":       args.inject_factor,
            "method":       "spike",
            "window_frac":  frac,
        }
        print(f"[export_rf] ENJEKSIYON: eksenler {injection_meta['axes']} "
              f"seq [{i_start},{i_end}) frame_no [{fn_start},{fn_end}] "
              f"tork ×{1+args.inject_factor:.2f}")

    # ── Inverse dynamics batch ────────────────────────────────────────────────
    tau_model_all = None
    if use_inv_dyn and urdf_path:
        try:
            sys.path.insert(0, str(_STATS_DIR))
            from urdf_dynamics import compute_inverse_dynamics_batch
            print("[export_rf] Computing inverse dynamics (batch) ...")
            q_v   = q_smooth[idx_valid]
            qd_v  = q_dot[idx_valid]
            qdd_v = q_ddot[idx_valid]
            tau_model_all = compute_inverse_dynamics_batch(urdf_path, q_v, qd_v, qdd_v)
            print(f"[export_rf] Inverse dynamics done — shape: {tau_model_all.shape}")
        except Exception as e:
            print(f"[export_rf] WARNING: inverse dynamics failed ({e}) — skipping")
            tau_model_all = None

    # ── Score each frame ──────────────────────────────────────────────────────
    print("[export_rf] Scoring frames ...")
    results_per_axis = {f"TORQUE_A{i+1}": [] for i in range(6)}
    overall = []

    for ii, ri in enumerate(idx_valid):
        r        = rows[ri]
        ts       = r.get("timestamp", "")
        q_i      = q_smooth[ri]
        qd_i     = q_dot[ri]
        tau_i    = tau_valid[ii]        # injection varsa zaten uygulanmış
        curr_i   = curr_all[ri] if curr_all is not None else None

        if curr_i is not None:
            base_x = np.concatenate([q_i, qd_i, curr_i])
        else:
            base_x = np.concatenate([q_i, qd_i])

        residuals = []
        any_anomaly = False
        worst_j = None
        max_res = 0.0

        for j, mdl in enumerate(models):
            ax = j + 1
            thr = thresholds[j]

            if tau_model_all is not None:
                x = np.append(base_x, tau_model_all[ii, j]).reshape(1, -1)
            else:
                x = base_x.reshape(1, -1)

            try:
                tau_pred = float(mdl.predict(x)[0])
            except Exception:
                tau_pred = 0.0

            tau_real = float(tau_i[j])
            residual = abs(tau_real - tau_pred)
            is_anom  = residual > thr
            ratio    = residual / thr if thr > 0 else 0.0

            residuals.append(residual)
            if is_anom:
                any_anomaly = True
            if residual > max_res:
                max_res = residual
                worst_j = ax

            feat_key = f"TORQUE_A{ax}"
            results_per_axis[feat_key].append({
                "frame_no":   int(ri),
                "timestamp":  ts,
                "feature":    feat_key,
                "score":      round(residual, 6),
                "threshold":  round(thr, 6),
                "is_anomaly": bool(is_anom),
                "severity_ratio": round(ratio, 4),
            })

        overall.append({
            "frame_no":      int(ri),
            "timestamp":     ts,
            "max_residual":  round(max_res, 6),
            "is_anomaly":    bool(any_anomaly),
            "worst_joint":   worst_j,
            "residuals":     [round(r, 6) for r in residuals],
        })

        if (ii + 1) % 500 == 0:
            print(f"  ... {ii+1}/{len(idx_valid)} frames scored")

    # ── Meta ──────────────────────────────────────────────────────────────────
    n_anom = sum(1 for o in overall if o["is_anomaly"])
    out_meta = {
        "generated_at":        datetime.now().isoformat(),
        "thresholds":          [round(t, 6) for t in thresholds],
        "torque_axes":         meta.get("torque_axes", [f"A{i+1}" for i in range(6)]),
        "sigma_mult":          meta.get("sigma_mult", 3.0),
        "n_estimators":        meta.get("n_estimators", 100),
        "feature_dim":         meta.get("feature_dim", len(base_x)),
        "training_samples":    meta.get("training_samples"),
        "inverse_dynamics_used": use_inv_dyn,
        "vel_axis_act_used":   vel_cols is not None,
        "curr_used":           curr_cols is not None,
        "samples_exported":    len(overall),
        "anomaly_count":       n_anom,
        "injection":           injection_meta,
    }

    # Enjeksiyon varsa pencere içi/dışı tespit oranını raporla
    if injection_meta:
        s, e = injection_meta["seq_start"], injection_meta["seq_end"]
        inside  = [o for o in overall[s:e] if o["is_anomaly"]]
        outside = [o for o in (overall[:s] + overall[e:]) if o["is_anomaly"]]
        n_in = len(overall[s:e]); n_out = len(overall[:s]) + len(overall[e:])
        print(f"[export_rf] Tespit pencere İÇİ : {len(inside)}/{n_in} = {100*len(inside)/max(n_in,1):.1f}%")
        print(f"[export_rf] Tespit pencere DIŞI: {len(outside)}/{n_out} = {100*len(outside)/max(n_out,1):.1f}%")

    result = {
        "meta":       out_meta,
        "overall":    overall,
        "per_feature": results_per_axis,
    }

    os.makedirs(args.output_dir, exist_ok=True)
    out_path = os.path.join(args.output_dir, "rf_results.json")
    print(f"[export_rf] Writing {out_path} ...")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False)

    print(f"[export_rf] Done -> {out_path}")
    print(f"[export_rf] samples={out_meta['samples_exported']}  anomalies={n_anom}")
    print(f"[export_rf] thresholds: {out_meta['thresholds']}")


if __name__ == "__main__":
    main()
