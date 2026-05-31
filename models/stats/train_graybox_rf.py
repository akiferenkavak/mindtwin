"""
Grey-box Random Forest trainer — Python port of MATLAB Egit_ve_Kaydet.m

Features : [q(6, rad), q_dot(6, rad/s), tau_model_j(1, Nm)]  — kinematics + physics
Target   : normalised torque for A1..A6 (tau_raw / producer_scale)
Threshold: mean(|residual_val|) + sigma_mult * std(|residual_val|)  [on 20% validation split]
Model    : BaggingRegressor(DecisionTreeRegressor, n_estimators=100) — matches MATLAB 'Bag'

q_dot kaynağı (öncelik sırasıyla):
  1. VEL_AXIS_ACT_A1..A6 kolonları varsa → deg/s → rad/s (ölçülmüş, daha az gürültü)
  2. Yoksa → AXIS_ACT'tan SG filtre + double gradient (eski yöntem)

MATLAB karşılığı (Egit_ve_Kaydet.m):
  robot = importrobot('robot.urdf'); robot.Gravity = [0 0 -9.81];
  tau_model = inverseDynamics(robot, q, q_dot, q_ddot);
  X_train = [q_valid, q_dot_valid, tau_model_valid(:, j)];  (13 özellik, her eksen için)

Usage:
  cd mindtwin
  python models/stats/train_graybox_rf.py \\
      --csv "autoencoder/Data_for_Train/kuka_log_900scnd_100hz(=5,8hz).csv" \\
      --urdf robot.urdf \\
      --output-dir artifacts
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
from scipy.signal import savgol_filter
from sklearn.ensemble import BaggingRegressor
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor

try:
    from ..feature_builder import calibrate_scale, detect_torque_columns, load_csv_rows
except ImportError:
    models_dir = Path(__file__).resolve().parents[1]
    sys.path.append(str(models_dir))
    from feature_builder import calibrate_scale, detect_torque_columns, load_csv_rows


# ── helpers ──────────────────────────────────────────────────────────────────

def _to_float(x):
    try:
        return float(x)
    except Exception:
        return None


def _detect_axis_act_col(headers) -> Optional[str]:
    norm = {k.lower(): k for k in headers if isinstance(k, str)}
    return norm.get("axis_act")


def _detect_brake_sig_col(headers) -> Optional[str]:
    norm = {k.lower(): k for k in headers if isinstance(k, str)}
    return norm.get("brake_sig")


def _detect_vel_axis_act_cols(headers) -> Optional[list]:
    """VEL_AXIS_ACT_A1..A6 kolonlarını tespit et. Tümü yoksa None döndür."""
    norm = {k.lower(): k for k in headers if isinstance(k, str)}
    cols = []
    for i in range(1, 7):
        key = f"vel_axis_act_a{i}"
        if key in norm:
            cols.append(norm[key])
        else:
            return None
    return cols


def _detect_curr_cols(headers) -> Optional[list]:
    """CURR_A1..A6 kolonlarını tespit et. Tümü yoksa None döndür."""
    norm = {k.lower(): k for k in headers if isinstance(k, str)}
    cols = []
    for i in range(1, 7):
        key = f"curr_a{i}"
        if key in norm:
            cols.append(norm[key])
        else:
            return None
    return cols


def _parse_axis_act(raw: str) -> Optional[list[float]]:
    """Parse AXIS_ACT string 'A1 45.23 A2 -12.34 ...' → list of 6 angles in radians."""
    tokens = re.findall(r'A(\d)\s+([-\d.]+(?:[Ee][-+]?\d+)?)', raw)
    q = [0.0] * 6
    found = 0
    for idx_str, val_str in tokens:
        idx = int(idx_str) - 1
        if 0 <= idx < 6:
            q[idx] = float(val_str) * (np.pi / 180.0)  # deg → rad
            found += 1
    return q if found > 0 else None


def _parse_timestamps(rows: list[dict]) -> np.ndarray:
    """Return seconds-since-start array. Falls back to uniform 0.1 s on failure."""
    from datetime import datetime

    ts_list = []
    for r in rows:
        ts_raw = r.get("timestamp") or r.get("Timestamp") or ""
        try:
            ts_list.append(datetime.fromisoformat(ts_raw.replace(" ", "T")))
        except Exception:
            ts_list = []
            break

    if len(ts_list) == len(rows) and len(ts_list) > 1:
        t0 = ts_list[0]
        return np.array([(t - t0).total_seconds() for t in ts_list])

    print("[graybox_rf] WARNING: could not parse timestamps, assuming 100 Hz (dt=0.01 s)")
    return np.arange(len(rows)) * 0.01


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Train grey-box RF anomaly detector")
    parser.add_argument("--csv", required=True,
                        help="Eğitim CSV yolu (AXIS_ACT ve BRAKE_SIG kolonları gereklidir)")
    parser.add_argument("--output-dir", default="artifacts", help="Artifact çıkış klasörü")
    parser.add_argument("--n-estimators", type=int, default=100,
                        help="BaggingRegressor ağaç sayısı (varsayılan: 100)")
    parser.add_argument("--sigma", type=float, default=3.0,
                        help="Eşik katsayısı: mean(|res|) + sigma*std(|res|) [validation]")
    parser.add_argument("--val-size", type=float, default=0.2,
                        help="Validasyon seti oranı (varsayılan: 0.2)")
    parser.add_argument("--no-producer-scale", action="store_true",
                        help="Producer normalizasyonunu uygulama (ham tork ile çalış)")
    parser.add_argument("--no-brake-filter", action="store_true",
                        help="BRAKE_SIG == 63 filtresini atla")
    parser.add_argument("--max-rows", type=int, default=None,
                        help="Maksimum satır sayısı (hızlı test için)")
    parser.add_argument("--urdf", default=None,
                        help="robot.urdf yolu (varsayılan: mindtwin/robot.urdf otomatik aranır)")
    parser.add_argument("--no-inverse-dynamics", action="store_true",
                        help="Ters dinamiği atla — sadece [q, q_dot] özellik vektörü kullan (12 özellik)")
    parser.add_argument("--no-vel-act", action="store_true",
                        help="VEL_AXIS_ACT kolonlarını kullanma — AXIS_ACT'tan türev hesapla")
    parser.add_argument("--no-curr", action="store_true",
                        help="CURR kolonlarını özellik vektörüne ekleme")
    parser.add_argument("--random-state", type=int, default=42)
    args = parser.parse_args()

    # ── 1. CSV yükle ──────────────────────────────────────────────────────────
    print(f"[graybox_rf] CSV yükleniyor: {args.csv}")
    rows = load_csv_rows(args.csv)
    if args.max_rows:
        rows = rows[:args.max_rows]
    print(f"[graybox_rf] Toplam satır: {len(rows)}")

    headers = list(rows[0].keys())
    axis_act_col = _detect_axis_act_col(headers)
    brake_sig_col = _detect_brake_sig_col(headers)
    vel_cols = None if args.no_vel_act else _detect_vel_axis_act_cols(headers)
    curr_cols = None if args.no_curr else _detect_curr_cols(headers)

    if axis_act_col is None:
        sys.exit(
            "[graybox_rf] HATA: CSV'de AXIS_ACT kolonu bulunamadı.\n"
            "Graybox RF modeli eklem açılarına ihtiyaç duyar.\n"
            "Lütfen kuka_log_900scnd_100hz(=5,8hz).csv gibi tam log dosyası kullanın."
        )

    try:
        torque_cols = detect_torque_columns(headers)
    except RuntimeError as e:
        sys.exit(f"[graybox_rf] HATA: {e}")

    torque_cols_6 = torque_cols[:6]
    print(f"[graybox_rf] Tork kolonları (A1-A6): {torque_cols_6}")
    print(f"[graybox_rf] AXIS_ACT kolonu: {axis_act_col}")
    if vel_cols:
        print(f"[graybox_rf] VEL_AXIS_ACT kolonları bulundu — ölçülmüş hız kullanılıyor: {vel_cols}")
    else:
        print(f"[graybox_rf] VEL_AXIS_ACT yok — AXIS_ACT'tan çift türev hesaplanacak")
    if curr_cols:
        print(f"[graybox_rf] CURR kolonları bulundu — motor akımı özellik olarak kullanılacak: {curr_cols}")
    else:
        print("[graybox_rf] CURR kolonları yok — motor akımı özellik vektörüne eklenmeyecek")
    if brake_sig_col:
        print(f"[graybox_rf] BRAKE_SIG kolonu: {brake_sig_col}")
    else:
        print("[graybox_rf] UYARI: BRAKE_SIG kolonu bulunamadı — filtre uygulanmıyor")

    # ── 2. Producer scale ─────────────────────────────────────────────────────
    scale = 1.0
    p99 = 0.0
    if not args.no_producer_scale:
        scale, p99 = calibrate_scale(rows, torque_cols_6)
        print(f"[graybox_rf] Producer scale={scale:.4f}  p99(diff)={p99:.4f}")
    else:
        print("[graybox_rf] Producer scale: kapalı (ham tork kullanılıyor)")

    # ── 3. Zaman dizisi ve dt ─────────────────────────────────────────────────
    t_arr = _parse_timestamps(rows)
    dt = float(np.median(np.diff(t_arr)))
    print(f"[graybox_rf] Medyan dt={dt:.6f} s")

    # ── 4. AXIS_ACT → q matrisi ───────────────────────────────────────────────
    print("[graybox_rf] AXIS_ACT ayrıştırılıyor ...")
    q_all = np.zeros((len(rows), 6), dtype=float)
    valid_axis = np.ones(len(rows), dtype=bool)
    for i, r in enumerate(rows):
        raw = r.get(axis_act_col, "") or ""
        parsed = _parse_axis_act(raw)
        if parsed is None:
            valid_axis[i] = False
        else:
            q_all[i] = parsed

    bad = np.sum(~valid_axis)
    if bad > 0:
        print(f"[graybox_rf] UYARI: {bad} satırda AXIS_ACT ayrıştırılamadı → sıfır kullanıldı")

    # ── 5. SG filtre + hız + ivme ──────────────────────────────────────────────
    q_smooth = savgol_filter(q_all, window_length=7, polyorder=2, axis=0)

    if vel_cols is not None:
        # Ölçülmüş hız: deg/s → rad/s
        print("[graybox_rf] VEL_AXIS_ACT kullanılıyor (q_dot doğrudan ölçüm, q_ddot tek türev) ...")
        vel_raw = np.zeros((len(rows), 6), dtype=float)
        for j, col in enumerate(vel_cols):
            for i, r in enumerate(rows):
                v = _to_float(r.get(col))
                vel_raw[i, j] = v if v is not None else 0.0
        vel_rad = vel_raw * (np.pi / 180.0)
        q_dot_all  = savgol_filter(vel_rad, window_length=7, polyorder=2, axis=0)
        q_ddot_all = np.gradient(q_dot_all, dt, axis=0)
    else:
        print("[graybox_rf] SG filtre + çift türev (q_dot ve q_ddot) ...")
        q_dot_all  = np.gradient(q_smooth, dt, axis=0)
        q_ddot_all = np.gradient(q_dot_all, dt, axis=0)

    # ── 6. BRAKE_SIG filtresi ─────────────────────────────────────────────────
    if brake_sig_col and not args.no_brake_filter:
        brake_vals = np.array([_to_float(r.get(brake_sig_col)) for r in rows])
        valid_brake = (brake_vals == 63)
        n_valid = int(np.sum(valid_brake))
        print(f"[graybox_rf] BRAKE_SIG==63 filtresi: {n_valid}/{len(rows)} satır geçerli")
    else:
        valid_brake = np.ones(len(rows), dtype=bool)
        print("[graybox_rf] BRAKE_SIG filtresi: uygulanmıyor")

    idx_valid = np.where(valid_brake)[0]

    q_valid      = q_smooth[idx_valid]
    q_dot_valid  = q_dot_all[idx_valid]
    q_ddot_valid = q_ddot_all[idx_valid]

    # ── 7. Tork matrisi (6 eksen) ─────────────────────────────────────────────
    print("[graybox_rf] Tork verisi hazırlanıyor (6 eksen) ...")
    tau_valid = np.zeros((len(idx_valid), 6), dtype=float)
    for j, col in enumerate(torque_cols_6):
        for ii, row_idx in enumerate(idx_valid):
            v = _to_float(rows[row_idx].get(col))
            tau_valid[ii, j] = v if v is not None else 0.0

    if scale != 1.0:
        tau_valid = tau_valid / scale

    # ── 8. Motor akımı matrisi (CURR) ────────────────────────────────────────
    curr_valid = None
    if curr_cols is not None:
        print("[graybox_rf] CURR verisi hazırlanıyor (6 eksen) ...")
        curr_valid = np.zeros((len(idx_valid), 6), dtype=float)
        for j, col in enumerate(curr_cols):
            for ii, row_idx in enumerate(idx_valid):
                v = _to_float(rows[row_idx].get(col))
                curr_valid[ii, j] = v if v is not None else 0.0

    # ── 9(eski 8). Ters dinamik ──────────────────────────────────────────────
    use_inverse_dynamics = not args.no_inverse_dynamics
    tau_model_valid = None
    urdf_path_used = None

    if use_inverse_dynamics:
        if args.urdf:
            urdf_candidate = Path(args.urdf)
        else:
            script_dir = Path(__file__).resolve().parents[2]
            urdf_candidate = script_dir / "robot.urdf"

        if not urdf_candidate.exists():
            print(
                f"[graybox_rf] UYARI: URDF bulunamadı ({urdf_candidate})\n"
                f"  --urdf bayrağı ile yolu belirtin ya da robot.urdf'yi mindtwin/ klasörüne koyun.\n"
                f"  Ters dinamik olmadan devam ediliyor (12 özellik)."
            )
            use_inverse_dynamics = False
        else:
            urdf_path_used = str(urdf_candidate.resolve())
            print(f"[graybox_rf] URDF yükleniyor: {urdf_path_used}")
            try:
                from urdf_dynamics import compute_inverse_dynamics_batch
            except ImportError:
                stats_dir = Path(__file__).resolve().parent
                sys.path.insert(0, str(stats_dir))
                from urdf_dynamics import compute_inverse_dynamics_batch

            print(f"[graybox_rf] Ters dinamik hesaplanıyor ({len(idx_valid)} satır) ...")
            tau_model_valid = compute_inverse_dynamics_batch(
                urdf_path_used, q_valid, q_dot_valid, q_ddot_valid
            )
            print("[graybox_rf] Ters dinamik tamamlandı.")

    # ── 10. Özellik matrisi ───────────────────────────────────────────────────
    if curr_valid is not None:
        X_base = np.hstack([q_valid, q_dot_valid, curr_valid])  # (N, 18)
    else:
        X_base = np.hstack([q_valid, q_dot_valid])              # (N, 12)
    base_dim = X_base.shape[1]  # 12 or 18
    feature_dim = base_dim if (tau_model_valid is None) else base_dim + 1
    print(f"[graybox_rf] Temel özellik matrisi: {X_base.shape}  (toplam özellik/model: {feature_dim})")

    # ── 11. 6 eksen için RF eğitimi ──────────────────────────────────────────
    models = []
    thresholds = []

    for j in range(6):
        ax = j + 1

        if tau_model_valid is not None:
            X = np.hstack([X_base, tau_model_valid[:, j:j+1]])  # (N, 13)
        else:
            X = X_base  # (N, 12)

        Y = tau_valid[:, j]

        # 80/20 train/val split — eşik validation artıklarından hesaplanır
        X_train, X_val, Y_train, Y_val = train_test_split(
            X, Y, test_size=args.val_size, random_state=args.random_state
        )

        print(f"[graybox_rf] A{ax} eğitiliyor (n_estimators={args.n_estimators}, "
              f"train={len(X_train)}, val={len(X_val)}) ...")
        mdl = BaggingRegressor(
            estimator=DecisionTreeRegressor(),
            n_estimators=args.n_estimators,
            random_state=args.random_state,
            n_jobs=-1,
        )
        mdl.fit(X_train, Y_train)
        models.append(mdl)

        # Eşik: validation setinde mutlak artık (signed değil) — mean + sigma*std
        Y_val_pred = mdl.predict(X_val)
        residuals  = np.abs(Y_val - Y_val_pred)
        thr = float(np.mean(residuals) + args.sigma * np.std(residuals))
        thresholds.append(thr)

        print(
            f"[graybox_rf] A{ax}: eşik(val)={thr:.6f}  "
            f"(mean_abs={np.mean(residuals):.4f}  std={np.std(residuals):.4f})"
        )

    # ── 12. Artifact kaydet ──────────────────────────────────────────────────
    os.makedirs(args.output_dir, exist_ok=True)

    model_path = os.path.join(args.output_dir, "graybox_rf_models.pkl")
    thr_path   = os.path.join(args.output_dir, "graybox_rf_thresholds.json")

    joblib.dump({"models": models}, model_path)

    base_features = ["q1","q2","q3","q4","q5","q6","qd1","qd2","qd3","qd4","qd5","qd6"]
    if curr_valid is not None:
        base_features = base_features + ["curr1","curr2","curr3","curr4","curr5","curr6"]
    features_per_axis = {
        f"A{j+1}": base_features + [f"tau_model_A{j+1}"] if use_inverse_dynamics else base_features
        for j in range(6)
    }

    meta = {
        "thresholds": thresholds,
        "sigma_mult": float(args.sigma),
        "val_size": float(args.val_size),
        "n_estimators": args.n_estimators,
        "producer_scale": float(scale),
        "producer_scale_p99": float(p99),
        "dt_median": dt,
        "training_samples": int(X_base.shape[0]),
        "feature_dim": feature_dim,
        "inverse_dynamics_used": use_inverse_dynamics,
        "vel_axis_act_used": vel_cols is not None,
        "curr_used": curr_valid is not None,
        "urdf_path": urdf_path_used,
        "torque_axes": [f"A{j+1}" for j in range(6)],
        "features_per_axis": features_per_axis,
        "csv": str(args.csv),
    }

    with open(thr_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print("[graybox_rf] ── Eğitim tamamlandı ──────────────────────────────────")
    for j in range(6):
        print(f"[graybox_rf] A{j+1}: eşik={thresholds[j]:.4f}")
    print(f"[graybox_rf] Model  : {model_path}")
    print(f"[graybox_rf] Eşikler: {thr_path}")
    if vel_cols:
        print("[graybox_rf] q_dot kaynağı: VEL_AXIS_ACT (ölçülmüş)")
    else:
        print("[graybox_rf] q_dot kaynağı: AXIS_ACT türevi")
    if curr_valid is not None:
        print("[graybox_rf] CURR özelliği: aktif (18-dim base_x)")
    else:
        print("[graybox_rf] CURR özelliği: yok (12-dim base_x)")


if __name__ == "__main__":
    main()
