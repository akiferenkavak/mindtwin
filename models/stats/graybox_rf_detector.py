import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import joblib
import numpy as np


@dataclass
class GrayboxRFDetector:
    """
    Grey-box Random Forest detector — Python port of MATLAB Egit_ve_Kaydet.m

    Özellik vektörü (ters dinamik aktifse):
        [q(6), q_dot(6), tau_model_j(1)]  = 13 boyut  (MATLAB ile birebir aynı)

    Ters dinamik kapalıysa (--no-inverse-dynamics ile eğitilmişse):
        [q(6), q_dot(6)]                  = 12 boyut

    Loaded from artifacts/graybox_rf_models.pkl + artifacts/graybox_rf_thresholds.json
    """

    models: list
    thresholds: list
    producer_scale: float
    feature_dim: int
    inverse_dynamics_used: bool
    curr_used: bool
    urdf_path: Optional[str]

    # ── yükleme ──────────────────────────────────────────────────────────────

    @classmethod
    def load(cls, model_path: str, threshold_path: str) -> "GrayboxRFDetector":
        bundle = joblib.load(model_path)
        models = bundle.get("models") or []

        # Avoid loky/multiprocessing issues (especially in sandboxed envs)
        # by forcing single-threaded prediction.
        for mdl in models:
            if hasattr(mdl, "n_jobs"):
                try:
                    mdl.n_jobs = 1
                except Exception:
                    pass

        with open(threshold_path, "r", encoding="utf-8") as f:
            meta = json.load(f)

        use_inv_dyn = bool(meta.get("inverse_dynamics_used", False))
        curr_used   = bool(meta.get("curr_used", False))
        urdf_path   = meta.get("urdf_path")

        if use_inv_dyn:
            # URDF yolunu doğrula; bulunamazsa projede ara ve otomatik düzelt
            if urdf_path and not Path(urdf_path).exists():
                proj_urdf = Path(threshold_path).resolve().parent.parent / "robot.urdf"
                if proj_urdf.exists():
                    urdf_path = str(proj_urdf)
                    print(f"[graybox_rf] URDF otomatik düzeltildi: {urdf_path}")
                else:
                    cwd_urdf = Path("robot.urdf").resolve()
                    if cwd_urdf.exists():
                        urdf_path = str(cwd_urdf)
                        print(f"[graybox_rf] URDF otomatik düzeltildi (CWD): {urdf_path}")
                    else:
                        print(
                            f"[graybox_rf] UYARI: URDF bulunamadı ({urdf_path})\n"
                            "  Model ters dinamiksiz (12-özellik) modunda çalışacak.\n"
                            "  Doğru yolu artifacts/graybox_rf_thresholds.json'da güncelleyin."
                        )
                        use_inv_dyn = False

        return cls(
            models=models,
            thresholds=meta["thresholds"],
            producer_scale=float(meta.get("producer_scale", 1.0)),
            feature_dim=int(meta.get("feature_dim", 12)),
            inverse_dynamics_used=use_inv_dyn,
            curr_used=curr_used,
            urdf_path=urdf_path,
        )

    # ── çıkarım ──────────────────────────────────────────────────────────────

    def score(
        self,
        q: list,
        q_dot: list,
        torque_actual: list,
        q_ddot: Optional[list] = None,
        curr: Optional[list] = None,
    ) -> dict:
        """
        q             : 6 eklem açısı (rad)
        q_dot         : 6 eklem hızı (rad/s)
        torque_actual : 6 tork değeri (producer-scale normalise)
        q_ddot        : 6 eklem ivmesi (rad/s²) — ters dinamik için gerekli

        MATLAB karşılığı:
            tau_model = inverseDynamics(robot, q, q_dot, q_ddot)
            X = [q, q_dot, tau_model(j)]
            tau_pred = predict(model_j, X)
            residual = abs(tau_real - tau_pred)
            anomaly = residual > threshold_j
        """
        q_arr   = np.asarray(q[:6],     dtype=float)
        qd_arr  = np.asarray(q_dot[:6], dtype=float)
        qdd_arr = (np.asarray(q_ddot[:6], dtype=float)
                   if q_ddot is not None else np.zeros(6))
        curr_arr = (np.asarray(curr[:6], dtype=float)
                    if (curr is not None and self.curr_used) else None)

        # Ters dinamik: fizik tabanlı tork tahmini
        tau_model = None
        if self.inverse_dynamics_used and self.urdf_path:
            try:
                stats_dir = Path(__file__).resolve().parent
                if str(stats_dir) not in sys.path:
                    sys.path.insert(0, str(stats_dir))
                from urdf_dynamics import compute_inverse_dynamics
                tau_model = compute_inverse_dynamics(
                    self.urdf_path, q_arr, qd_arr, qdd_arr
                )
            except Exception as e:
                print(f"[graybox_rf] ters dinamik hatasi: {e}")
                tau_model = None

        if curr_arr is not None:
            base_x = np.concatenate([q_arr, qd_arr, curr_arr])  # (18,)
        else:
            base_x = np.concatenate([q_arr, qd_arr])  # (12,)

        payload: dict = {
            "rf_anomaly": False,
            "rf_worst_joint": None,
            "rf_max_residual": 0.0,
            "rf_scores": {},
            "rf_thresholds": {},
            "rf_anomaly_joints": [],
        }

        anomaly_joints: list[int] = []
        max_res = 0.0
        worst_j: Optional[int] = None

        for i in range(len(self.models)):
            ax = i + 1

            # Özellik vektörü — ters dinamik varsa tau_model[i] eklenir
            if tau_model is not None:
                x = np.append(base_x, tau_model[i]).reshape(1, -1)   # (1, 13)
            else:
                x = base_x.reshape(1, -1)                             # (1, 12)

            tau_pred = float(self.models[i].predict(x)[0])
            tau_real = float(torque_actual[i]) if i < len(torque_actual) else 0.0
            residual = abs(tau_real - tau_pred)
            thr      = self.thresholds[i]

            payload["rf_scores"][f"A{ax}"]     = round(residual, 4)
            payload["rf_thresholds"][f"A{ax}"] = round(thr, 4)

            if residual > thr:
                anomaly_joints.append(ax)
            if residual > max_res:
                max_res = residual
                worst_j = ax

        payload["rf_anomaly"]        = len(anomaly_joints) > 0
        payload["rf_worst_joint"]    = worst_j
        payload["rf_max_residual"]   = round(max_res, 4)
        payload["rf_anomaly_joints"] = anomaly_joints
        return payload
