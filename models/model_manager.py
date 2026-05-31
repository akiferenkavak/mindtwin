import os
from typing import Optional

try:
    from .stats.iforest_detector import IForestDetector
    from .stats.pca_detector import PCADetector
    from .stats.graybox_rf_detector import GrayboxRFDetector
except ImportError:  # script execution fallback
    from stats.iforest_detector import IForestDetector
    from stats.pca_detector import PCADetector
    from stats.graybox_rf_detector import GrayboxRFDetector


class TorqueModelManager:
    def __init__(
        self,
        pca: Optional[PCADetector],
        iforest: Optional[IForestDetector],
        graybox_rf: Optional[GrayboxRFDetector] = None,
    ) -> None:
        self.pca = pca
        self.iforest = iforest
        self.graybox_rf = graybox_rf

    @classmethod
    def load(cls, artifacts_dir: str = "artifacts") -> "TorqueModelManager":
        pca = None
        iforest = None
        graybox_rf = None

        pca_model = os.path.join(artifacts_dir, "pca_model.pkl")
        pca_scaler = os.path.join(artifacts_dir, "pca_scaler.pkl")
        pca_thr = os.path.join(artifacts_dir, "pca_thresholds.json")
        if os.path.exists(pca_model) and os.path.exists(pca_scaler) and os.path.exists(pca_thr):
            pca = PCADetector.load(pca_model, pca_scaler, pca_thr)

        iforest_model = os.path.join(artifacts_dir, "iforest.pkl")
        iforest_thr = os.path.join(artifacts_dir, "iforest_threshold.json")
        if os.path.exists(iforest_model) and os.path.exists(iforest_thr):
            iforest = IForestDetector.load(iforest_model, iforest_thr)

        rf_model = os.path.join(artifacts_dir, "graybox_rf_models.pkl")
        rf_thr = os.path.join(artifacts_dir, "graybox_rf_thresholds.json")
        if os.path.exists(rf_model) and os.path.exists(rf_thr):
            graybox_rf = GrayboxRFDetector.load(rf_model, rf_thr)

        return cls(pca=pca, iforest=iforest, graybox_rf=graybox_rf)

    def enabled(self) -> bool:
        return self.pca is not None or self.iforest is not None or self.graybox_rf is not None

    def score(
        self,
        x,
        q: Optional[list] = None,
        q_dot: Optional[list] = None,
        q_ddot: Optional[list] = None,
        curr: Optional[list] = None,
    ) -> dict:
        """
        x      : torque_actual (producer-scaled, 6 values)
        q      : joint angles in radians (6 values) — required for graybox_rf
        q_dot  : joint velocities in rad/s (6 values) — required for graybox_rf
        q_ddot : joint accelerations in rad/s² (6 values) — required for inverse dynamics
        curr   : motor currents A1..A6 — optional, used when model was trained with CURR feature
        """
        payload: dict = {
            "pca_score": None,
            "pca_threshold": None,
            "pca_anomaly": False,
            "iforest_score": None,
            "iforest_threshold": None,
            "iforest_anomaly": False,
            "rf_anomaly": False,
            "rf_worst_joint": None,
            "rf_max_residual": 0.0,
            "rf_scores": {},
            "rf_thresholds": {},
            "rf_anomaly_joints": [],
            "model_anomaly": False,
        }

        if self.pca is not None:
            p_score = self.pca.score(x)
            payload["pca_score"] = p_score
            payload["pca_threshold"] = self.pca.threshold
            payload["pca_anomaly"] = p_score >= self.pca.threshold

        if self.iforest is not None:
            i_score = self.iforest.score(x)
            payload["iforest_score"] = i_score
            payload["iforest_threshold"] = self.iforest.threshold
            payload["iforest_anomaly"] = i_score >= self.iforest.threshold

        if self.graybox_rf is not None and q is not None and q_dot is not None:
            try:
                rf_result = self.graybox_rf.score(q, q_dot, x, q_ddot=q_ddot, curr=curr)
                payload.update(rf_result)
            except Exception as e:
                print(f"[model_manager] graybox_rf scoring error: {e}")

        payload["model_anomaly"] = bool(
            payload["pca_anomaly"]
            or payload["iforest_anomaly"]
            or payload["rf_anomaly"]
        )
        return payload

    def reconstruct(self, x):
        if self.pca is None:
            return None
        return self.pca.reconstruct(x)
