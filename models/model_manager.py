import os
import sys
import json
import subprocess
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

        if os.path.exists(rf_thr) and not os.path.exists(rf_model):
            try:
                with open(rf_thr, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                project_dir = os.path.abspath(os.path.join(artifacts_dir, os.pardir))
                csv_path = meta.get("csv") or os.path.join(project_dir, "autoencoder", "Data_for_Train", "kuka_log_900scnd_100hz(=5,8hz).csv")
                urdf_path = meta.get("urdf_path") or os.path.join(project_dir, "robot.urdf")
                if not os.path.isabs(csv_path):
                    csv_path = os.path.join(project_dir, csv_path)
                if not os.path.isabs(urdf_path):
                    urdf_path = os.path.join(project_dir, urdf_path)
                train_script = os.path.join(project_dir, "models", "stats", "train_graybox_rf.py")
                if os.path.exists(train_script) and os.path.exists(csv_path) and os.path.exists(urdf_path):
                    print("[model_manager] graybox_rf_models.pkl missing; training grey-box RF ...")
                    subprocess.run(
                        [sys.executable, train_script, "--csv", csv_path, "--urdf", urdf_path, "--output-dir", artifacts_dir],
                        check=True,
                    )
            except Exception as e:
                print(f"[model_manager] graybox RF auto-train skipped: {e}")

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
            payload["pca_anomaly"] or payload["iforest_anomaly"] or payload["rf_anomaly"]
        )
        return payload

    def reconstruct(self, x):
        if self.pca is None:
            return None
        return self.pca.reconstruct(x)
