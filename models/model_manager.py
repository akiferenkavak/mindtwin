import os
from typing import Optional

try:
    from .stats.iforest_detector import IForestDetector
    from .stats.pca_detector import PCADetector
except ImportError:  # script execution fallback
    from stats.iforest_detector import IForestDetector
    from stats.pca_detector import PCADetector


class TorqueModelManager:
    def __init__(self, pca: Optional[PCADetector], iforest: Optional[IForestDetector]) -> None:
        self.pca = pca
        self.iforest = iforest

    @classmethod
    def load(cls, artifacts_dir: str = "artifacts") -> "TorqueModelManager":
        pca = None
        iforest = None

        pca_model = os.path.join(artifacts_dir, "pca_model.pkl")
        pca_scaler = os.path.join(artifacts_dir, "pca_scaler.pkl")
        pca_thr = os.path.join(artifacts_dir, "pca_thresholds.json")
        if os.path.exists(pca_model) and os.path.exists(pca_scaler) and os.path.exists(pca_thr):
            pca = PCADetector.load(pca_model, pca_scaler, pca_thr)

        iforest_model = os.path.join(artifacts_dir, "iforest.pkl")
        iforest_thr = os.path.join(artifacts_dir, "iforest_threshold.json")
        if os.path.exists(iforest_model) and os.path.exists(iforest_thr):
            iforest = IForestDetector.load(iforest_model, iforest_thr)

        return cls(pca=pca, iforest=iforest)

    def enabled(self) -> bool:
        return self.pca is not None or self.iforest is not None

    def score(self, x) -> dict:
        payload: dict = {
            "pca_score": None,
            "pca_threshold": None,
            "pca_anomaly": False,
            "iforest_score": None,
            "iforest_threshold": None,
            "iforest_anomaly": False,
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

        payload["model_anomaly"] = bool(payload["pca_anomaly"] or payload["iforest_anomaly"])
        return payload

    def reconstruct(self, x):
        if self.pca is None:
            return None
        return self.pca.reconstruct(x)
