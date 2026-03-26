import json
from dataclasses import dataclass

import joblib
import numpy as np

@dataclass
class PCADetector:
    model: object
    scaler: object
    threshold: float
    meta: dict

    @classmethod
    def load(cls, model_path: str, scaler_path: str, threshold_path: str) -> "PCADetector":
        model = joblib.load(model_path)
        scaler = joblib.load(scaler_path)
        with open(threshold_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        threshold = float(meta.get("recon_error_threshold", meta.get("threshold")))
        return cls(model=model, scaler=scaler, threshold=threshold, meta=meta)

    def score(self, x) -> float:
        arr = np.asarray(x, dtype=float).reshape(1, -1)
        xs = self.scaler.transform(arr)
        xp = self.model.transform(xs)
        xr = self.model.inverse_transform(xp)
        err = float(np.mean((xs - xr) ** 2))
        return err

    def reconstruct(self, x):
        arr = np.asarray(x, dtype=float).reshape(1, -1)
        xs = self.scaler.transform(arr)
        xp = self.model.transform(xs)
        xr = self.model.inverse_transform(xp)
        x_rec = self.scaler.inverse_transform(xr)
        return x_rec.reshape(-1).tolist()

    def is_anomaly(self, x) -> bool:
        return self.score(x) >= self.threshold
