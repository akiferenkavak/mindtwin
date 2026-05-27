import json
from dataclasses import dataclass
from typing import Optional

import joblib
import numpy as np


@dataclass
class IForestDetector:
    model: object
    scaler: Optional[object]
    threshold: float
    meta: dict

    @classmethod
    def load(cls, model_path: str, threshold_path: str) -> "IForestDetector":
        payload = joblib.load(model_path)
        if isinstance(payload, dict) and "model" in payload:
            model = payload["model"]
            scaler = payload.get("scaler")
        else:
            model = payload
            scaler = None

        with open(threshold_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        threshold = float(meta.get("score_threshold", meta.get("threshold")))
        return cls(model=model, scaler=scaler, threshold=threshold, meta=meta)

    def score(self, x) -> float:
        arr = np.asarray(x, dtype=float).reshape(1, -1)
        if self.scaler is not None:
            arr = self.scaler.transform(arr)
        score = float(-self.model.score_samples(arr)[0])
        return score

    def is_anomaly(self, x) -> bool:
        return self.score(x) >= self.threshold
