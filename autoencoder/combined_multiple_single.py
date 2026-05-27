import numpy as np
import pandas as pd

from step4_anomoly_detection import load_test_data, prepare_test_features
from step4_single_feature_anomaly_detection import feature_to_text


def run_combined_anomaly_detection(model, scaler, feat_cols, fixed_threshold):
    print("COMBINED ANOMALY DETECTION baslatildi")

    full_df = load_test_data()
    X_valid, X_scaled, valid_idx = prepare_test_features(full_df, feat_cols, scaler)

    X_pred = model.predict(X_scaled, verbose=0)

    # 1) General anomaly detection
    error_matrix = (X_scaled - X_pred) ** 2
    recon_error_valid = np.mean(error_matrix, axis=1)

    anomaly_valid_pos = np.where(recon_error_valid > fixed_threshold)[0]
    anomaly_full_idx = valid_idx[anomaly_valid_pos]

    # 2) Single feature/root cause detection
    max_feature_error = np.max(error_matrix, axis=1)
    top_feature_idx = np.argmax(error_matrix, axis=1)
    top_feature_names = [feat_cols[i] for i in top_feature_idx]

    signed_diff = X_scaled - X_pred
    top_feature_signed_diff = signed_diff[np.arange(len(signed_diff)), top_feature_idx]

    result_df = full_df.copy()

    result_df["reconstruction_error"] = np.nan
    result_df["max_feature_error"] = np.nan
    result_df["is_anomaly"] = False
    result_df["top_error_feature"] = ""
    result_df["root_cause_text"] = ""

    result_df.loc[valid_idx, "reconstruction_error"] = recon_error_valid
    result_df.loc[valid_idx, "max_feature_error"] = max_feature_error
    result_df.loc[valid_idx, "top_error_feature"] = top_feature_names

    result_df.loc[anomaly_full_idx, "is_anomaly"] = True

    for valid_pos, full_i in zip(anomaly_valid_pos, anomaly_full_idx):
        feat = top_feature_names[valid_pos]
        diff = top_feature_signed_diff[valid_pos]

        direction = "high" if diff > 0 else "low"
        comment = feature_to_text(feat, direction)

        result_df.at[full_i, "root_cause_text"] = comment

    print("Toplam anomaly sayisi:", len(anomaly_full_idx))

    return result_df, anomaly_full_idx.tolist()