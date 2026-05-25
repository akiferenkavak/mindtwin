from step3_autoencoder import train_autoencoder
from step4_single_feature_anomaly_detection import run_anomaly_detection


def main():
    # Train
    model, scaler, feat_cols, fixed_threshold, single_feature_threshold = train_autoencoder(
        batch_size=16,
        epochs=40
    )

    # Single feature anomaly detection
    result_df, anomaly_idx = run_anomaly_detection(
        model,
        scaler,
        feat_cols,
        single_feature_threshold
    )

    print("\n================ FINAL SONUC ================\n")
    print("Toplam anomaly sayisi:", len(anomaly_idx))
    print("Single feature threshold:", single_feature_threshold)

    if len(anomaly_idx) > 0:
        print("\nTUM ANOMALYLER:")
        for i in anomaly_idx:
            ts = result_df["timestamp"].iloc[i]
            feat = result_df["top_error_feature"].iloc[i]
            comment = result_df["root_cause_text"].iloc[i]
            err = result_df["max_feature_error"].iloc[i]
            distance = result_df["distance_from_threshold"].iloc[i]
            ratio = result_df["severity_ratio"].iloc[i]

            print(
                f"{ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} | "
                f"{feat} | {comment} | "
                f"feature_error={err:.6f} | "
                f"distance={distance:.6f} | "
                f"severity_ratio={ratio:.3f}"
            )
    else:
        print("Anomaly bulunamadi.")


if __name__ == "__main__":
    main()