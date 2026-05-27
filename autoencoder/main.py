from step3_autoencoder import train_autoencoder

from step4_anomoly_detection import run_anomaly_detection as run_general_anomaly_detection
from step4_single_feature_anomaly_detection import run_anomaly_detection as run_single_feature_anomaly_detection


def main():
    print("\n================ STEP 1: TRAIN AUTOENCODER ================\n")

    model, scaler, feat_cols, fixed_threshold, single_feature_threshold = train_autoencoder(
        batch_size=16,
        epochs=40
    )

    print("\nTrain tamamlandi.")
    print("Feature sayisi:", len(feat_cols))
    print("General fixed threshold:", fixed_threshold)
    print("Single feature threshold:", single_feature_threshold)

    print("\n================ STEP 2: GENERAL / MULTIPLE FEATURE ANOMALY DETECTION ================\n")

    general_result_df, general_anomaly_idx = run_general_anomaly_detection(
        model,
        scaler,
        feat_cols,
        fixed_threshold
    )

    print("\n================ GENERAL RESULT ================\n")
    print("Toplam general anomaly sayisi:", len(general_anomaly_idx))
    print("General fixed threshold:", fixed_threshold)

    if len(general_anomaly_idx) > 0:
        print("\nIlk general anomaly zamanlari:")
        for i in general_anomaly_idx[:20]:
            ts = general_result_df["timestamp"].iloc[i]
            error = general_result_df["reconstruction_error"].iloc[i]

            root_1 = general_result_df["root_cause_1"].iloc[i] if "root_cause_1" in general_result_df.columns else ""
            root_2 = general_result_df["root_cause_2"].iloc[i] if "root_cause_2" in general_result_df.columns else ""
            root_3 = general_result_df["root_cause_3"].iloc[i] if "root_cause_3" in general_result_df.columns else ""

            print(
                ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "| reconstruction error:",
                round(error, 6),
                "| root causes:",
                root_1,
                root_2,
                root_3
            )
    else:
        print("General anomaly bulunamadi.")

    print("\n================ STEP 3: SINGLE FEATURE ANOMALY DETECTION ================\n")

    single_result_df, single_anomaly_idx = run_single_feature_anomaly_detection(
        model,
        scaler,
        feat_cols,
        single_feature_threshold
    )

    print("\n================ SINGLE FEATURE RESULT ================\n")
    print("Toplam single feature anomaly sayisi:", len(single_anomaly_idx))
    print("Single feature threshold:", single_feature_threshold)

    if len(single_anomaly_idx) > 0:
        print("\nIlk single feature anomaly zamanlari:")
        for i in single_anomaly_idx[:20]:
            ts = single_result_df["timestamp"].iloc[i]
            max_error = single_result_df["max_feature_error"].iloc[i]
            top_feature = single_result_df["top_error_feature"].iloc[i]
            root_cause = single_result_df["root_cause_text"].iloc[i]

            distance = single_result_df["distance_from_threshold"].iloc[i] if "distance_from_threshold" in single_result_df.columns else max_error - single_feature_threshold
            ratio = single_result_df["severity_ratio"].iloc[i] if "severity_ratio" in single_result_df.columns else max_error / single_feature_threshold

            print(
                ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "| max feature error:",
                round(max_error, 6),
                "| threshold distance:",
                round(distance, 6),
                "| severity ratio:",
                round(ratio, 3),
                "| feature:",
                top_feature,
                "| root cause:",
                root_cause
            )
    else:
        print("Single feature anomaly bulunamadi.")

    print("\n================ FINAL SUMMARY ================\n")
    print("General anomaly count       :", len(general_anomaly_idx))
    print("Single feature anomaly count:", len(single_anomaly_idx))
    print("General threshold           :", fixed_threshold)
    print("Single feature threshold    :", single_feature_threshold)
    print("Islem tamamlandi.")


if __name__ == "__main__":
    main()