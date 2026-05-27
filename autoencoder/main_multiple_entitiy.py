from step3_autoencoder import train_autoencoder
from step4_anomoly_detection import run_anomaly_detection


def main():
    # 1) Train
    model, scaler, feat_cols, fixed_threshold = train_autoencoder(batch_size=16, epochs=40)

    # 2) Test data uzerinde anomaly detection
    result_df, anomaly_idx = run_anomaly_detection(model, scaler, feat_cols, fixed_threshold)

    print("\n================ FINAL SONUC ================\n")
    print("Toplam anomaly sayisi:", len(anomaly_idx))

    if len(anomaly_idx) > 0:
        print("\nIlk anomaly zamanlari:")
        for i in anomaly_idx[:20]:
            ts = result_df["timestamp"].iloc[i]
            print(ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
    else:
        print("Anomaly bulunamadi.")


if __name__ == "__main__":
    main()