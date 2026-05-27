import matplotlib
matplotlib.use("TkAgg")

import logging
import numpy as np
import matplotlib.pyplot as plt
from tensorflow.keras import layers, models

from step2_prepare_features import prepare_features


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)


def build_autoencoder(input_dim):
    model = models.Sequential([
        layers.Input(shape=(input_dim,)),
        layers.Dense(16, activation="relu"),
        layers.Dense(8, activation="relu"),
        layers.Dense(16, activation="relu"),
        layers.Dense(input_dim, activation="linear")
    ])

    model.compile(
        optimizer="adam",
        loss="mse"
    )

    return model


def train_autoencoder(batch_size=16, epochs=40):
    logging.info("STEP-3 TRAIN baslatildi")

    df_all, X, X_scaled, scaler, feat_cols = prepare_features()

    input_dim = X_scaled.shape[1]
    model = build_autoencoder(input_dim)

    logging.info(
        f"Training basliyor | shape={X_scaled.shape} | "
        f"epochs={epochs} | batch_size={batch_size}"
    )

    history = model.fit(
        X_scaled,
        X_scaled,
        epochs=epochs,
        batch_size=batch_size,
        shuffle=False,
        verbose=1
    )

    logging.info("Training tamamlandi")

    # =====================================
    # TRAIN ERROR ve SABIT THRESHOLDLAR
    # =====================================

    train_pred = model.predict(X_scaled, verbose=0)

    train_error_matrix = (X_scaled - train_pred) ** 2

    # General / multiple anomaly threshold
    # Her satirdaki tum feature hatalarinin ortalamasi
    train_reconstruction_error = np.mean(train_error_matrix, axis=1)

    # Single feature anomaly threshold
    # Her satirdaki en buyuk feature hatasi
    train_max_feature_error = np.max(train_error_matrix, axis=1)

    # Thresholdlar train datasindan hesaplanir
    fixed_threshold = np.percentile(train_reconstruction_error, 99)
    single_feature_threshold = np.percentile(train_max_feature_error, 99)

    logging.info(f"Fixed reconstruction threshold hesaplandi: {fixed_threshold:.6f}")
    logging.info(f"Single feature threshold hesaplandi: {single_feature_threshold:.6f}")

    logging.info(f"Train reconstruction max error: {np.max(train_reconstruction_error):.6f}")
    logging.info(f"Train max feature error: {np.max(train_max_feature_error):.6f}")

    # =====================================
    # LOSS GRAFIGI
    # =====================================

    fig, ax = plt.subplots(figsize=(8, 4))

    ax.plot(
        history.history["loss"],
        linewidth=2,
        label="Train Loss"
    )

    ax.set_title("Autoencoder Training Loss")
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Loss")
    ax.grid(True)
    ax.legend()

    plt.tight_layout()
    plt.show(block=True)

    return model, scaler, feat_cols, fixed_threshold, single_feature_threshold


if __name__ == "__main__":
    train_autoencoder(batch_size=16, epochs=40)