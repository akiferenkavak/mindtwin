import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Dosya Data_For_Test icindeyse bunu kullan
TEST_DATA_DIR = os.path.join(BASE_DIR, "Data_For_Test")

# Eger dosya direkt proje klasorundeyse, ustteki satiri yorumlayip bunu ac:
# TEST_DATA_DIR = BASE_DIR

TIMESTAMP_COL = "timestamp"
SEP = ";"

TEST_FILES = [
    "kuka_log_900scnd_injected.csv",
]


def load_test_data():
    all_dfs = []

    print("BASE_DIR:", BASE_DIR)
    print("TEST_DATA_DIR:", TEST_DATA_DIR)

    if not os.path.exists(TEST_DATA_DIR):
        raise RuntimeError(f"Test klasoru bulunamadi: {TEST_DATA_DIR}")

    print("Klasordeki dosyalar:", os.listdir(TEST_DATA_DIR))
    print("TEST_FILES:", TEST_FILES)

    for fname in TEST_FILES:
        path = os.path.join(TEST_DATA_DIR, fname)
        print(f"\nKontrol ediliyor: {path}")

        if not os.path.exists(path):
            print(f"UYARI: Dosya bulunamadi -> {path}")
            continue

        try:
            df = pd.read_csv(path, sep=SEP, engine="python")
        except Exception as e:
            print(f"UYARI: Dosya okunamadi -> {fname} | Hata: {e}")
            continue

        print(f"Okundu: {fname}")
        print("Kolonlar:", df.columns.tolist())

        # "index,timestamp" bozuk kolon durumu
        if len(df.columns) > 0 and df.columns[0] == "index,timestamp":
            split_cols = df[df.columns[0]].astype(str).str.split(",", n=1, expand=True)
            df["index"] = split_cols[0]
            df["timestamp"] = split_cols[1]
            df = df.drop(columns=[df.columns[0]])
            print("index,timestamp ayrildi.")
            print("Yeni kolonlar:", df.columns.tolist())

        if TIMESTAMP_COL not in df.columns:
            print(f"UYARI: {fname} icinde timestamp kolonu yok")
            continue

        print("Ilk 5 ham timestamp:")
        print(df[TIMESTAMP_COL].head())

        df[TIMESTAMP_COL] = df[TIMESTAMP_COL].astype(str).str.strip()
        df[TIMESTAMP_COL] = pd.to_datetime(
            df[TIMESTAMP_COL],
            errors="coerce",
            format="mixed"
        )

        nat_count = df[TIMESTAMP_COL].isna().sum()
        print("Timestamp NaT sayisi:", nat_count)
        print("Toplam satir:", len(df))

        df = df.dropna(subset=[TIMESTAMP_COL]).copy()
        print("Timestamp temizleme sonrasi satir:", len(df))

        if len(df) == 0:
            print(
                f"UYARI: {fname} icindeki tum satirlar "
                "timestamp parse edilemedigi icin silindi."
            )
            continue

        df["source_file"] = fname
        all_dfs.append(df)

    if not all_dfs:
        raise RuntimeError(
            "Test verisi yuklenemedi. Yukaridaki loglarda dosya bulundu mu, "
            "timestamp kolonu var mi ve parse sonrasi satir kaldi mi kontrol et."
        )

    df_all = pd.concat(all_dfs, ignore_index=True)
    df_all = df_all.sort_values(TIMESTAMP_COL).reset_index(drop=True)

    print("\n====================")
    print("TEST VERISI")
    print("====================")
    print("Toplam satir:", len(df_all))
    print("Baslangic:", df_all[TIMESTAMP_COL].min())
    print("Bitis    :", df_all[TIMESTAMP_COL].max())

    return df_all


def prepare_test_features(df_all, feat_cols, scaler):
    print("\ndf_all shape:", df_all.shape)
    print("feat_cols:", feat_cols)
    print("df_all columns:", df_all.columns.tolist())

    missing_cols = [c for c in feat_cols if c not in df_all.columns]
    print("missing_cols:", missing_cols)

    if missing_cols:
        raise ValueError(f"Test verisinde eksik kolonlar var: {missing_cols}")

    X = df_all[feat_cols].copy()

    print("\nIlk 5 satir ham X:")
    print(X.head())

    for c in feat_cols:
        X[c] = pd.to_numeric(X[c], errors="coerce")

    print("\nHer kolondaki NaN sayisi:")
    print(X.isna().sum())

    valid_idx = X.dropna().index
    print("\nDropna sonrasi kalan satir:", len(valid_idx))

    X = X.loc[valid_idx].reset_index(drop=True)
    df_all = df_all.loc[valid_idx].reset_index(drop=True)

    if len(df_all) == 0:
        raise RuntimeError("load_test_data() bos dataframe dondurdu")

    if len(X) == 0:
        raise RuntimeError(
            "Tum satirlar NaN oldugu icin silindi. "
            "Test veri formati train ile uyusmuyor."
        )

    X_scaled = scaler.transform(X)

    print("\nTEST FEATURE OZETI")
    print("Toplam ornek sayisi:", X.shape[0])
    print("Feature sayisi:", X.shape[1])
    print("Scaled shape:", X_scaled.shape)

    return df_all, X, X_scaled


def feature_to_text(feature_name, direction):
    direction_text = "yuksek" if direction == "high" else "dusuk"

    if feature_name.startswith("TORQUE_A"):
        return f"{feature_name} normalden {direction_text}"

    if feature_name.startswith("CURR_A"):
        return f"{feature_name} normalden {direction_text}"

    if feature_name.startswith("MOT_TEMP_A"):
        return f"{feature_name} normalden {direction_text}"

    if feature_name.startswith("VEL_AXIS_ACT_A"):
        return f"{feature_name} normalden {direction_text}"

    return f"{feature_name} normalden {direction_text}"


def run_anomaly_detection(model, scaler, feat_cols, single_feature_threshold):
    print("STEP-4 SINGLE FEATURE ANOMALY DETECTION baslatildi")

    df_all = load_test_data()
    df_all, X, X_scaled = prepare_test_features(df_all, feat_cols, scaler)

    X_pred = model.predict(X_scaled, verbose=0)

    error_matrix = (X_scaled - X_pred) ** 2
    signed_diff = X_scaled - X_pred

    # Her timestamp icin en buyuk feature-level reconstruction error
    max_feature_error = np.max(error_matrix, axis=1)

    # Her timestamp icin en cok hata yapan feature
    top_feature_idx = np.argmax(error_matrix, axis=1)
    top_feature_names = [feat_cols[i] for i in top_feature_idx]

    # En cok hata yapan feature'in signed difference degeri
    # Pozitifse normalden yuksek, negatifse normalden dusuk yorumu yapacagiz
    top_feature_signed_diff = signed_diff[
        np.arange(len(signed_diff)),
        top_feature_idx
    ]

    # ONEMLI:
    # Threshold artik test datasindan hesaplanmiyor.
    # Data_For_Train uzerinden step3_autoencoder.py icinde hesaplanan
    # single_feature_threshold burada kullaniliyor.
    threshold_feature = single_feature_threshold

    anomaly_idx = np.where(max_feature_error > threshold_feature)[0]

    print("\n====================")
    print("SINGLE FEATURE ANOMALY DETECTION SONUCLARI")
    print("====================")
    print("Toplam test ornegi:", len(df_all))
    print("Anomaly sayisi:", len(anomaly_idx))
    print("Train-based single feature threshold:", threshold_feature)
    print("Max feature error min :", max_feature_error.min())
    print("Max feature error mean:", max_feature_error.mean())
    print("Max feature error max :", max_feature_error.max())

    result_df = df_all.copy()
    result_df["max_feature_error"] = max_feature_error
    result_df["single_feature_threshold"] = threshold_feature
    result_df["distance_from_threshold"] = max_feature_error - threshold_feature
    result_df["severity_ratio"] = max_feature_error / threshold_feature
    result_df["is_anomaly"] = False
    result_df["top_error_feature"] = top_feature_names
    result_df["top_feature_signed_diff"] = top_feature_signed_diff
    result_df["root_cause_text"] = ""

    result_df.loc[anomaly_idx, "is_anomaly"] = True

    print("\nTUM ANOMALYLER:")
    if len(anomaly_idx) == 0:
        print("Anomaly bulunamadi.")
    else:
        for i in anomaly_idx:
            ts = result_df[TIMESTAMP_COL].iloc[i]
            err = max_feature_error[i]
            distance = err - threshold_feature
            ratio = err / threshold_feature

            feat = top_feature_names[i]
            diff = top_feature_signed_diff[i]

            direction = "high" if diff > 0 else "low"
            comment = feature_to_text(feat, direction)
            result_df.at[i, "root_cause_text"] = comment

            print(f"\nANOMALY -> saat={ts.strftime('%H:%M:%S.%f')[:-3]} | error={err:.6f}")
            print(f"  Threshold distance: {distance:.6f}")
            print(f"  Severity ratio    : {ratio:.3f}")
            print(f"  1. {comment} | scaled diff={diff:.3f}")

    print("\nSonuclar sadece ekranda gosteriliyor, dosyaya kaydedilmiyor.")

    fig, ax = plt.subplots(figsize=(18, 7))

    ax.plot(
        df_all[TIMESTAMP_COL],
        max_feature_error,
        linewidth=1,
        label="Max Feature Error"
    )

    ax.axhline(
        threshold_feature,
        color="red",
        linestyle="--",
        label=f"Train-based Threshold = {threshold_feature:.6f}"
    )

    if len(anomaly_idx) > 0:
        ax.scatter(
            df_all[TIMESTAMP_COL].iloc[anomaly_idx],
            max_feature_error[anomaly_idx],
            s=20,
            label="Single Feature Anomalies"
        )

    ax.set_xlabel("Time")
    ax.set_ylabel("Max Single Feature Error")
    ax.set_title("Single Feature Anomaly Detection on Test Data")
    ax.grid(True)
    ax.legend(loc="upper left")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))

    fig.autofmt_xdate()
    plt.tight_layout()
    plt.show()

    return result_df, anomaly_idx


# ======================================================
# JSON EXPORT HELPER (added for frontend integration)
# All existing code above is unchanged.
# ======================================================

def export_single_to_json(result_df, anomaly_idx, feat_cols, threshold, output_path):
    """
    Export single-feature anomaly results to a JSON file.
    Includes **all timestamps for every feature**, enabling continuous charts even when a feature
    is not the top‑error feature. Anomaly flag is computed per‑feature using the same threshold
    as the original single‑feature detection.
    """
    import json
    from pathlib import Path

    TIMESTAMP_COL = "timestamp"

    per_feature = {f: [] for f in feat_cols}

    for idx, rec in result_df.iterrows():
        ts = rec[TIMESTAMP_COL]
        ts_str = ts.strftime("%H:%M:%S.%f")[:-3] if hasattr(ts, "strftime") else str(ts)
        top_feat = rec.get("top_error_feature", "")
        top_signed = float(rec.get("top_feature_signed_diff", 0) or 0)

        for f in feat_cols:
            err = float(rec.get(f, 0) or 0)
            is_a = err > threshold
            if f == top_feat:
                direction = "high" if top_signed > 0 else "low" if top_signed < 0 else ""
                explanation = rec.get("root_cause_text", "")
                if not explanation:
                    explanation = "Normal" if not is_a else f"{f} anomaly"
            else:
                direction = ""
                explanation = "Normal" if not is_a else f"{f} anomaly"

            per_feature[f].append({
                "timestamp": ts_str,
                "error": round(err, 6),
                "threshold": round(threshold, 6),
                "threshold_distance": round(err - threshold, 6),
                "severity_ratio": round(err / threshold, 3) if threshold else 0,
                "is_anomaly": is_a,
                "feature": f,
                "direction": direction,
                "explanation": explanation,
            })

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(per_feature, f, ensure_ascii=False, indent=2)

    total_anom = sum(1 for rows in per_feature.values() for r in rows if r["is_anomaly"])
    print(f"[export] Single feature results written → {out}  ({total_anom} anomalies across {len(feat_cols)} features)")
    return per_feature