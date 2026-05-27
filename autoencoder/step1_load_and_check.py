import os
import logging
import pandas as pd

# ============================================================
# LOG AYARLARI
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)

# ============================================================
# AYARLAR
# ============================================================
DATA_DIR = "Data_for_Train"
TIMESTAMP_COL = "timestamp"
SEP = ";"

TRAIN_FILES = [
    "kuka_log300scnd-10hz.csv",
    "kuka_log_200scnd_10hz.csv",
    "kuka_log_900scnd_100hz(=5,8hz).csv",
    "kuka_log600_scnd_20hz(çanta kaldırıldı yerine koyuldu).csv",
    "kuka_log_900scnd_100hz(=5,8hz)26 aralık.csv"
]

# Kontrol etmek istediğin kolonlar
CHECK_COLS = [
    "TORQUE_A1",
    "CURR_A1",
    "MOT_TEMP_A1",
    "VEL_AXIS_ACT_A1"
]


# ============================================================
# TEK DOSYA OKU
# ============================================================
def read_single_csv(path: str, sep: str = SEP) -> pd.DataFrame:
    df = pd.read_csv(path, sep=sep, engine="python")

    if TIMESTAMP_COL not in df.columns:
        raise ValueError(f"{os.path.basename(path)} içinde '{TIMESTAMP_COL}' kolonu yok.")

    df[TIMESTAMP_COL] = pd.to_datetime(df[TIMESTAMP_COL], errors="coerce")
    df = df.dropna(subset=[TIMESTAMP_COL]).copy()
    df = df.sort_values(TIMESTAMP_COL).reset_index(drop=True)
    df["source_file"] = os.path.basename(path)

    return df


# ============================================================
# BÜTÜN TRAIN DOSYALARINI ART ARDA TEK TIMELINE YAP
# ============================================================
def stack_train_files_sequentially(
    data_dir: str = DATA_DIR,
    train_files=None,
    sep: str = SEP
) -> pd.DataFrame:

    if train_files is None:
        train_files = TRAIN_FILES

    all_dfs = []
    last_end_time = None

    for fname in train_files:
        path = os.path.join(data_dir, fname)

        if not os.path.exists(path):
            raise FileNotFoundError(f"Dosya bulunamadı: {path}")

        logging.info(f"Okunuyor: {fname}")
        df = read_single_csv(path, sep=sep)

        if df.empty:
            logging.warning(f"Boş dosya atlandı: {fname}")
            continue

        # Her dosyanın kendi iç zaman farkını koru
        relative_time = df[TIMESTAMP_COL] - df[TIMESTAMP_COL].iloc[0]

        # Yeni dosyayı bir öncekinin sonundan 1 saniye sonra başlat
        if last_end_time is None:
            new_start = pd.Timestamp("2025-01-01 00:00:00")
        else:
            new_start = last_end_time + pd.Timedelta(seconds=1)

        df[TIMESTAMP_COL] = new_start + relative_time
        last_end_time = df[TIMESTAMP_COL].iloc[-1]

        logging.info(
            f"{fname} eklendi | satır={len(df)} | "
            f"yeni zaman aralığı: {df[TIMESTAMP_COL].iloc[0]} -> {df[TIMESTAMP_COL].iloc[-1]}"
        )

        all_dfs.append(df)

    if not all_dfs:
        raise RuntimeError("Hiç train dosyası okunamadı.")

    df_all = pd.concat(all_dfs, ignore_index=True)
    df_all = df_all.sort_values(TIMESTAMP_COL).reset_index(drop=True)

    logging.info(f"Toplam birleşik train satırı: {len(df_all)}")
    return df_all


# ============================================================
# KONTROL EKRANI
# ============================================================
def print_basic_check(df: pd.DataFrame):
    print("BIRLESTIRILMIS TRAIN VERISI OKUNDU")
    print("Toplam kolon sayisi:", len(df.columns))
    print(df.columns.tolist())

    print("\nIlk 5 satir:")
    print(df.head())

    print("\nSon 5 satir:")
    print(df.tail())

    print("\nKOLON TIPLERI:")
    for c in CHECK_COLS:
        if c in df.columns:
            print(c, "->", df[c].dtype)
        else:
            print(c, "-> KOLON YOK")

    print("\nDOSYA BAZLI SATIR SAYILARI:")
    print(df["source_file"].value_counts())

    print("\nZAMAN ARALIGI:")
    print("Baslangic:", df[TIMESTAMP_COL].min())
    print("Bitis    :", df[TIMESTAMP_COL].max())


# ============================================================
# DISARIDAN CAGIRILACAK FONKSIYON
# ============================================================
def load_and_check_train_data():
    import os
    import pandas as pd

    all_dfs = []

    for fname in TRAIN_FILES:
        path = os.path.join(DATA_DIR, fname)

        if not os.path.exists(path):
            print(f"UYARI: {fname} bulunamadi")
            continue

        print(f"Okunuyor: {fname}")

        df = pd.read_csv(path, sep=SEP, engine="python")

        df[TIMESTAMP_COL] = pd.to_datetime(df[TIMESTAMP_COL], errors="coerce")
        df = df.dropna(subset=[TIMESTAMP_COL])

        df["source_file"] = fname

        all_dfs.append(df)

    if not all_dfs:
        raise RuntimeError("Hic veri yuklenemedi")

    # 🔥 TÜM DOSYALARI BIRLESTIR
    df_all = pd.concat(all_dfs, ignore_index=True)

    # 🔥 EN KRITIK SATIR
    df_all = df_all.sort_values(TIMESTAMP_COL).reset_index(drop=True)

    print("\n====================")
    print("BIRLESTIRILMIS VERI")
    print("====================")

    print("Toplam satir:", len(df_all))
    print("Zaman araligi:")
    print("Baslangic:", df_all[TIMESTAMP_COL].min())
    print("Bitis    :", df_all[TIMESTAMP_COL].max())

    print("\nDosya dagilimi:")
    print(df_all["source_file"].value_counts())

    return df_all


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    df = load_and_check_train_data()