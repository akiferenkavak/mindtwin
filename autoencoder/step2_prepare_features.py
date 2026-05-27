from sklearn.preprocessing import StandardScaler
from step1_load_and_check import load_and_check_train_data

# =====================================
# 1) STEP1'DEN BIRLESMIS VERIYI AL
# =====================================
df_all = load_and_check_train_data()

# =====================================
# 2) FEATURE KOLONLARI
# =====================================
torque_cols = [f"TORQUE_A{i}" for i in range(1, 7)]
curr_cols   = [f"CURR_A{i}" for i in range(1, 7)]
temp_cols   = [f"MOT_TEMP_A{i}" for i in range(1, 7)]
vel_cols    = [f"VEL_AXIS_ACT_A{i}" for i in range(1, 7)]

feat_cols = torque_cols + curr_cols + temp_cols + vel_cols

# =====================================
# 3) FEATURE MATRIX
# =====================================
X = df_all[feat_cols].copy()

for c in feat_cols:
    X[c] = X[c].astype(float)

valid_idx = X.dropna().index
X = X.loc[valid_idx].reset_index(drop=True)
df_all = df_all.loc[valid_idx].reset_index(drop=True)

print("Toplam ornek sayisi:", X.shape[0])
print("Feature sayisi:", X.shape[1])

# =====================================
# 4) SCALE
# =====================================
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

print("Scaled shape:", X_scaled.shape)
print("Mean (ilk 5):", X_scaled.mean(axis=0)[:5])
print("Std (ilk 5):", X_scaled.std(axis=0)[:5])

# =====================================
# 5) DISARIDAN CAGIRILACAK FONKSIYON
# =====================================
def prepare_features():
    return df_all, X, X_scaled, scaler, feat_cols


if __name__ == "__main__":
    prepare_features()