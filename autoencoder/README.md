# 🤖 KUKA Robot Anomaly Detection with Autoencoder

Bu proje, KUKA robotundan alınan çok sensörlü veriler kullanılarak robot davranışındaki anormal durumları otomatik olarak tespit etmeyi amaçlar.
Robotun **tork, akım, sıcaklık ve hız** sensörlerinden alınan veriler birlikte değerlendirilir ve sistemin normal davranışı öğrenilerek bu davranıştan sapmalar **anomali** olarak işaretlenir.

---

## 🎯 Project Goal

- Robotun normal çalışma davranışını öğrenmek
- Anormal davranışları otomatik olarak tespit etmek
- Hangi sensörün problem yarattığını belirlemek
- Sonuçları görselleştirmek

---

## 🧠 Why Autoencoder?

Bu projede Autoencoder kullanılmasının nedenleri:

- ✔ **Etiketsiz veride çalışır (unsupervised learning)**
- ✔ **Tüm sensörleri birlikte öğrenir (multivariate)**
- ✔ **Normalden sapmayı reconstruction error ile yakalar**
- ✔ **Endüstride yaygın kullanılan yöntemdir**

📌 Mantık:
> Model bir veriyi iyi reconstruct edemiyorsa → bu veri anormaldir

---

## 📊 Features

Toplam 24 sensör kullanıldı:

| Grup | Sensörler |
|------|-----------|
| Tork | `TORQUE_A1` – `TORQUE_A6` |
| Akım | `CURR_A1` – `CURR_A6` |
| Sıcaklık | `MOT_TEMP_A1` – `MOT_TEMP_A6` |
| Hız | `VEL_AXIS_ACT_A1` – `VEL_AXIS_ACT_A6` |

---

## 🔧 Data Preparation

- Ham KUKA log verisi temizlendi
- `;` ve `,` karışıklıkları düzeltildi
- Timestamp ayrıştırıldı
- Feature kolonları düzenlendi
- Eksik ve hatalı veriler silindi

---

## ⚙️ Preprocessing

- Tüm değerler `float` yapıldı
- NaN satırlar kaldırıldı
- `StandardScaler` ile normalize edildi

---

## 🧠 Model Architecture

Sistemin "normal" davranışını öğrenmek üzere tasarlanmış bir **Autoencoder** mimarisi kullanılmaktadır.

```
Input(24) → Encoder(16) → Bottleneck(8) → Decoder(16) → Output(24)
```

| Parametre | Değer |
|-----------|-------|
| Mimari | `24 → 16 → 8 → 16 → 24` |
| Kayıp Fonksiyonu | MSE (Mean Squared Error) |
| Yaklaşım | Unsupervised |
| Amaç | Normal davranışı öğrenmek ve yeniden yapılandırmak |

---

## 🚨 Anomaly Detection Strategies

Farklı hata tiplerini yakalayabilmek için iki ayrı strateji test edilmiştir:

### 1️⃣ Multi-feature Detection

Tüm özellikler (features) birlikte değerlendirilir ve ortalama hata alınır.

```python
anomaly_score = mean(reconstruction_errors)  # mean_error
```

| ✅ Avantaj | ❌ Dezavantaj |
|------------|--------------|
| Genel sistem bozulmalarını yakalar | Tek sensör spike'ını kaçırabilir |

---

### 2️⃣ Single-feature Detection

Her bir özellik ayrı ayrı incelenir ve en büyük hataya sahip olan özellik seçilir.

```python
anomaly_score = max(reconstruction_errors per feature)  # max_feature_error
```

| ✅ Avantaj | ❌ Dezavantaj |
|------------|--------------|
| Tek sensör anomalilerini (spike) yakalar | Yayılmış anomalileri kaçırabilir |

---

## 🎯 Threshold & Root Cause Strategy

### Threshold (Eşik Değeri) Belirleme

Başlangıç yaklaşımı olarak istatistiksel bir eşik değeri kullanılmaktadır:

```python
threshold = mean + 3 * std
```

> ⚠️ **Kritik:** Threshold **mutlaka train datasından** hesaplanmalıdır.  
> Test datasından alınırsa sonuçlar yanıltıcı olur — bu bir **data leakage** hatasıdır.

---

### 🔍 Root Cause Detection

Her anomaly için otomatik kök neden analizi yapılır:

1. En yüksek reconstruction error'a sahip feature tespit edilir
2. Değerin normalden artmış mı yoksa azalmış mı olduğu belirlenir
3. İnsan okunabilir açıklama üretilir

**Örnek çıktılar:**
```
⚠  A2 motor temperature higher than expected
⚠  TORQUE_A4 lower than normal
```

---

## 📈 Visualization

Anomaly grafikleri şu bileşenleri içerir:

- **X ekseni** → Zaman
- **Y ekseni** → Reconstruction error
- **Threshold çizgisi** → Anomaly sınırı
- **Anomaly noktaları** → Tespit edilen anormallikler

---

## 🧪 Test Dataset & Anomaly Injection

Test verisi özel olarak hazırlanmıştır — spike ekiin yazdığı spike injection kodundan uretılenı kullandım

- Birden fazla log birleştirildi
- Ground truth anomaly zamanları oluşturuldu
- Ekin tarafından spike injection yapıldı

**Neden özel dataset?**

| Sebep | Açıklama |
|-------|----------|
| Gerçekçilik | Gerçek KUKA log verisi kullanıldı |
| Kontrol | Anomaly zamanları tam olarak biliniyor |
| Doğruluk | Model performansı güvenilir şekilde ölçüldü |

---

## 🔄 Pipeline

```
1. Load data              →  step1_load_and_check.py
       ↓
2. Prepare features       →  step2_prepare_features.py
       ↓
3. Train autoencoder      →  step3_autoencoder.py
       ↓
4. Detect anomalies       →  step4_anomoly_detection.py
       ↓
5. Visualize results
```

---

## 📁 Project Structure

```
├── main.py                                    # Batch size = 3 denemeleri
├── main_multiple_entity.py                       #  multi-feature pipeline
├── main_single_entity.py                      # Single-feature pipeline
│
├── step1_load_and_check.py                    # Veri yükleme ve kontrol
├── step2_prepare_features.py                  # Feature seçimi ve preprocessing
├── step3_autoencoder.py                       # Autoencoder model eğitimi
├── step4_anomoly_detection.py                 # Multi-feature anomaly detection
└── step4_single_feature_anomaly_detection.py  # Single-feature anomaly detection
```

---

## 🧪 Experiments

| Senaryo | Açıklama |
|---------|----------|
| Batch size karşılaştırması |  batch size denemeleri |
| Multi vs Single-feature | İki yöntemin karşılaştırılması |
| Threshold stratejileri | Farklı eşik değeri yaklaşımları |
| Anomaly injection testi | Sentetik spike'larla model doğrulama |

**Sonuç:**
- **Single-feature** → Spike anomalilerde daha iyi performans
- **Multi-feature** → Genel sistem bozulmalarında daha iyi performans

---

## 📚 Related Work

Bu proje aşağıdaki alanlardaki araştırmalara dayanmaktadır:

- Autoencoder ile multivariate anomaly detection
- Unsupervised robot anomaly detection
- Industrial time series modeling

---

## 🚀 Future Work

- [ ] LSTM Autoencoder (temporal modeling)
- [ ] Sliding window yaklaşımı
- [ ] Gerçek zamanlı dashboard
- [ ] Daha gelişmiş threshold yöntemleri
- [ ] Explainability geliştirme

---

## 🧠 Key Takeaways

1. **Etiketsiz veride** anomaly detection mümkündür
2. **Autoencoder**, zaman serisi anomaly detection için güçlü bir yöntemdir
3. **Threshold train datasından** alınmalıdır — test datasından değil
4. **Single vs Multi** yaklaşım, farklı anomaly tiplerini çözer
5. **Anomaly injection**, model değerlendirmesi için kritik öneme sahiptir

---

## ❓ Bu Proje Hangi Soruları Cevaplıyor?

> **1.** Robot davranışı anormal mi?
>
> **2.** Hangi sensör bu anomaliye sebep oluyor?




## 📚 Related Work

Bu proje, endüstriyel robot verilerinde anomaly detection için kullanılan modern yaklaşımlara dayanmaktadır. Özellikle autoencoder tabanlı yöntemler ve multivariate sensor analizi literatürde yaygın olarak kullanılmaktadır.

Aşağıdaki çalışmalar bu projeyi desteklemektedir:

---

### 🔹 Autoencoder & Multivariate Anomaly Detection

✔ **Sakurada & Yairi (2014)**  
Autoencoder kullanarak reconstruction error ile anomaly detection yapılmıştır.  
👉 Temel yaklaşımımızın akademik karşılığıdır  
🔗 https://arxiv.org/abs/1408.6404  

✔ **Zong et al. (DAGMM, 2018)**  
Autoencoder + density estimation ile anomaly detection  
👉 Multisensor veri ve unsupervised learning kullanımı  
🔗 https://arxiv.org/abs/1802.00187  

---

### 🔹 Time-Series Based Industrial Anomaly Detection

✔ **Malhotra et al. (LSTM-based Anomaly Detection)**  
Zaman serilerinde anomaly detection için deep learning yaklaşımı  
👉 Sequence modeling kullanılan alternatif yöntem  
🔗 https://arxiv.org/abs/1502.04431  

✔ **Chen et al. (Robot Anomaly Detection)**  
Industrial robot verisinde unsupervised anomaly detection  
👉 Robot sensör verisi üzerinde uygulama  
🔗 https://doaj.org/article/d1c2f807a28c4c15b09c13af09db1cc0  

---

### 🔹 KUKA Robot Based Studies

✔ **H. Kim (Purdue Thesis)**  
KUKA robot üzerinde anomaly detection  
👉 Spectrogram + neural network yaklaşımı  
👉 Reconstruction mantığı ile benzer  
🔗 https://hammer.purdue.edu/articles/Machine_Anomaly_Detection_using_Sound_Spectrogram_Images_and_Neural_Networks/8864375  

✔ **AnoF-Diff (KUKA LBR iiwa)**  
KUKA robot kolunda anomaly detection  
👉 Farklı yöntem (diffusion) ama aynı problem  
🔗 https://arxiv.org/pdf/2509.15153  

---

## 📌 Key Takeaways from Literature

✔ Autoencoder ile anomaly detection endüstride standarttır  
✔ Multivariate sensor verisi birlikte değerlendirilmelidir  
✔ Reconstruction error en yaygın anomaly metriğidir  
✔ Unsupervised learning gerçek robot verileri için kritiktir  
✔ Time-series ve non-time-series yaklaşımlar birlikte kullanılabilir  

---

## 🎯 This Project's Position

Bu proje:

✔ Autoencoder tabanlı  
✔ Multivariate sensor analysis  
✔ Reconstruction error approach  
✔ Unsupervised learning  
✔ Industrial robot (KUKA) domain  

yaklaşımıyla literatürdeki modern anomaly detection yöntemleriyle uyumludur.


