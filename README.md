# 🛡️ NovaPay — Sistema de Detección de Fraude en Tiempo Real

> Sistema end-to-end de detección de fraude financiero con ingesta Kafka, modelo XGBoost, explicabilidad SHAP y despliegue en producción.  
> Proyecto colaborativo del bootcamp **The Bridge** — equipo Data Science (Blue Team).

---

## 📋 Descripción

NovaPay es una fintech ficticia que procesa pagos entre usuarios. Este repositorio contiene el sistema de detección de fraude construido por el equipo de **Data Science**, que actúa como *Blue Team* (defensor) en un ejercicio adversarial de dos rondas:

- **Ronda 1** — El equipo de Ciberseguridad genera fraude sintético. Data Science construye el detector.
- **Ronda 2** — Ciber analiza qué escapó y diseña fraude más sigiloso. Data Science mejora el modelo.

El sistema detecta transacciones fraudulentas **antes de que el usuario reclame**, y proporciona explicabilidad en lenguaje natural para que el analista de fraude tome decisiones informadas.

---

## 🏗️ Arquitectura

```
Producer (simulación)
      ↓
Apache Kafka (topic: transacciones)
      ↓
Consumer → POST /predict
      ↓
FastAPI + Valkey (rate limiting · enriquecimiento de usuario)
      ↓
Pipeline ML: FE → OHE/Scaler → XGBoost
      ↓
SHAP TreeExplainer → top-5 razones fraude/legítima
      ↓
PostgreSQL (predicción + explicabilidad en JSONB)
      ↓
Dashboard Full Stack → Analista de Fraude
      ↓
Datos etiquetados → Re-entrenamiento (retrain.py)
```

---

## 📁 Estructura del repositorio

```
novapay-fraud-detection/
├── notebooks/
│   └── model.ipynb              # Entrenamiento completo del modelo
├── models/
│   └── modelo_fraude_v1.pkl     # Pipeline de inferencia (sin SMOTE)
├── utils/
│   ├── __init__.py
│   └── utils.py                 # feature_engineering() reutilizable
├── src/
│   └── shap_explainer.py        # SHAP + templates lenguaje natural
├── dockers/
│   └── docker-compose.yml       # Kafka + Producer + Consumer
├── data/
│   ├── raw/                     # Datasets originales (no versionados)
│   └── processed/               # Datasets procesados
├── model.ipynb                  # Notebook principal de entrenamiento
├── model_test.py                # Test de predicción + SHAP local
├── retrain.py                   # Re-entrenamiento con datos Ronda 2
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## 🧠 Modelo ML

### Pipeline de entrenamiento

```python
ImbPipeline([
    ('fe',           FunctionTransformer(feature_engineering)),  # Feature Engineering
    ('preprocessor', ColumnTransformer(OHE + StandardScaler)),   # Preprocesado
    ('smote',        SMOTE()),                                    # Balanceo (solo train)
    ('clf',          XGBClassifier(scale_pos_weight=spw))        # Clasificador
])
```

### Feature Engineering aplicado

| Feature creada | Origen |
|---|---|
| `hora` | Extraída de `fecha` |
| `dia_semana`, `es_fin_de_semana`, `mes` | Extraídas de `fecha` |
| `pais_distinto` | `pais_emision != pais_pago` |
| `pais_alto_riesgo` | `pais_pago` en países de alta tasa de fraude (calculado desde train) |
| `hora_madrugada` | `hora` entre 0 y 5 |
| `categoria_alto_riesgo` | Categorías con tasa > 3% (calculado desde train) |
| `online_sin_3ds` | `es_online=1` AND `paso_3d_secure=0` |

### Métricas (Ronda 1)

| Métrica | Valor |
|---|---|
| PR-AUC | **0.9596** |
| ROC-AUC | 0.9911 |
| Precision (fraude) | 0.82 |
| Recall (fraude) | **0.93** |
| IC 95% (PR-AUC) | [0.9584, 0.9620] |
| Threshold operativo | 0.60 |

### Decisiones técnicas

**¿Por qué XGBoost?** Mejor rendimiento en datos tabulares vs redes neuronales. PR-AUC 0.9596 vs LightGBM 0.9585 en este dataset. Integración nativa con SHAP.

**¿Por qué scale_pos_weight y no solo SMOTE?** SMOTE es incompatible con `sample_weight` necesario en el re-entrenamiento con datos reales etiquetados. `scale_pos_weight` modifica directamente la función de pérdida sin generar datos sintéticos.

**¿Por qué threshold 0.60?** Optimizado según tabla de coste del negocio: minimiza `fraudes_perdidos × 150€ + falsos_positivos × 10€`. Recall 92.9% con precision 81.6%.

**¿Por qué SHAP y no feature importance nativa?** SHAP calcula el impacto de cada feature para cada predicción individual. Feature importance es global. El EU AI Act exige explicabilidad por decisión, no global.

---

## ⚡ Kafka — Ingesta en Tiempo Real

```
Producer → genera ~5 TX/segundo con distribuciones realistas
         → publica en topic transacciones

Consumer → lee de Kafka con offset manual
         → POST /predict a FastAPI
         → commit del offset solo si API responde 200
         → reintento automático en caso de fallo
```

**¿Por qué Kafka y no script directo?** Si FastAPI cae, los mensajes se acumulan en el topic (retención 7 días) y se procesan al recuperarse. Con script directo, la caída implica pérdida de datos.

### Arranque local

```bash
# 1. Levantar Kafka
docker-compose up -d zookeeper kafka

# 2. Producer (terminal 1)
uv run python producer.py

# 3. Consumer (terminal 2)
uv run python consumer.py
```

### Despliegue en producción

```bash
# Crear .env
echo "PREDICT_URL=http://web:8000/predict/" > .env

# Arrancar todo
docker-compose up -d

# Ver logs
docker-compose logs -f consumer
```

---

## 🔍 Explicabilidad SHAP

Cada predicción incluye las 5 features más influyentes separadas en dos grupos:

```json
{
  "razones_fraude": [
    {"feature": "pais_distinto", "impacto": 1.716, "descripcion": "países de emisión y pago distintos"},
    {"feature": "pais_pago_IN",  "impacto": 3.621, "descripcion": "pago realizado en India, país de alto riesgo"}
  ],
  "razones_legitima": [
    {"feature": "mismo_envio_facturacion", "impacto": -0.850, "descripcion": "envío y facturación coinciden"}
  ],
  "resumen": {
    "nivel": "ALTO",
    "score": "93%",
    "razones_fraude": ["pago realizado en India, país de alto riesgo", "países de emisión y pago distintos"],
    "razones_legitima": ["envío y facturación coinciden"]
  }
}
```

---

## 🔄 Re-entrenamiento (Ronda 2)

```bash
python retrain.py
```

El script:
1. Carga modelo actual y evalúa con datos Ronda 2
2. Extrae fraudes y legítimas confirmadas por analistas desde PostgreSQL
3. Combina dataset original + datos reales (sample_weight 5x en reales)
4. Re-entrena XGBoost con los mismos hiperparámetros
5. Compara métricas — despliega solo si PR-AUC mejora >5%

---

## 🐳 Docker

```bash
# Build y push a Docker Hub
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t mcdataxdev/kafka-novapay:latest \
  --push .

# En producción — solo necesitas docker-compose.yml y .env
docker-compose pull
docker-compose up -d
```

---

## 📦 Instalación local

```bash
# Clonar
git clone https://github.com/mcdatax/novapay-fraud-detection.git
cd novapay-fraud-detection

# Instalar dependencias
uv sync

# Test del modelo
uv run python model_test.py
```

---

## 🛠️ Stack tecnológico

| Capa | Tecnología | Justificación |
|---|---|---|
| Ingesta | Apache Kafka | Desacoplamiento, cero pérdida de datos |
| Mensajería | aiokafka (async) | Procesamiento no bloqueante |
| API | FastAPI + uvicorn | Asíncrono, tipado, autodocumentación |
| Cache/State | Valkey (Redis) | Rate limiting en microsegundos sin saturar DB |
| ML | XGBoost + sklearn Pipeline | Mejor en tabular, integración SHAP |
| Explicabilidad | SHAP TreeExplainer | Explicabilidad por predicción individual |
| Base de datos | PostgreSQL + JSONB | Queries sobre explicabilidad sin schema fijo |
| Contenedores | Docker + docker-compose | Reproducibilidad y despliegue |
| Lenguaje | Python 3.11 + uv | Ecosistema ML, gestión de dependencias moderna |

---

## 🔮 Mejoras Futuras

### Isolation Forest — Detección de Zero-Days
El modelo actual (XGBoost supervisado) detecta fraude basándose en patrones conocidos del dataset de entrenamiento. Su punto débil son los **zero-days** — fraudes completamente nuevos que Ciber diseñe sin ningún patrón previo.

**Solución propuesta:** capa de detección no supervisada con Isolation Forest:

```python
from sklearn.ensemble import IsolationForest

# Entrenado solo con transacciones legítimas
iso = IsolationForest(contamination=0.05, random_state=42)
iso.fit(X_legitimas)

# En producción: doble capa
score_xgb = pipeline.predict_proba(tx)[:, 1][0]      # fraude conocido
score_iso  = iso.decision_function(tx)                # anomalía desconocida

# Alerta si cualquiera de los dos detecta riesgo
es_sospechosa = score_xgb >= 0.60 or score_iso < umbral_iso
```

**¿Por qué complementa a XGBoost?**
- XGBoost → detecta fraude con patrones conocidos (supervisado)
- Isolation Forest → detecta anomalías nunca vistas (no supervisado)
- Juntos cubren fraude conocido + zero-days

Esta arquitectura de doble capa es el estándar en sistemas de fraude bancario de producción real.

---

## 👤 Autor

**Manuel Correa** — Data Science / AI Engineering  
GitHub: [@mcdatax](https://github.com/mcdatax)  
Bootcamp: The Bridge · Madrid 2026  