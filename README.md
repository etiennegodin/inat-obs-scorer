# inat-obs-scorer

> **Expert Review Prioritization Engine for iNaturalist**
> *Which "Needs ID" observations are most likely to reach Research Grade — and should be reviewed first?*

[![Python 3.11+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)
[![LightGBM](https://img.shields.io/badge/model-LightGBM-brightgreen)](https://lightgbm.readthedocs.io/)
[![MLflow](https://img.shields.io/badge/tracking-MLflow-orange)](https://mlflow.org/)
[![DuckDB](https://img.shields.io/badge/storage-DuckDB-yellow)](https://duckdb.org/)
[![ROC-AUC](https://img.shields.io/badge/ROC--AUC-0.887-success)]()

---

## Overview

iNaturalist accumulates millions of wildlife observations submitted by citizen scientists. A subset of these earn **Research Grade (RG)** status — a quality threshold that makes observations useful for biodiversity science. Getting there requires community taxon agreement from knowledgeable identifiers, but expert attention is a scarce resource.

This project builds a **binary classifier** that scores each open "Needs ID" observation on its probability of reaching Research Grade, enabling triage of expert review queues. It is scoped to the plant kingdom (*Plantae*) in Québec and is designed as a production-style ML system.

### Problem framing

```
Observer quality      → How reliable is this observer's documentation?
Identifier quality    → How knowledgeable are the identifiers involved?
Taxon difficulty      → How much community attention does this species require?
Community consensus   → What has the community already signalled?
                                        ↓
                           P(Research Grade) score
```

The core modelling challenge is **temporal**: all features must be reconstructed at the exact moment of each observation, and the label itself must be derived from a point-in-time simulation of iNaturalist's identification algorithm — not the current scraped state.

---

## Architecture

```
[Raw Source]
    iNaturalist open data export (CSV) + targeted API scraping
          ↓
[Ingestion Layer]
    Async API client — rate-limited, fault-tolerant, Protocol-based
    DuckDB as single source of truth
          ↓
[Feature Engineering Layer]
    SQL-heavy transforms in DuckDB
    Point-in-time windowed features — no temporal leakage
          ↓
[Label Engineering]
    Community taxon re-derived via DuckDB table macro
    Research Grade label computed from windowed identification history
          ↓
[Training Dataset]
    Hard temporal split with gap buffers (train / val / test)
    Closed-window binary label: RG status at obs_date + 90 days
          ↓
[Model Training]
    Modular scikit-learn Pipeline with registry-pattern components
    LightGBM + Optuna hyperparameter search + MLflow tracking
          ↓
[Explainability]
    SHAP value analysis logged as MLflow artifacts
          ↓
[Serving Layer]  ← (v0.3)
    FastAPI  POST /score → { observation_id, rg_probability, rank }
```

---

## Key Engineering Decisions

### 1. Temporal leakage has three distinct risk vectors

Most ML pipelines guard against one form of leakage. This project explicitly addresses three:

| Vector | Risk | Mitigation |
|---|---|---|
| **Label leakage** | Using scraped `quality_grade` reflects current state, not state at prediction time | Re-derive RG label from windowed identification history |
| **Feature leakage** | Aggregating observer/taxon stats across the full dataset contaminates the past with the future | All window functions are bounded to `obs_created_at` |
| **Split leakage** | Shuffling records within temporal partitions destroys gap buffer integrity | Hard date-range boundaries; val/test rows sorted by `created_at`, never shuffled |

### 2. Community taxon re-derived from first principles

iNaturalist's [Community taxon](https://help.inaturalist.org/en/support/solutions/articles/151000173076-what-are-the-community-taxon-and-the-observation-taxon-) algorithm is non-trivial: it involves a taxonomic tree traversal that scores cumulative agreement at each node. Rather than trusting the scraped `quality_grade` column, this project implements the actual algorithm as a **DuckDB table macro** (`community_taxon_windowed(eval_interval)`) — parameterized by an evaluation timestamp to enable fully point-in-time label computation.

```sql
-- Threshold: cumulative_agreement / (agreements + disagreements + ancestor_disagreements) ≥ 2/3
-- Minimum count: 2 identifications required
-- research_grade_windowed() wraps this and surfaces is_rg as the external label
```


### 3. Research grade label

[Label](https://help.inaturalist.org/en/support/solutions/articles/151000169936)

### 4. Taxon difficulty with Bayesian shrinkage and hierarchical fallback

Rare taxa have too few observations to compute a reliable RG rate. A naive approach either drops them or overfits to small samples. This project uses:

- **Bayesian shrinkage** (α = 10) to blend the taxon-specific rate toward the global prior
- **Hierarchical fallback**: species → genus → family → order → global mean, applied when the shrunk estimate is still unreliable
- All rates computed **point-in-time** on the training partition only, then applied to val/test — never recomputed on the full dataset

### 5. Species confusion graph features

Visually similar species create systematic misidentification patterns. The confusion graph encodes:

- **Neighborhood difficulty**: how hard is the local species cluster to disambiguate?
- **Asymmetric sink flag**: is this taxon disproportionately the *recipient* of misidentifications from visually similar species?
- **Focal taxon rank within neighborhood**: where does this species sit in terms of identifier confidence?

*Graph figures*

### 6. Protocol-based async API client

The enrichment layer uses a fully async client designed around Python `Protocol` interfaces rather than inheritance, keeping fetchers and writers decoupled and independently testable.

```
BatchEndpointClient      — fixed-set ID requests, bulk pagination
ParametrizedEndpointClient — flexible endpoint/param formatting per call

asyncio.Queue            — bridges fetch workers and the write thread
ThreadPoolExecutor(max_workers=1) — serializes DuckDB writes from async context
Exponential backoff + jitter — handles iNaturalist rate limiting gracefully
_resolve_id cascade      — flexible ID field mapping across endpoint shapes
```

### 7. Modular scikit-learn pipeline with registry pattern

Each pipeline stage (imputer, encoder, scaler, reducer, classifier) is registered by name and resolved at runtime from CLI arguments, enabling clean experiment configuration without code changes:

```bash
inat_pipe train \
  --classifier lightgbm \
  --imputer median \
  --encoder onehot \
  --scaler robust \
  --reducer none \
  --n_trials 50 \
  --cv_folds 5
```

---

## Feature Groups

| Group | Features |
|---|---|
| **Observer history** | Historical RG rate (actual vs. expected), total obs count, account tenure, taxon diversity |
| **Observation documentation** | Photo count, presence of notes, coordinate uncertainty |
| **Taxon context** | Taxon rank, RG rate with Bayesian shrinkage, hierarchical fallback chain |
| **Identification dynamics** | Number of IDs received, agreement rate |
| **Confusion graph** | Neighborhood difficulty, sink-species flag, focal taxon rank in cluster |
| **Temporal** | Day of year, hour of submission, time elapsed since submission |
---

## ML Stack

| Concern | Tool |
|---|---|
| Storage & transforms | DuckDB (SQL-first, no ORM) |
| Pipeline composition | scikit-learn `Pipeline` |
| Model | LightGBM |
| Hyperparameter search | Optuna (fANOVA importance logged to MLflow) |
| Experiment tracking | MLflow (params, metrics, artifacts, model registry) |
| Explainability | SHAP (feature importance, beeswarm plots) |
| Validation  *(v0.3)* | Pydantic models for config and schema enforcement |
| Serving *(v0.3)* | FastAPI |

**Current performance**: ROC-AUC **~0.88** on out-of-time val set.

---

## Data Pipeline CLI

```bash
pip install -e .
```

### Ingest

```bash
# Ingest from local export files
inat_pipe ingest local

# Enrich via iNaturalist API (async, rate-limited)
inat_pipe ingest api --rate 15 --ignore_not_found
```

### Feature engineering

```bash
inat_pipe features
```

### Train

```bash
inat_pipe train \
  --classifier lightgbm \
  --imputer median \
  --encoder onehot \
  --scaler robust \
  --n_trials 50 \
  --cv_folds 5
```

### Inference *(v0.3)*

```bash
inat_pipe inference --obs_id <id>
```

---

## Sampling Strategy

Observers are included if they meet both criteria:

- **Minimum activity**: ≥ 20 observations
- **Time coverage**: oldest observation before 2020, newest after 2024

This ensures every observer in the training set has a meaningful historical footprint and spans the label window cleanly.

---

## Split Strategy

Splits use hard date-range boundaries derived from a `SplitConfig` dataclass anchored on a single `cutoff_date`. Gap buffers between partitions prevent label-time contamination. Val and test sets are downsampled by `ORDER BY created_at` to preserve temporal ordering.

```
[──── Train ────][gap][── Val ──][gap][─── Test ───]
  ~60%                  ~16%            ~24%
```

A natural positive-rate drift (57% → 52%) from train to val/test is expected and is not a sign of overfitting — it reflects the evolving composition of the iNaturalist community over time.

---

## Project Structure

```
inat_pipeline/
├── ingest/
│   ├── local.py          # CSV ingestion
│   └── api/
│       ├── client.py     # Async Protocol-based API client
│       ├── fetchers.py   # BatchEndpointClient, ParametrizedEndpointClient
│       └── writers.py    # ThreadPoolExecutor-backed DuckDB writer
├── features/
│   ├── observer.py
│   ├── taxon.py          # Bayesian shrinkage + hierarchical fallback
│   ├── confusion_graph.py
│   └── sql/              # All transforms as .sql files, injected via params CTE
├── labels/
│   └── community_taxon.sql  # Windowed RG label derivation
├── training/
│   ├── pipeline.py       # Registry-pattern component resolution
│   ├── cv.py             # Custom CV loop with LightGBM eval_set support
│   ├── optuna_study.py
│   └── mlflow_logging.py
└── cli.py                # Entrypoints: ingest / features / train / inference
```

---

## Roadmap

### ✅ v0.1 — Data pipeline and baseline
- ELT pipeline, DuckDB storage layer
- Basic feature engineering
- Logistic regression baseline

### ✅ v0.2 — Extended features and real model
- scikit-learn Pipeline with registry pattern
- LightGBM + Optuna + MLflow
- SHAP explainability
- Windowed community taxon and RG label re-derivation
- DVC for data versioning
- Bayesian shrinkage for taxon RG rates

### 🔲 v0.3 — System design and serving
- FastAPI inference endpoint (`POST /score`)
- Cold-start fallback paths via precomputed inference cache
- Run manifest and pipeline lineage table (idempotent retries)
- Schema drift assertions + lightweight feature versioning tied to MLflow runs

### 🔲 v0.4 — Advanced features and routing
- Shap evaluation at borderline observations with incorrect classification
- Additionnal features ideas:
  - Phenology alignement indicators
  - Observer × top-identifier expertise interaction term
  - Geographic range signal
- Survival model (time-to-RG)
- Rare species → expert routing
- AWS S3 ingestion source migration

---

## Scope & Limitations

- Currently scoped to **Plantae** observations in **Québec**

---

*Built as a portfolio project modeled on a production ML team working within the iNaturalist ecosystem.*
