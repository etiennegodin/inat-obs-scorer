# inat-obs-scorer

> **Expert Review Prioritization Engine for iNaturalist**
> *Resurface valuable "Needs ID" observations before they go unnoticed*

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)
[![LightGBM](https://img.shields.io/badge/model-LightGBM-brightgreen)](https://lightgbm.readthedocs.io/)
[![MLflow](https://img.shields.io/badge/tracking-MLflow-orange)](https://mlflow.org/)
[![DuckDB](https://img.shields.io/badge/storage-DuckDB-yellow)](https://duckdb.org/)
[![DVC](https://img.shields.io/badge/data-DVC-purple)](https://dvc.org/)

---

## The Problem

iNaturalist accumulates millions of wildlife observations from citizen scientists. A subset earn **Research Grade (RG)** status — a quality threshold that makes observations scientifically usable for biodiversity research. Getting there requires community agreement from knowledgeable identifiers, but expert attention is a scarce resource.

The challenge is that **most observations resolve quickly on their own**. The community finds and confirms high-quality observations of common, well-photographed species within days. What gets lost are the observations that slip through unnoticed — unusual taxa, ambiguous photos, or submissions from regions with few active identifiers.

This project builds a **probabilistic ranking system** that scores each open "Needs ID" observation on its likelihood of reaching Research Grade, enabling triage of expert review queues toward observations with real potential that the community has not yet engaged with.

---

## Why Triage? — The Settlement Curve

RG label resolution is strongly front-loaded. Of all observations that will eventually reach Research Grade in the dataset:

| Days after submission | Cumulative RG captured | Remainder |
|---|---|---|
| 1 | 56% | 44% |
| 7 | 69.5% | 30.5% |
| 30 | 76.4% | 23.6% |
| 90 | 82.3% | 17.7% |
| 365 | 93.6% | 6.4% |
| 730 | 97.0% | 3.0% |

Observations that haven't resolved RG status by day 7 represent the population that has slipped through without community engagement. **This is the model's exclusive target**: observations that are not Research Grade at the 7-day mark, either because they received no identifications or because early identifications haven't reached consensus.

This framing deliberately excludes the 70% of eventual-RG observations that are self-resolving — they don't need routing. The model is a filter for *unattended* observations, not a general RG predictor.

---

## The Actionable Zone

The model produces a P(Research Grade) score. Not all score ranges are equally useful:

```
P(RG) < 0.35   →  High-confidence negatives: poor documentation,
                   structurally ineligible. Expert time is unlikely to change the outcome.

P(RG) 0.35–0.70 →  ACTIONABLE ZONE: real potential but not self-resolving.
                    Expert identification or a confirming ID could tip these to RG.

P(RG) > 0.70   →  High-confidence positives: likely to get confirmed through
                   normal community activity without intervention.
```

Expert review is routed toward the actionable zone. High-confidence positives and negatives are filtered out.

### Operating Point

The triage threshold is selected to prioritise recall — missing a recoverable observation is costlier than occasionally showing an expert a borderline case. The current operating point (post-calibration) targets:

- **Recall**: ~90% of actionable-zone RG observations surfaced
- **Precision**: ~60% of routed observations are confirmed RG within the label window

> ⚠️ *Note: probability scores are not natively well-calibrated from LightGBM. Platt scaling is applied as part of the serving layer (v0.3) to ensure P(RG) = 0.5 corresponds to 50% true probability. Threshold values in the unserved model should not be interpreted as literal probabilities, but ranking metrics (PR-AUC, lift@K) are calibration-independent and are the primary evaluation signal.*

---

## Model Performance

*Metrics reflect v0.2 results. HP tuning results and final test evaluation will be logged here after v0.2 lock.*

**Validation set — no-HP-tuning baseline**

| Metric | Value |
|---|---|
| PR-AUC (Average Precision) | ~0.795 |
| Baseline (positive rate) | ~0.323 |
| Lift over baseline | ~2.5× |

**Ranking metrics at validation set (overall RG rate ~32.3%)**

| Top K% reviewed | Observations | Recall@K | Precision@K | Lift@K |
|---|---|---|---|---|
| 1% | 183 | 3.1% | 98.9% | 3.06× |
| 5% | 911 | 14.8% | 95.3% | 2.95× |
| 10% | 1,822 | 27.4% | 88.4% | 2.74× |
| 20% | 3,643 | 49.5% | 80.0% | 2.48× |
| 50% | 9,108 | 89.9% | 58.0% | 1.80× |

Reviewing the top 10% of scored observations captures ~27% of all eventual-RG observations at 88% precision — meaning roughly 9 in 10 observations shown to an expert are genuine triage candidates.

---

## Architecture

```
[Raw Source]
    iNaturalist open data export (CSV)
          ↓
[Ingestion Layer]
    Async API client — rate-limited, fault-tolerant, Protocol-based
    DuckDB as single source of truth
          ↓
[Feature Engineering Layer]
    SQL-heavy transforms in DuckDB
    Point-in-time windowed features — no temporal leakage
    Static taxon difficulty aggregates — Bayesian-shrunk at species/genus/family
          ↓
[Label Engineering]
    Community taxon re-derived via DuckDB table macro
    Research Grade label computed from windowed identification history
    Population filter: observations not RG at day 7
          ↓
[Training Dataset]
    Hard temporal split with gap buffers (train / val / test)
    Closed-window binary label: RG status at obs_date + 365 days
          ↓
[Model Training]
    Modular scikit-learn Pipeline with registry-pattern components
    LightGBM + Optuna hyperparameter search + MLflow tracking
          ↓
[Explainability]
    SHAP value analysis — global importance + error-bucket delta analysis
          ↓
[Serving Layer]  ← (v0.3)
    FastAPI  POST /score → { observation_id, rg_probability, rank }
    Probability calibration (Platt scaling)
    Triage threshold with configurable precision floor
```

---

## Key Engineering Decisions

### 1. Temporal leakage — four distinct risk vectors

Most ML pipelines guard against one form of leakage. This project explicitly identifies and addresses four:

| Vector | Risk | Mitigation |
|---|---|---|
| **Label leakage** | Scraped `quality_grade` reflects current state, not state at prediction time | RG label re-derived from windowed identification history via DuckDB table macro |
| **Feature leakage** | Aggregating observer/taxon stats across the full dataset contaminates past observations with future signal | All window functions bounded to `created_at` |
| **Split leakage** | Shuffling within temporal partitions destroys gap buffer integrity | Hard date-range boundaries from `SplitConfig`; val/test rows ordered by `created_at`, never shuffled |
| **CV split leakage** | Standard K-fold with shuffling violates temporal structure, producing optimistically biased estimates | Custom `ExpandingWindowCvSplit(BaseCrossValidator)` — equal-chunk expanding window, sklearn-compatible, with `gap_size` hook |

### 2. Research Grade — a two-stage label

Research Grade is not simply a community consensus signal. It is a compound label with two distinct requirements, both re-derived here from windowed identification history:

**Stage 1 — Community taxon** ([iNaturalist docs](https://help.inaturalist.org/en/support/solutions/articles/151000173076))

The community taxon is computed via a taxonomic tree traversal. At each node, the algorithm scores cumulative agreement against disagreement including ancestor disagreements, and requires a 2/3 supermajority with at least 2 identifications. This project re-implements the algorithm as a **DuckDB table macro** (`community_taxon_windowed(eval_interval)`) parameterised by evaluation timestamp:

```sql
-- cumulative_agreement / (agreements + disagreements + ancestor_disagreements) ≥ 2/3
-- Minimum 2 identifications required at the agreed node
```

**Stage 2 — Research Grade eligibility** ([iNaturalist docs](https://help.inaturalist.org/en/support/solutions/articles/151000169936))

Community taxon is necessary but not sufficient. The `research_grade_windowed()` wrapper enforces all conditions — verifiable media, geolocation, date, non-captive, species-level community taxon — and surfaces `is_rg` as the training label, replacing the scraped `quality_grade` column entirely.

### 3. Taxon difficulty with Bayesian shrinkage, hierarchical fallback, and static aggregates

Rare taxa have too few observations to compute reliable difficulty estimates. This project uses a layered approach:

**Dynamic features (point-in-time):** Taxon RG rates are computed with Bayesian shrinkage (α = 10) against the global prior, with hierarchical fallback — species → genus → family → order → global mean — applied when sample counts are insufficient. All rates are computed on the training partition only and applied to val/test without recomputation.

**Static taxon difficulty aggregates:** Beyond RG rates, per-taxon historical patterns are encoded at the species, genus, and family level:
- Average and standard deviation of time-to-RG (how long does this taxon typically take?)
- Average and maximum of lag distributions (how long before this taxon receives any ID?)
- Average number of identifications required to reach RG
- `tx_lag_deviation`: how much this observation's lag deviates from the taxon historical norm



### 4. Species confusion graph features

Visually similar species create systematic misidentification patterns. The confusion graph, built with DuckPGQ, encodes:

- **Neighborhood difficulty**: how hard is the local species cluster to disambiguate?
- **Asymmetric sink flag**: is this taxon disproportionately the *recipient* of misidentifications from visually similar species?
- **Focal taxon rank within neighborhood**: where does this species sit in terms of identifier confidence?
- **2-hop graph metrics**: clustering coefficient and graph size capturing the broader confusion neighbourhood

### 5. Protocol-based async API client

The enrichment layer uses a fully async client designed around Python `Protocol` interfaces rather than inheritance, keeping fetchers and writers decoupled and independently testable.

```
BatchEndpointClient        — fixed-set ID requests, bulk pagination
ParametrizedEndpointClient — flexible endpoint/param formatting per call

asyncio.Queue              — bridges fetch workers and the write thread
ThreadPoolExecutor(max_workers=1) — serialises DuckDB writes from async context
Exponential backoff + jitter — handles iNaturalist rate limiting gracefully
_resolve_id cascade        — flexible ID field mapping across endpoint shapes
```

### 6. Modular scikit-learn pipeline with registry pattern

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

| Group | Key Features |
|---|---|
| **Observer history** | Historical RG rate (actual vs. expected), total obs count, account tenure, taxon diversity, observer reputation rank |
| **Observation documentation** | Photo count, average photo count, presence of notes, coordinate uncertainty, submission lag (observed → created) |
| **Taxon context** | Taxon rank, RG rate with Bayesian shrinkage and hierarchical fallback, taxon popularity rank |
| **Static taxon difficulty** | Time-to-RG mean/std, lag distributions at species/genus/family, IDs required for RG, lag deviation from taxon norm |
| **Identification dynamics** | IDs received, agreement rate, identifier diversity, ID velocity, reciprocity ratio, maverick count |
| **Confusion graph** | Neighborhood difficulty, sink-species flag, focal taxon rank in cluster, 2-hop clustering coefficient |
| **Temporal / phenological** | Submitted week (sin/cos), observed week (sin/cos), submission pressure, activity at phenological peak, months from peak phenology |
| **Community consensus** | Community taxon rank, first-ID agreement, pct IDs refining at window |

---

## ML Stack

| Concern | Tool |
|---|---|
| Storage & transforms | DuckDB (SQL-first, no ORM) + DuckPGQ |
| Pipeline composition | scikit-learn `Pipeline` |
| Model | LightGBM |
| Hyperparameter search | Optuna, fANOVA importance logged to MLflow |
| Experiment tracking | MLflow (params, metrics, artifacts, model registry) |
| Explainability | SHAP (global importance, beeswarm plots, error-bucket delta analysis) |
| Data versioning | DVC |
| Calibration *(v0.3)* | Platt scaling on top of LightGBM raw probabilities |
| Validation *(v0.3)* | Pydantic models for config and schema enforcement |
| Serving *(v0.3)* | FastAPI |

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

### Evaluate

```bash
# Final one-shot evaluation against the held-out test set
inat_pipe test
```

Reserved for a single terminal evaluation run. Outputs PR-AUC, ranking metrics, and calibration diagnostics against the held-out test partition — never used during model selection or feature iteration.

### Inference *(v0.3)*

```bash
inat_pipe inference --obs_id <id>
```

---

## Data Selection

Not all records in the raw iNaturalist export are suitable for training. Selection happens at two levels:

**Observation-level eligibility** — only verifiable observations are retained: georeferenced, dated, with media, non-captive. Casual and ineligible observations are excluded from the training set but preserved as a separate class for potential future modelling.

**Observer-level coverage** — observers must meet both:
- **Minimum activity**: ≥ 20 observations, ensuring a meaningful historical footprint for observer reputation features
- **Time coverage**: oldest observation before 2020 and newest after 2024, ensuring the observer's history spans the label window cleanly

**Population filter** — the model is trained exclusively on observations that are not Research Grade at day 7 after submission. Observations that self-resolved within the first week are excluded: they represent community-engaged cases that do not benefit from routing.

---

## Split Strategy

Splits use hard date-range boundaries derived from a `SplitConfig` dataclass anchored on a single `cutoff_date`. Gap buffers between partitions prevent label-time contamination. Val and test sets are ordered by `created_at` to preserve temporal integrity.

```
[──── Train ────][gap][── Val ──][gap][─── Test ───]
  ~55%                  ~17%            ~27%
```

Positive rate drifts naturally across splits (train ~41%, val ~32%, test ~28%), reflecting the evolving composition of the iNaturalist community over time and the growing proportion of difficult-taxon observations in more recent data. This is expected and is not a sign of overfitting.

Cross-validation during HP search uses a custom `ExpandingWindowCvSplit` (scikit-learn `BaseCrossValidator` subclass) with equal-chunk expanding windows.

---

## Project Structure

```
inat_pipeline/
├── api/
├── app/
│   ├── container.py         # App dependencies
│   └── service.py           # App entry point
├── db/
│   ├── adapters/
│   │   └── duckdb_adapter.py
│   ├── protocols.py
│   └── sql.py
├── ingest/
│   ├── inat_client/
│   │   ├── base.py          # Async Protocol-based API client
│   │   ├── clients.py       # BatchEndpointClient, ParametrizedEndpointClient
│   │   ├── config.py
│   │   ├── factory.py
│   │   ├── fetchers.py      # RateLimiterFetcher
│   │   ├── protocols.py
│   │   ├── registry.py      # Specific endpoint fields
│   │   └── writers.py       # ThreadPoolExecutor-backed DuckDB writer
│   └── local/
│       ├── ingestors.py     # Expandable backend support *(v0.4)*
│       └── protocols.py
├── queries/                 # All .sql queries
│   ├── api/                 # Prep raw data receiving
│   ├── features/            # Features suite, injected via params CTE
│   ├── graph/               # Graph queries for taxa confusion, with DuckPGQ
│   ├── split/               # Train/Val/Test splits
│   ├── stage/               # Raw data staging
│   ├── params.py
│   └── registry.py
├── train/
│   ├── utils/
│   ├── config.py
│   ├── core.py
│   ├── explainability.py
│   ├── final.py
│   ├── objective.py
│   └── registry.py
├── utils/                   # Misc utils, logger, etc.
├── workflows/
│   ├── features_workflow.py
│   ├── ingest_api_observations_workflow.py
│   ├── ingest_api_similar_species_workflow.py
│   ├── ingest_api_workflow.py
│   ├── ingest_local_workflow.py
│   ├── test_workflow.py
│   └── train_workflow.py
├── exceptions.py            # Custom exceptions hierarchy
└── cli.py                   # Entrypoints: ingest / features / train / test / inference
```

---

## Roadmap

### ✅ v0.1 — Data pipeline and baseline
- ELT pipeline, DuckDB storage layer
- Basic feature engineering
- Logistic regression baseline

### ✅ v0.2 — Extended features and production model
- scikit-learn Pipeline with registry pattern
- LightGBM + Optuna (TPE + MedianPruner) + MLflow experiment tracking
- SHAP explainability — global importance and beeswarm plots
- Windowed community taxon and RG label re-derivation (DuckDB table macro)
- Bayesian shrinkage for taxon RG rates with hierarchical fallback
- Species confusion graph features via DuckPGQ
- Static taxon difficulty aggregates (time-to-RG, lag distributions, IDs to RG)
- Temporal and phenological features
- Custom `ExpandingWindowCvSplit` for temporally-safe cross-validation
- Population filter: model scoped to observations not-RG at day 7
- Primary metric: Average Precision (PR-AUC) — calibration-independent, penalises false positives at low recall thresholds
- DVC for data versioning

### 🔲 v0.3 — Serving, calibration, and system integrity
- FastAPI inference endpoint (`POST /score → { observation_id, rg_probability, rank }`)
- **Probability calibration** (Platt scaling) — corrects LightGBM's systematic underconfidence; ensures P(RG) = 0.5 corresponds to 50% true probability
- **Triage threshold correction** — configurable precision floor with recall-maximising threshold selection post-calibration
- Cold-start fallback paths via precomputed inference cache (observer and taxon summary stats)
- Schema drift assertions + lightweight feature versioning tied to MLflow runs
- Pydantic models for config and schema enforcement
- Run manifest and pipeline lineage table (idempotent retries, auditability)

### 🔲 v0.4 — Model diagnostics and targeted improvement
The v0.2 model already exhibits meaningful concentration of SHAP signal in a small number of features (`obv_tx_rg_rate`, `tx_conf_nbrhd_rank_min`). This raises a diagnostic question: for observations in the uncertain zone (P(RG) ≈ 0.35–0.70), are errors driven by missing signal (cold-start, sparse taxon history) or by the model misfiring on well-documented observations?

v0.4 applies a structured SHAP delta analysis across error buckets to answer this:

| Bucket | Definition |
|---|---|
| Correct/certain | Model is confident and right |
| Incorrect/certain | Model is confident and wrong — the most diagnostic failures |
| Correct/uncertain | Model hedges but is right — what features help resolution? |
| Incorrect/uncertain | Model hedges and is wrong — where is signal absent? |

Comparing mean absolute SHAP values across these buckets reveals which features are helping vs. absent in each failure mode. Preliminary analysis shows that errors in the uncertain zone correlate with thin community consensus signals (`pct_ids_agree_at_window`, `community_taxon_rank`) — pointing toward cold-start uncertainty as the primary failure driver rather than model misspecification.

Planned work:
- Complete EDA by feature × error bucket — feature value distributions (not just SHAP) per bucket to distinguish absence from misfiring
- Targeted feature engineering for cold-start observations (zero or one ID at window)
- Observer × top-identifier expertise interaction term
- Geographic range signal (is this taxon observed outside its typical range?)
- AWS S3 ingestion source migration to facilitate scope expansion beyond Québec

---

## Scope & Limitations

- Currently scoped to **Plantae** observations in **Québec**
- Model trained on observers with ≥ 20 observations and history spanning 2020–2024; cold-start observers are handled via precomputed fallback stats (v0.3)
- Probability scores are uncalibrated pre-v0.3; ranking metrics are the primary evaluation signal in v0.2

---

*Built as a portfolio project modeled on a production ML team working within the iNaturalist ecosystem.*
