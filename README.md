# inat-obs-scorer

> **Expert Review Prioritization Engine for iNaturalist**
> *Resurface valuable "Needs ID" observations before they go unnoticed*

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)
[![LightGBM](https://img.shields.io/badge/model-LightGBM-brightgreen)](https://lightgbm.readthedocs.io/)
[![MLflow](https://img.shields.io/badge/tracking-MLflow-orange)](https://mlflow.org/)
[![DuckDB](https://img.shields.io/badge/storage-DuckDB-yellow)](https://duckdb.org/)
[![DVC](https://img.shields.io/badge/data-DVC-purple)](https://dvc.org/)


* Notes

static graph_confusion topology
dynamic confusion rates, is_rg, time_to_rg - at train cutoff


---

## The Problem

iNaturalist accumulates millions of wildlife observations from citizen scientists. A subset earn **Research Grade (RG)** status — a quality threshold that makes observations scientifically usable for biodiversity research. Getting there requires community agreement from knowledgeable identifiers, but expert attention is a scarce resource.

The challenge is that **most observations resolve on their own**. The community finds and confirms high-quality observations of well-documented species within days. What gets lost are observations that slip through unnoticed — unusual taxa, ambiguous photos, submissions from regions with few active identifiers, or species that require specialist knowledge to confirm.

This project builds a **probabilistic ranking system** that scores each open "Needs ID" observation on its likelihood of reaching Research Grade, enabling triage of expert review queues toward observations with real potential that the community has not yet engaged with.

---

## Why Triage? — The Settlement Curve

RG label resolution is strongly front-loaded. Of all observations that will eventually reach Research Grade in the dataset:

```
Day   1  ██████████████████████████░░░░░░░░░░░░░░░░░░  56%
Day   7  █████████████████████████████████░░░░░░░░░░░  70%
Day  30  ████████████████████████████████████░░░░░░░░  76%
Day  90  ██████████████████████████████████████░░░░░░  82%
Day 365  █████████████████████████████████████████░░░  94%
Day 730  ████████████████████████████████████████████  97%
```

Over half of all eventually-RG observations are confirmed within 24 hours. By day 7, nearly 70% have resolved without any intervention — found and confirmed through normal community activity.

The **30% remainder at day 7** is the model's target: observations that have slipped through the first wave of community engagement. This population filter is applied at training time — the model is scoped exclusively to observations that are not Research Grade one week after submission, either because they received no identifications or because early identifications have not yet reached community consensus.

**Why a 365-day label window?** A common approach is to label an observation as RG or not based on its status at a fixed horizon — 90 days is a natural choice since most activity happens early. But the settlement curve shows that 90 days captures only 82% of observations that will eventually reach RG. The remaining 18% are disproportionately the hard cases: unusual taxa, observations that required a specialist to discover, or species where identifications trickle in slowly over months. Labelling them as negatives at 90 days would introduce systematic noise into exactly the population the model is trying to learn from.

Extending the window to 365 days captures 93.6% of eventual-RG observations — recovering most of that signal — while keeping the dataset manageable. Observations must be at least 365 days old at scrape time to receive a label, ensuring the closed-window assumption holds and that no observation is labelled before its outcome is known.

---

## What the Model Actually Learns

The model's predictive signal decomposes into three independently meaningful dimensions, confirmed by SHAP importance analysis:

**1 — Who observed it**
Observer reputation is the single strongest signal. An observer's historical Research Grade rate for a given taxon (`obv_tx_rg_rate`, SHAP rank #1), their lifetime RG rate, tenure, and account-level behavioural signals (photo count habits, description rate, use of mobile vs. desktop) all encode whether this observer reliably produces confirmable observations.

**2 — What was observed**
Taxon identifiability drives the second cluster of signal. The confusion graph neighbourhood rank (`tx_conf_nbrhd_rank_min`, SHAP rank #2) captures how distinguishable this species is within its cluster of visually similar taxa. Static taxon difficulty aggregates — how long this family historically takes to reach RG, how many identifications are typically required, how much the lag varies — capture structurally difficult groups like *Carex* or *Asteraceae* that resist rapid community confirmation regardless of observation quality.

**3 — What the community has already signalled**
Early identification dynamics carry real information even in the not-RG-at-day-7 population. The taxon confusion rate relative to neighbourhood peers (`tx_conf_rate_vs_neighbors`), identification reciprocity, and whether the first identifier agreed with the submitted taxon all encode the community's implicit confidence in the observation.

---

## The Actionable Zone

The model produces a P(Research Grade) score. Not all score ranges are equally useful for triage:

```
P(RG) < 0.35    →  Low-signal observations: poor documentation, structurally
                    ineligible taxa, or species that consistently resist community
                    confirmation. Expert attention is unlikely to change the outcome.
                    Deprioritised in the review queue.

P(RG) 0.35–0.70 →  ACTIONABLE ZONE: real potential but not self-resolving.
                    Expert identification or a confirming ID could tip these to RG.
                    Primary routing target.

P(RG) > 0.70    →  High-confidence candidates: likely to receive community confirmation
                    through normal activity. Included in the queue but lower priority.
```

The model ranks all open observations and outputs a priority score. Threshold boundaries are configurable deployment parameters — the model scores the full population; the actionable zone is a queue management decision, not a hard filter.

### Operating Point (v0.2 — uncalibrated, val set)

The triage threshold is selected to prioritise recall — missing a recoverable observation is costlier than occasionally surfacing a borderline case. The current operating point targets:

- **Recall**: ~90% of eventual-RG observations in the not-RG-at-day-7 population surfaced
- **Precision**: ~60% of routed observations are confirmed RG within the label window

> ⚠️ *LightGBM's raw probabilities are systematically underconfident — the model's predicted 0.5 corresponds to a true positive rate closer to 0.44. Probability calibration (Platt scaling) is part of the v0.3 serving layer. Until then, threshold values should not be interpreted as literal probabilities. Ranking metrics (PR-AUC, Lift@K) are calibration-independent and are the primary evaluation signal in v0.2.*

---

## Model Performance

*Final HP-tuned results on held-out test set: **TBD** — pending Optuna search completion.*

**Ranking metrics — held-out test set (overall RG rate: TBD)**

| Top K% reviewed | Recall@K | Precision@K | Lift@K |
|---|---|---|---|
| 1% | TBD | TBD | TBD |
| 5% | TBD | TBD | TBD |
| 10% | TBD | TBD | TBD |
| 20% | TBD | TBD | TBD |
| 50% | TBD | TBD | TBD |

| Metric | Value |
|---|---|
| PR-AUC (Average Precision) | TBD |
| Baseline (positive rate) | TBD |

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
    Closed-window label: RG status at obs_date + 365 days
          ↓
[Training Dataset]
    Hard temporal split with gap buffers (train / val / test)
          ↓
[Model Training]
    Modular scikit-learn Pipeline with registry-pattern components
    LightGBM + Optuna  + MLflow tracking
          ↓
[Explainability]
    SHAP value analysis — global importance + error-bucket delta analysis
          ↓
[Serving Layer]  ← (v0.3)
    FastAPI  POST /score/batch → ranked observation list with rg_probability
    Probability calibration (Platt scaling)
    Configurable triage threshold with precision floor
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

### 2. Research Grade — a two-stage label re-derivation

Research Grade is a compound label with two requirements, both re-derived from windowed identification history rather than the scraped current state:

**Stage 1 — Community taxon** ([iNaturalist docs](https://help.inaturalist.org/en/support/solutions/articles/151000173076))

The community taxon algorithm performs a taxonomic tree traversal, scoring cumulative agreement against disagreement (including ancestor disagreements), requiring a 2/3 supermajority with a minimum of 2 identifications. This project re-implements the algorithm as a **DuckDB table macro** (`community_taxon_windowed(eval_interval)`) parameterised by evaluation timestamp:

```sql
-- cumulative_agreement / (agreements + disagreements + ancestor_disagreements) ≥ 2/3
-- Minimum 2 identifications required at the agreed node
```

**Stage 2 — Research Grade eligibility** ([iNaturalist docs](https://help.inaturalist.org/en/support/solutions/articles/151000169936))

The `research_grade_windowed()` wrapper enforces all eligibility conditions — verifiable media, geolocation, date, non-captive, species-level community taxon — and surfaces `is_rg` as the training label, replacing the scraped `quality_grade` column entirely and eliminating current-state label leakage.

### 3. Taxon difficulty — Bayesian shrinkage, hierarchical fallback, and static aggregates

Rare taxa have too few observations to compute reliable difficulty estimates. A layered approach handles the full spectrum from common to rare:

**Dynamic features (point-in-time):** Taxon RG rates are computed with Bayesian shrinkage (α = 5) blending the taxon-specific rate toward the global prior. Hierarchical fallback — species → genus → family → order → global mean — activates when sample counts are insufficient. All rates are computed on the training partition only and applied to val/test without recomputation.

**Taxon difficulty aggregates:** Historical patterns encoded at species, genus, and family level:
- Average and standard deviation of time-to-RG (how long does this taxon typically take to resolve?)
- Lag distributions across the taxonomic hierarchy (how long before identifications arrive?)
- Average number of identifications required to reach RG
- Average time required to reach RG
- Identifier specialist, calculates per-family identifier entropy and calculates rate of `specialist` (near 0), `near_specialist` (<0.5) and `generalist` (<2.0) for users that have more than 5 ids and taxa with at least 6 identifications.


These aggregates specifically capture structurally difficult groups where community expertise is sparse and resolution timelines are long regardless of individual observation quality.

### 4. Species confusion graph features

Visually similar species create systematic misidentification patterns. The confusion graph, built with DuckPGQ, encodes:

- **Neighbourhood difficulty**: how hard is the local species cluster to disambiguate?
- **Asymmetric sink flag**: is this taxon disproportionately the *recipient* of misidentifications from visually similar species?
- **Focal taxon rank within neighbourhood**: where does this species sit in terms of identifier confidence relative to its confusion cluster?
- **2-hop graph metrics**: clustering coefficient and graph size capturing the broader confusion neighbourhood

### 5. Protocol-based async API client

The enrichment layer uses a fully async client built around Python `Protocol` interfaces rather than inheritance, keeping fetchers and writers decoupled and independently testable:

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

### 7. SQL-first feature engineering with params CTE injection

All feature transforms are expressed as `.sql` files using DuckDB window functions. Point-in-time constraints are injected via a single `WITH params AS (SELECT ...)` CTE block — keeping SQL files valid and readable as standalone queries while Python controls evaluation timestamps without scattered `str.format()` placeholders.

---

## Feature Groups

| Group | Key Features |
|---|---|
| **Observer history** | Historical RG rate (actual vs. expected), obs count, account tenure, taxon diversity, observer reputation rank |
| **Observation documentation** | Photo count, average photo count, presence of notes, coordinate uncertainty, submission lag (observed → created) |
| **Taxon context** | Taxon rank, RG rate with Bayesian shrinkage and hierarchical fallback, taxon popularity rank |
| **Static taxon difficulty** | Time-to-RG mean/std, lag distributions at species/genus/family, IDs required for RG, lag deviation from taxon norm |
| **Identification dynamics** | IDs received, agreement rate, identifier diversity, ID velocity, reciprocity ratio, maverick count |
| **Confusion graph** | Neighbourhood difficulty, sink-species flag, focal taxon rank in cluster, 2-hop clustering coefficient |
| **Temporal / phenological** | Submitted week (sin/cos), observed week (sin/cos), submission pressure, activity at phenological peak, months from peak phenology |
| **Community consensus** | Community taxon rank, first-ID agreement, pct IDs refining at window, pct IDs agreeing at window |

---

## ML Stack

| Concern | Tool |
|---|---|
| Storage & transforms | DuckDB (SQL-first, no ORM) + DuckPGQ |
| Pipeline composition | scikit-learn `Pipeline` |
| Model | LightGBM |
| Hyperparameter search | Optuna (TPE sampler, MedianPruner, fANOVA importance logged to MLflow) |
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

Reserved for a single terminal evaluation run against the held-out test partition — never used during model selection or feature iteration.

### Score *(v0.3)*

```bash
# Score all open observations submitted in the past N days
inat_pipe score --since_days 7 --output priority_queue.csv

# Score a specific taxon group or region
inat_pipe score --taxon_id 47126 --region QC
```

The score command is designed for **periodic batch execution** — weekly or on-demand — rather than per-observation inference. It queries open "Needs ID" observations not yet RG at the 7-day mark, applies the trained pipeline, and outputs a ranked list for expert review queue consumption.

---

## Data Selection

Selection happens at two levels:

**Observation-level eligibility** — only verifiable observations: georeferenced, dated, with media. Casual and ineligible observations are excluded from the training set but preserved as a separate class for potential future modelling.

**Observer-level coverage** — observers must meet both:
- **Minimum activity**: ≥ 20 observations, ensuring a meaningful historical footprint for observer reputation features
- **Time coverage**: oldest observation before 2020 and newest after 2024, ensuring the observer's history spans the label window cleanly

**Population filter** — the model is trained exclusively on observations that are not Research Grade at day 7 after submission.

---

## Split Strategy

Splits use hard date-range boundaries derived from a `SplitConfig` dataclass anchored on a single `cutoff_date`. Gap buffers between partitions prevent label-time contamination. Val and test sets are ordered by `created_at` to preserve temporal integrity.

```
[──── Train ────][gap][── Val ──][gap][─── Test ───]
  ~55%                  ~17%            ~27%
```

Positive rate drifts across splits (train ~41% → val ~32% → test ~28%), reflecting the evolving composition of the iNaturalist community over time. This is expected and is not a sign of overfitting.


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
└── cli.py                   # Entrypoints: ingest / features / train / test / score
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
- Primary metric: Average Precision (PR-AUC) — calibration-independent, directly reflects ranking quality at low recall thresholds
- DVC for data versioning

### 🔲 v0.3 — Serving, calibration, and system integrity
- FastAPI batch scoring endpoint (`POST /score/batch → ranked priority list`)
- **Probability calibration** (Platt scaling) — corrects LightGBM's systematic underconfidence and enables threshold values to be interpreted as literal probabilities
- **Triage threshold correction** — configurable precision floor with recall-maximising threshold selection post-calibration; designed for weekly re-scoring of open observations
- Cold-start fallback paths via precomputed inference cache (observer and taxon summary stats for new accounts and rare taxa)
- Schema drift assertions + lightweight feature versioning tied to MLflow runs
- Pydantic models for config and schema enforcement
- Run manifest and pipeline lineage table (idempotent retries, auditability)

### 🔲 v0.4 — Targeted model improvement via error diagnostics
The v0.2 model's SHAP values reveal meaningful signal concentration in a small number of features — primarily observer-taxon history and confusion graph neighbourhood rank. The open question is whether errors in the uncertain zone (P(RG) ≈ 0.35–0.70) are driven by **missing signal** (cold-start, sparse taxon history, undocumented observations) or by the **model misfiring on well-documented cases**.

v0.4 applies a structured SHAP delta analysis across error buckets:

| Bucket | Definition |
|---|---|
| Correct / certain | Model is confident and right — baseline for what good signal looks like |
| Incorrect / certain | Model is confident and wrong — the most actionable failures |
| Correct / uncertain | Model hedges but is right — which features help resolve ambiguity? |
| Incorrect / uncertain | Model hedges and is wrong — where is signal absent or misleading? |

Comparing mean absolute SHAP values **and feature value distributions** across these buckets distinguishes absence (feature is missing or near-zero) from misfiring (feature is present but pushes the wrong direction). Preliminary analysis suggests that uncertain-zone errors correlate with thin community consensus signals (`pct_ids_agree_at_window`, `community_taxon_rank`) — pointing toward cold-start uncertainty as the primary failure driver.

Planned work:
- Complete EDA by feature × error bucket — feature value distributions per bucket, not just SHAP deltas
- Complementary species confusuon graph features.
  - PageRank / eigenvector centrality
  - 2-hop expansion rate
- Geographic range signal (is this observation outside the taxon's typical range?)
- AWS S3 ingestion source migration to facilitate scope expansion beyond Québec
- **Two-model routing architecture**: route observations at inference time on
  `has_any_id` — a discoverability model (no-ID population, current scope) and
  a resolution model (has-ID population, disputed/specialist cases). The routing
  gate is a single binary feature; both models share the same pipeline and
  serving infrastructure. Requires ~10× current has-ID training volume to be
  viable — addressable by expanding scope beyond Québec.
---

## Scope & Limitations

- Currently scoped to **Plantae** observations in **Québec**
- Observer cold-start (accounts with < 20 observations) handled via precomputed fallback stats in v0.3
- Probability scores are uncalibrated pre-v0.3; ranking metrics are the primary evaluation signal in v0.2
**Population scope — discovery vs. resolution**
Open "Needs ID" observations that haven't reached Research Grade at day 7 fall
into two structurally distinct sub-populations:

| Sub-population | Definition | Train positives | Positive rate |
|---|---|---|---|
| **No-ID** | Zero external identifications at day 7 | ~23,000 | ~43% |
| **Has-ID** | Identifications received but no consensus | ~1,500 | ~26% |

The no-ID population represents a **discoverability problem** — quality
observations that haven't gained community attention yet. The has-ID population
represents a **resolution problem** — observations where early identifications
exist but taxonomic consensus hasn't formed.

This model is scoped exclusively to the no-ID population. The has-ID population
has a lower and more stable positive rate (~20% across val/test) consistent with
structural difficulty rather than neglect, and its training set size (~1,500
positives) is insufficient for a reliable separate model at the current data
volume. It is documented here as a distinct problem class for future work.

---

*Built as a portfolio project modeled on a production ML team working within the iNaturalist ecosystem.*
