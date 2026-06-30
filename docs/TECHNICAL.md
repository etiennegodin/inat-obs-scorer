# Technical Design — inat-obs-scorer

This document holds the engineering depth that doesn't belong in a first-contact README: full SHAP rankings, leakage analysis, label re-derivation logic, CV design, and project layout. Start with the [README](../README.md) for the project pitch and headline results.

> **Scope reminder:** every figure below — SHAP rankings, positive rates, leakage severity, population sizes — comes from a single dataset (Plantae, Québec, ~100K observations). Treat these as evidence of *how this system was built and validated*, not as general claims about iNaturalist as a whole.

---

## What the Model Learns

Full SHAP importance ranking on the held-out test set, by cluster.

**1 — Observer-taxon history**

`obv_tx_rg_rate` (rank #1, mean |SHAP| = 0.745) — the observer's Bayesian-shrunk RG rate within this specific taxon, prior drawn from the taxon's community-wide RG rate — and `obv_rg_rate_lifetime` (#6, 0.106) together signal whether this person reliably produces confirmable observations.

**2 — Confusion graph neighbourhood**

Six of the top eleven features come from the species confusion graph, in two sub-signals:

- *Static topology:* `single_hop_rank_max` (#2, 0.721) — the maximum taxonomic rank level of any confusion neighbor in the focal species' confusion graph.
- *Dynamic neighbourhood rates* (point-in-time, computed at train cutoff): `time_to_rg_vs_neighbors` (#3, 0.324), `rg_rate_vs_neighbors` (#4, 0.210), `nbor_rg_rate_mean` (#8, 0.094), `rg_percentile_in_neighborhood` (#10, 0.074), `nbor_rg_rate_median` (#11, 0.067). Together these five rival the top static feature in aggregate SHAP weight.

**3 — Taxon difficulty and identifiability**

`tx_avg_ids_to_rg` (#5, 0.157) and `effective_time_to_rg_mean` (#7, 0.096) measure how many identifications a taxon typically requires and how long resolution takes — independent signals that together flag specialist-dependent families. Hierarchical RG rate fallback (`tx_genus_rg_rate` #12, `tx_family_rg_rate` #17, popularity ranks #18, #20) covers taxa with limited per-species history.

Community signals are present but secondary: `trailing_community_rg_rate_90d` (#13, 0.058). The day-7 population filter has already screened against observations with strong early engagement, so what remains carries limited signal.

---

## Key Engineering Decisions

### 1. Temporal leakage — four distinct risk vectors

| Vector | Risk | Mitigation |
|---|---|---|
| **Label leakage** | Scraped `quality_grade` reflects current state, not state at prediction time | RG label re-derived from windowed identification history with a DuckDB table macro |
| **Feature leakage** | Aggregating observer/taxon stats across the full dataset inroduces future signal | All window functions bounded to when the specific observation was created (`created_at`) |
| **Split leakage** | Shuffling within temporal partitions destroys gap buffer integrity | Hard date-range boundaries from `SplitConfig`; val/test rows ordered by `created_at`, never shuffled |
| **CV split leakage** | Standard K-fold with shuffling violates temporal structure | Custom `ExpandingWindowCvSplit(BaseCrossValidator)` — equal-chunk expanding window, sklearn-compatible; gap expressed as `timedelta` applied to the training tail boundary |

### 2. Research Grade — a two-stage label re-derivation

Research Grade is a two-step label, re-derived from windowed identification history.

**Stage 1 — Community taxon** ([iNaturalist docs](https://help.inaturalist.org/en/support/solutions/articles/151000173076)): a taxonomic tree traversal scoring cumulative agreement against disagreement (including ancestor disagreements), requiring 2/3 majority with a minimum of 2 identifications. Re-implemented as a DuckDB table macro, `community_taxon_windowed(eval_interval)`, parameterised by evaluation timestamp.

**Stage 2 — Research Grade eligibility** ([iNaturalist docs](https://help.inaturalist.org/en/support/solutions/articles/151000169936)): the `research_grade_windowed()` wrapper enforces verifiable media, geolocation, date, non-captive status, and species-level community taxon — surfacing `is_rg` as the training label.

### 3. Taxon difficulty — Bayesian shrinkage, hierarchical fallback, static aggregates

**Dynamic features (point-in-time):** Taxon RG rates use Bayesian shrinkage (α = 5) blending the taxon-specific rate toward the global prior, with hierarchical fallback (species → genus → family → order → global mean) when sample counts are thin. All rates computed on the training partition only, applied to val/test without recomputation.

**Static taxon difficulty aggregates** at species/genus/family level: time-to-RG mean/std, lag distributions across the taxonomic hierarchy, average IDs required to reach RG, and identifier specialist concentration — per-family identifier entropy partitioned into `specialist` (H < 0.5), `near_specialist` (H < 1.0), and `generalist` (H ≥ 2.0) rates for users with ≥ 5 IDs across taxa with ≥ 6 identifications.

### 4. Species confusion graph features

Built with DuckPGQ, contributing two feature families:

**Static graph topology** (computed once from full graph structure): neighbourhood difficulty (aggregate RG rate and identifier confidence across the local cluster), asymmetric sink flag (whether this taxon disproportionately receives misidentifications from visually similar species) and 2-hop topology metrics (expansion rate, clustering coefficient, connected component size).

**Dynamic confusion rates** (windowed, computed at train cutoff): `time_to_rg_vs_neighbors` (#3), `rg_rate_vs_neighbors` (#4), neighbour RG rate distributions, and focal taxon percentile rank within the neighbourhood. These outperform static topology on most metrics — they encode current identifier community behaviour rather than graph structure alone.

### 5. Protocol-based async API client

```
BatchEndpointClient        — fixed-set ID requests, bulk pagination
ParametrizedEndpointClient — flexible endpoint/param formatting per call

asyncio.Queue              — bridges fetch workers and the write thread
ThreadPoolExecutor(max_workers=1) — serialises DuckDB writes from async context
Exponential backoff + jitter — handles iNaturalist rate limiting gracefully
_resolve_id cascade        — flexible ID field mapping across endpoint shapes
```

Fetchers and writers are decoupled via `Protocol` interfaces rather than inheritance, keeping each independently testable.

### 6. Modular scikit-learn pipeline with registry pattern

Each pipeline stage (imputer, encoder, scaler, reducer, classifier) is registered by name and resolved at runtime from CLI arguments:

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

All feature transforms are `.sql` files using DuckDB window functions. Point-in-time constraints are injected via a single `WITH params AS (SELECT ...)` CTE block — SQL files stay valid and readable as standalone queries while Python controls evaluation timestamps without scattered `str.format()` placeholders.

---

## Split Strategy

```
[──── Train ────][gap][── Val ──][gap][─── Test ───]
  ~55%                  ~17%            ~27%
```

Hard date-range boundaries from a `SplitConfig` dataclass anchored on a single `cutoff_date`, with gap buffers preventing label-time contamination. Val and test sets are ordered by `created_at`.

**Positive rate drift across splits:** train ~47% → val ~34% → test ~29%. This is a genuine distribution shift — the dataset was scraped March 3, 2026, and all observations required a completed 365-day label window before receiving a label, so drift can't be attributed to label maturation. It reflects platform-level change plausibly: observation volume has grown faster than identifiers.

Practical consequence: Optuna's CV objective is evaluated on training-period folds (~47% positive rate), while the model is ultimately tested against a ~29% distribution. HPs tuned on CV may be slightly overfit to the richer-positive training — a known limitation without a clean fix that preserves temporal split integrity.

---

## Known Leakage

| Feature | Leakage type | Severity | Notes |
|---|---|---|---|
| `taxon_avg_ids_to_rg` | Taxon-level aggregate computed from training observations using their 365-day label window outcomes — not strictly the point-in-time ID count at the score window | Low | Historical average for the taxon, not the current observation's future ID count; taxon-level aggregation attenuates per-observation bias |

---

## Population Scope — Discovery vs. Resolution

Open "Needs ID" observations not RG at day 7 split into two structurally distinct sub-populations:

| Sub-population | Definition | Train positives | Positive rate |
|---|---|---|---|
| **No-ID** | Zero external identifications at day 7 | ~23,000 | ~43% |
| **Has-ID** | Identifications received but no consensus | ~1,500 | ~26% |

No-ID is a **discoverability problem** — quality observations that haven't gained community attention. Has-ID is a **resolution problem** — early identifications exist but consensus hasn't formed. This model is scoped exclusively to no-ID: has-ID has a lower, more stable positive rate (~20% across val/test) consistent with structural difficulty rather than neglect, and ~1,500 positives is insufficient for a reliable separate model at current data volume.

---

## Feature Groups

| Group | Key Features |
|---|---|
| **Observer history** | Historical RG rate (actual vs. expected), obs count, account tenure, taxon diversity, observer reputation rank |
| **Observation documentation** | Photo count, average photo count, presence of notes, coordinate uncertainty, submission lag (observed → created) |
| **Taxon context** | Taxon rank, RG rate with Bayesian shrinkage and hierarchical fallback, taxon popularity rank |
| **Static taxon difficulty** | Time-to-RG mean/std, lag distributions at species/genus/family, IDs required for RG, identifier specialist concentration |
| **Identification dynamics** | IDs received, agreement rate, identifier diversity, ID velocity, reciprocity ratio, maverick count |
| **Confusion graph — static topology** | Neighbourhood size, sink-species asymmetry flag, focal taxon rank in cluster (`single_hop_rank_max`), 2-hop expansion rate and clustering coefficient |
| **Confusion graph — dynamic rates** | Neighbour RG rate mean/median, `time_to_rg_vs_neighbors`, `rg_rate_vs_neighbors`, focal taxon percentile in neighbourhood — all computed at train cutoff |
| **Temporal / phenological** | Submitted week (sin/cos), observed week (sin/cos), submission pressure, activity at phenological peak, months from peak phenology |
| **Community** | Trailing community RG rate over 90-day window (`trailing_community_rg_rate_90d`), community observation count in window |

---

## CLI Reference

```bash
pip install -e .
```

**Ingest**
```bash
inat_pipe ingest local                              # from local export files
inat_pipe ingest api --rate 15 --ignore_not_found    # async, rate-limited API enrichment
```

**Feature engineering**
```bash
inat_pipe features
```

**Train**
```bash
inat_pipe train \
  --classifier lightgbm \
  --imputer median \
  --encoder onehot \
  --scaler robust \
  --n_trials 50 \
  --cv_folds 5
```

**Evaluate**
```bash
inat_pipe test
```
Reserved for a single terminal evaluation run against the held-out test partition — never used during model selection or feature iteration.

**Score** *(v0.3)*
```bash
inat_pipe score --since_days 7 --output priority_queue.csv
inat_pipe score --taxon_id 47126 --region QC
```
Designed for periodic batch execution (weekly or on-demand) rather than per-observation inference. Queries open "Needs ID" observations not yet RG at the 7-day mark, applies the trained pipeline, outputs a ranked list for expert review queue consumption.

---

## Data Selection

**Observation-level eligibility** — only verifiable observations: georeferenced, dated, with media. Casual/ineligible observations are excluded from training but preserved as a separate class for potential future modelling.

**Observer-level coverage** — observers must meet both:
- **Minimum activity**: ≥ 20 observations
- **Time coverage**: oldest observation before 2020 and newest after 2024, so observer history spans the label window cleanly

**Population filter** — trained exclusively on observations not Research Grade at day 7 after submission.

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
│       ├── ingestors.py     # Expandable backend support (v0.4)
│       └── protocols.py
├── queries/                 # All .sql queries
│   ├── api/                 # Prep raw data receiving
│   ├── features/            # Features suite, injected via params CTE
│   ├── graph/                # Graph queries for taxa confusion, with DuckPGQ
│   ├── split/                # Train/Val/Test splits
│   ├── stage/                # Raw data staging
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

**✅ v0.1 — Data pipeline and baseline**
ELT pipeline, DuckDB storage layer, basic feature engineering, logistic regression baseline.

**✅ v0.2 — Extended features and production model**
scikit-learn Pipeline with registry pattern; LightGBM + Optuna (TPE + MedianPruner) + MLflow + Optuna Dashboard; SHAP explainability; windowed community taxon and RG label re-derivation; Bayesian-shrunk taxon RG rates with hierarchical fallback; static confusion graph topology via DuckPGQ; dynamic confusion graph features at train cutoff; static taxon difficulty aggregates; temporal/phenological features; community trailing RG rate features; custom `ExpandingWindowCvSplit`; population filter (not-RG at day 7); final model retrained on train+val before test; primary metric Average Precision (PR-AUC); DVC data versioning.

**🔲 v0.3 — Serving, calibration, and system integrity**
FastAPI batch scoring endpoint (`POST /score/batch`); Platt scaling calibration correcting LightGBM's systematic underconfidence; configurable precision-floor triage threshold post-calibration; cold-start fallback via precomputed observer/taxon inference cache; schema drift assertions and feature versioning tied to MLflow runs; Pydantic config/schema enforcement; run manifest and pipeline lineage table.

**🔲 v0.4 — Targeted model improvement via error diagnostics**
The open question: are uncertain-zone errors (P(RG) ≈ 0.35–0.70) driven by missing signal (cold-start, sparse history) or the model misfiring on well-documented cases? A structured SHAP delta analysis compares four error buckets:

| Bucket | Definition |
|---|---|
| Correct / certain | Model is confident and right — baseline for what good signal looks like |
| Incorrect / certain | Model is confident and wrong — the most actionable failures |
| Correct / uncertain | Model hedges but is right — which features help resolve ambiguity? |
| Incorrect / uncertain | Model hedges and is wrong — where is signal absent or misleading? |

Comparing mean |SHAP| and feature value distributions across buckets distinguishes absence (feature missing/near-zero) from misfiring (feature present but pushes the wrong direction). Preliminary analysis points to thin observer-taxon history and sparse confusion neighbourhood signal as the primary failure driver in the uncertain zone.

Planned work: complete EDA by feature × error bucket; complementary confusion graph features (PageRank/eigenvector centrality, hidden confusion rate); geographic range signal; AWS S3 ingestion migration to support scope expansion beyond Québec; two-model routing architecture (`has_any_id` gate splitting a discoverability model from a resolution model, requiring ~10× current has-ID training volume — addressable by expanding beyond Québec).
