# inat-obs-scorer

> **Expert Review Prioritization Engine for iNaturalist**
> *Resurface valuable "Needs ID" observations before they go unnoticed*

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)
[![LightGBM](https://img.shields.io/badge/model-LightGBM-brightgreen)](https://lightgbm.readthedocs.io/)
[![MLflow](https://img.shields.io/badge/tracking-MLflow-orange)](https://mlflow.org/)
[![DuckDB](https://img.shields.io/badge/storage-DuckDB-yellow)](https://duckdb.org/)
[![DVC](https://img.shields.io/badge/data-DVC-purple)](https://dvc.org/)

---

> **Scope:** Trained and evaluated on Plantae observations from Québec only (~100K observations, test set n = 27,474) — a single-kingdom, single-region slice of iNaturalist's global, multi-taxa dataset. Every statistic in this README describes a pattern observed in *this* sample. Resolution speed, identifier pool size, and class balance are assumed to vary substantially across taxa (e.g. birds vs. plants) and regions. See [Scope & Limitations](#scope--limitations).

## The Problem

iNaturalist observations reach **Research Grade (RG)** — the quality bar that makes them usable for biodiversity research once the community agrees on an identification. For well-documented species, that consensus often forms quickly through ordinary community activity. What falls through the cracks are observations that need a specialist eye — unusual taxa, ambiguous photos, species rarely encountered.

**inat-obs-scorer** scores each open "Needs ID" observation on its probability of reaching Research Grade, so expert reviewers can spend their limited time where it actually moves the needle — not on observations that will resolve themselves, and not on ones that never will.

---

## Why Triage?

Within this Plantae/Québec sample, RG resolution is heavily front-loaded: over half of all eventually-RG observations are confirmed within 24 hours. By day 7, nearly 70% have resolved without any intervention through ordinary community activity. The model is scoped to the **remaining ~30%** — observations still unresolved at day 7 — where expert attention actually changes outcomes. This is plausibly specific to plant identification dynamics and the size of Québec's identifier community; faster- or slower-moving taxa, or regions with larger identifier pools, would likely show a different shape.

Labels here are assigned at a **365-day horizon**. In this dataset, a shorter 90 day window captured only 82% of eventual RG outcomes and systematically mislabels the hardest cases, i.e. the slow-resolving, specialist-dependent observations the model exists to find. Extending to 365 days recovers 93.6% of eventual outcomes, at the cost of a longer (but still practical) label-maturation window.

---

## What the Model Learns

SHAP analysis on the held-out test set surfaces three dominant signal clusters:

- **Observer-taxon history** — the strongest single predictor. How reliably has this observer's work in this taxon group been confirmed before? An observer with a track record in a specific group is a much stronger prior than a first-time submission.
- **Confusion graph neighbourhood** — both static graph position (how distinct is this species from commonly-confused look-alikes) and dynamic neighbourhood behaviour (is this taxon resolving faster or slower than its visual peers right now). Six of the top eleven features come from this graph.
- **Taxon difficulty** — structural signals like typical time-to-RG and IDs required, with a hierarchical fallback (species → genus → family) for taxa with thin history.

Community-level signals (e.g. trailing 90-day RG rate) contribute but are secondary — by definition, the day-7 filter has already screened out observations with strong early community engagement.

*Full SHAP rankings and feature definitions: [`docs/TECHNICAL.md`](docs/TECHNICAL.md#what-the-model-learns).*

---

## Model Performance (Plantae, Québec)

**Held-out test set** (n = 27,474, positive rate 28.2%) — final model retrained on train+val, test set used once for final reporting. All numbers below are specific to this sample and would likely need re-validation before being applied to other taxa or regions.

 Top K% reviewed | n reviewed | Recall@K | Precision@K | Lift@K |
|---|---|---|---|---|
| 1% | 275 | 3.5% | 98.5% | 3.45× |
| 5% | 1,374 | 16.5% | 93.9% | 3.29× |
| 10% | 2,748 | 30.6% | 87.3% | 3.06× |
| 20% | 5,495 | 52.5% | 74.9% | 2.62× |
| 50% | 13,737 | 88.7% | 50.6% | 1.77× |


**PR-AUC (Average Precision): 0.743** · Brier Score: 0.134 · Baseline (positive rate): 0.282

On 613 observations from 402 **unseen taxa**, Average Precision drops to 0.54 (vs. an 18.4% baseline) — expected, since the model's strongest signals (observer-taxon history, confusion graph rank) are sparse or absent for taxa with no prior data. Cold-start handling is a v0.3 target.

---

## The Actionable Zone

The model outputs a continuous P(RG) score, not a binary label. The useful range for triage sits in the middle:

| Score range | Meaning |
|---|---|
| **< 0.35** | Low signal — unlikely to resolve regardless of expert attention. Deprioritized. |
| **0.35 – 0.70** | **Actionable zone** — real potential, not self-resolving. Primary routing target. |
| **> 0.70** | High confidence — likely to resolve through normal community activity. Lower priority. |

Two operating points on the (currently uncalibrated) v0.2 scores: **90% recall at 50% precision**, or **75% recall at 60% precision** for a tighter, time-constrained queue. Calibration (v0.3) will make these thresholds interpretable as literal probabilities. Both the 0.35/0.70 boundaries and these operating points were calibrated to this dataset's class balance and act as deployment parameters, and would most likely need re-fitting for other taxa or regions.

---

## Architecture

```
[Raw Source] → [Ingestion: async API client + DuckDB] → [Feature Engineering:
point-in-time SQL transforms] → [Label Engineering: windowed RG re-derivation] →
[Train/Val/Test split with gap buffers] → [LightGBM + Optuna + MLflow] →
[SHAP explainability] → [Serving: FastAPI, v0.3]
```

Built around four explicit safeguards against temporal leakage (label, feature, split, and CV leakage), a from-scratch windowed re-implementation of iNaturalist's Research Grade algorithm, Bayesian-shrunk taxon difficulty estimates, and a DuckPGQ-based species confusion graph. Full design rationale: [`docs/TECHNICAL.md`](docs/TECHNICAL.md#key-engineering-decisions).

---

## ML Stack

| Concern | Tool |
|---|---|
| Storage & transforms | DuckDB + DuckPGQ |
| Pipeline composition | scikit-learn `Pipeline` |
| Model | LightGBM |
| Hyperparameter search | Optuna (TPE + MedianPruner) |
| Experiment tracking | MLflow |
| Explainability | SHAP |
| Data versioning | DVC |
| Calibration / Serving *(v0.3)* | Platt scaling / FastAPI |

---

## Quick Start

```bash
pip install -e .

inat_pipe ingest local                # or: ingest api --rate 15
inat_pipe features
inat_pipe train --classifier lightgbm --n_trials 50 --cv_folds 5
inat_pipe test                        # single terminal evaluation on held-out test set
```

Full CLI reference and project layout: [`docs/TECHNICAL.md`](docs/TECHNICAL.md#cli-reference).

---

## Scope & Limitations

- **Dataset scope:** Plantae observations in Québec only (~100K rows, a small slice of iNaturalist's tens of millions). The settlement curve, label-window recovery rates, and all performance metrics above describe this sample — they are not iNaturalist-wide figures, and other taxa or regions (different identifier community sizes, different resolution dynamics) would likely show different numbers
- Scoped exclusively to the **no-ID population** (observations with zero external identifications at day 7); the has-ID population is a structurally distinct resolution problem, documented as future work
- Scores are uncalibrated pre-v0.3 — ranking metrics (PR-AUC, Lift@K) are the primary evaluation signal
- One accepted low-severity leakage source (`taxon_avg_ids_to_rg`) — see [`docs/TECHNICAL.md`](docs/TECHNICAL.md#known-leakage)

---

## Roadmap

- ✅ **v0.1** — Data pipeline, DuckDB storage, logistic regression baseline
- ✅ **v0.2** *(current)* — LightGBM + Optuna + MLflow, SHAP explainability, confusion graph features, leakage-safe CV, Bayesian-shrunk taxon difficulty
- 🔲 **v0.3** — FastAPI serving, Platt scaling calibration, cold-start fallback cache, Pydantic schema enforcement
- 🔲 **v0.4** — SHAP error-bucket diagnostics, two-model routing (no-ID vs. has-ID populations), geographic range features, scope expansion beyond Québec

Full roadmap detail: [`docs/TECHNICAL.md`](docs/TECHNICAL.md#roadmap).

---

*Built as a portfolio project modeled on a production ML team working within the iNaturalist ecosystem.*
