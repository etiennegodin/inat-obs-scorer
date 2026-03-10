# inat-obs-scorer v0.2

> Expert Review Prioritization Engine for inaturalist

Which “Needs ID” observations are most likely to reach Research Grade if reviewed soon?


- choose which obsv from larger pool
-20obs
time coverage


## Dataset Challenges

Main challenge is to avoid temporal leakage and reconstruct features at point in time per observation
- Observations features
    - Identifications history per observation
    - [Community taxon](https://help.inaturalist.org/en/support/solutions/articles/151000173076-what-are-the-community-taxon-and-the-observation-taxon) from identifications
        - Taxonomic tree traversal
    - [Research grade](https://help.inaturalist.org/en/support/solutions/articles/151000169936-what-is-the-data-quality-assessment-and-how-do-observations-qualify-to-become-research-grade-) using community taxon and other quality requirements (main-label)
- Observer features
    - Observations history
    - Research grade rate
    - Taxon diversity
    - Documentation habits summary

Other mentions:
- Sampling strategy from all observations records based on observers, using these metrics:
    - At least 20 observations
    - Time coverage. Oldest observation before 2020 and newest after 2024.
- Taxonomic fallback for taxon with low observations using Bayesian Shrinkage*
- Custom Train/Validation/Test splits accounting for non uniform distribution of observations.
    - Start/End of split accounting for label time



## Data Pipeline Architecture

```markdown
[Raw Source]
    iNaturalist observations exports
    + Low rate queries on inaturalist's api
          ↓
[Storage Layer]
    Local: DuckDB database
          ↓
[Feature Engineering Layer]
    Python features module running sql queries
    All transforms are reproducible and testable
          ↓
[Training Dataset]
    Snapshot at time T, label = RG status at T+90 days
          ↓
[Model Registry]
    MLflow tracking (params, metrics, artifacts)
    Optuna studies to run Cv
          ↓
[Serving Layer]*
    FastAPI endpoint: POST /score → returns {observation_id, rg_probability, rank}

* Not Implemented
```

## Main Engineered Features

| Group                     | Features                                                           |
| ------------------------- | ------------------------------------------------------------------ |
| Observer history          | historical RG rate, total obs count, tenure days, taxa diversity   |
| Identifiers history*       | historical RG rate, total id count, tenure days, taxa diversity    |
| Observation documentation | photo count, has notes, coordinate uncertainty                     |
| Taxon context             | taxon rank, taxon-level RG rate,                           |
| Temporal                  | day of year, time since submission, hour of day                    |
| Community signals*         | number of IDs already received, ID agreement rate so far           |
| Geographic context*      | regional iNat activity density, distance to nearest urban center |

*Not Implemented

## Python pipeline orchestrator

Used to ensure reproducibility


```bash
pip install inat_pipeline
```

## Pipeline commands

### Ingest
```bash
#Ingests data sources, runs api queries, saves to db and stage data
inat_pipe ingest
```

### Features
```bash
# Creates features suite
inat_pipe features
```

### Train
```bash
#  Train
inat_pipe train

inat_pipe train -h

  -h, --help            show this help message and exit
  --classifier {random_forest,gradient_boost,logistic,lightgbm}
  --reducer {pca,svd,none}
  --scaler {standard,minmax,robust,none}
  --encoder {onehot,ordinal}
  --imputer {median,mean,knn,constant}
  --n_trials N_TRIALS, -n N_TRIALS
  --cv_folds CV_FOLDS
  --test, -t            Run a quick test
```

### Inference

```bash
# Run inference on observations
inat_pipe inference*
```

*Not Implemented


## Domain Model
```
Observer quality     → Observation documentation quality
Identifier quality   → Identifier knowlege of the species
Taxon difficulty     → Community attention required
Geographic activity  → Speed of community response
Community consensus  →
                              ↓
                    Research Grade (outcome)
```

## Features
- Data pipeline for inaturalist observations
- Internal Dashboard API*
- CLI interface

*Not Implemented

## Working with inaturalist's API



## Roadmap

### v0.1 - Data pipeline and baseline model
- ELT data pipeline
- Basic feature engineering
- Simple logistic regression model on subset of features as baseline

### v0.2 - Extended user features, real model, evaluation strategy
- sklearn Pipeline
- mlFlow and Optuna setup
- LightGBM model
- SHAP analysis
- Bayesian shrinkage for taxon

### v0.3 - System design

- Model wrapped in FastApi

### v0.4 - Ranking and expert routing, additionnal features

- Survival model (time-to-RG)
- Identifiers features
- ID velocity features (time-to-first-ID, ID burst patterns)
- Specific rare species to expert
- Similar species
- Annotations
