# inat-obs-scorer

> Early triage machine learning model for inaturalist observations

> Which “Needs ID” observations are most likely to reach Research Grade if reviewed soon?

"I modeled consensus formation in citizen science and built a prioritization engine to improve expert review efficiency using time-aware supervised learning and calibrated ranking models."

Product prompt:

Experts are overwhelmed.
Thousands of “Needs ID” observations sit untouched.
We want to prioritize:
- Observations likely to be misidentified
- Observations likely to reach Research Grade with one more ID
- Observations from underrepresented taxa/regions

Resource allocation modeling
Ranking problem
Time-window prediction

## Features
- Data pipeline for inaturalist observations
- CLI interface


## Data Challenges 

- Avoid temporal leakage and reconstruct features at time $t$
    - Reconstruct identifications history 
    - Reconstruct community taxon from identifications
    - Reconstruct research grade in history based on identifications (not user current label from data)




## Installation

```bash
pip install inat_pipeline
```

## Pipeline modules

### Ingest
```
#Ingests data sources, run api queries
inat_pipe ingest
```

### Process
```
# Process features 
inat_pipe process
```

## Domain Model
```
Observer quality     → Observation documentation quality
Taxon difficulty     → Community attention required
Geographic activity  → Speed of community response
Observation recency  → Has time even elapsed?
                              ↓
                    Research Grade (outcome)
```

## Main Features

```
```


## Data Pipeline Architecture

```
[Raw Source]
    iNaturalist Open Data (AWS S3 — public Parquet/CSV dumps, updated monthly)
    + GBIF export if needed
          ↓
[Storage Layer]
    Local: DuckDB database + Parquet files organized by partition (taxon/year)
          ↓
[Feature Engineering Layer]
    Python modules per feature group (observer_features.py, taxon_features.py, etc.)
    All transforms are reproducible and testable
          ↓
[Training Dataset]
    Snapshot at time T, label = RG status at T+90 days
    Important: respect temporal ordering — no future leakage
          ↓
[Model Registry]
    MLflow tracking (params, metrics, artifacts)
          ↓
[Serving Layer]
    FastAPI endpoint: POST /score → returns {observation_id, rg_probability, rank}
```

## Roadmap

### v0.1 - Data pipeline and baseline model
- ELT data pipeline
- Basic feature engineering
- mlFlow and Optuna setup
- Simple logistic regression model on subset of features as baseline

### v0.2 - Extended user features and Real Model

### v0.3 - System design

### v0.4 - Ranking and expert routing

