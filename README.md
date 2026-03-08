# inat-obs-scorer v0.1


> Expert Review Prioritization Engine for inaturalist

Which “Needs ID” observations are most likely to reach Research Grade if reviewed soon?

## Data Challenges 

- Avoid temporal leakage and reconstruct features at time of observation submission
    - Reconstruct identifications history per observation 
    - Reconstruct community taxon from identifications using iNaturalist's algorithm
        - Taxonomic tree traversal
    - Reconstruct research grade in history based using community taxon


## Installation

```bash
pip install inat_pipeline
```

## Pipeline modules

### Ingest
```bash
#Ingests data sources, runs api queries and stores to db
inat_pipe ingest
```

### Process
```bash
# Unpacks raw data, creates features 
inat_pipe process
```

### Model
```bash
# Unpacks raw data, creates features 

inat_pipe model*
```
*Not Implemented


## Domain Model
```
Observer quality     → Observation documentation quality
Identifier quality   → Identifier knowlege of the species
Taxon difficulty     → Community attention required
Geographic activity  → Speed of community response
Community consensus  → 
Observation recency  → Has time even elapsed?
                              ↓
                    Research Grade (outcome)
```

## Main Engineered Features

| Group                     | Features                                                           |
| ------------------------- | ------------------------------------------------------------------ |
| Observer history          | historical RG rate, total obs count, tenure days, taxa diversity   |
| Identifiers history*       | historical RG rate, total id count, tenure days, taxa diversity    |
| Observation documentation | photo count, has notes, coordinate uncertainty                     |
| Taxon context             | taxon rank, taxon-level baseline RG rate,                          |
| Temporal                  | day of year, time since submission, hour of day                    |
| Community signals*         | number of IDs already received, ID agreement rate so far           |
| Geographic context*      | regional iNat activity density, distance to nearest urban center |

*Not Implemented

## Data Pipeline Architecture

```markdown
[Raw Source]
    iNaturalist observations exports 
    + Low rate queries on [inaturalist's api](api.inaturalist/v1/docs/)
          ↓
[Storage Layer]
    Local: DuckDB database
          ↓
[Feature Engineering Layer]
    Python features module running sql queries
    All transforms are reproducible and testable
          ↓
[Training Dataset]*
    Snapshot at time T, label = RG status at T+90 days
          ↓
[Model Registry]*
    MLflow tracking (params, metrics, artifacts)
          ↓
[Serving Layer]*
    FastAPI endpoint: POST /score → returns {observation_id, rg_probability, rank}

* Not Implemented
```

## Model Features
- Data pipeline for inaturalist observations
- Internal Dashboard API*
- CLI interface

*Not Implemented


## Roadmap

### v0.1 - Data pipeline and baseline model
- ELT data pipeline
- Basic feature engineering
- mlFlow and Optuna setup
- Simple logistic regression model on subset of features as baseline

### v0.2 - Extended user features, real model, evaluation strategy
- LightGBM model 
- Optuna tuning
- SHAP analysis

### v0.3 - System design

- Model wrapped in FastApi 

### v0.4 - Ranking and expert routing

- Survival model (time-to-RG)
- ID velocity features (time-to-first-ID, ID burst patterns)
- Specific rare species to expert


