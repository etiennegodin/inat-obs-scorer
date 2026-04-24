from dataclasses import dataclass, field
from datetime import date
from typing import Any


@dataclass
class IngestConfig:
    """Configuration for data ingestion stages."""

    api_place_id: int = 6712
    api_rate: int = 30
    api_ignore_not_found: bool = True


@dataclass
class FeaturesConfig:
    """Configuration for feature engineering sets."""

    sets: dict[str, dict[str, Any]] = field(
        default_factory=lambda: {
            "train_val": {
                "cutoff_date": date(2023, 1, 1),
                "label_window_days": 365,
                "scraped_at": date(2026, 3, 1),
                "score_window_days": 7,
                "max_val_size": 30000,
                "val_window_days": 410,
                "max_test_size": 100000,
                "gap_days": 30,
            },
            "full": {
                "cutoff_date": date(2024, 1, 1),
                "label_window_days": 365,
                "scraped_at": date(2026, 3, 1),
                "score_window_days": 7,
                "max_val_size": 30000,
                "val_window_days": 410,
                "max_test_size": 100000,
                "gap_days": 30,
            },
        }
    )


@dataclass
class PipelineConfig:
    """Master configuration for the entire inat_pipeline."""

    ingest: IngestConfig = field(default_factory=IngestConfig)
    features: FeaturesConfig = field(default_factory=FeaturesConfig)
