"""Deterministic synthetic fixtures for the offline Oracle/AWM/OED pipeline.

Nothing in this package is physical evidence. It generates Oracle-shaped
output from a declared synthetic scenario so the downstream consumers can be
exercised over a multi-candidate design domain without FlowMesh.
"""

from .contracts import (
    MINIMUM_FIXTURE_DESIGNS,
    SYNTHETIC_ORACLE_FIXTURE_SCHEMA_VERSION,
    SyntheticDesignSpec,
    SyntheticFixtureConfig,
    SyntheticFixtureConfigError,
    load_synthetic_fixture_config,
)
from .generator import (
    SYNTHETIC_FIXTURE_STATEMENT,
    SYNTHETIC_ORACLE_MANIFEST_SCHEMA_VERSION,
    SYNTHETIC_ORACLE_RECORD_SCHEMA_VERSION,
    SYNTHETIC_ORACLE_TRUTH_SCHEMA_VERSION,
    generate_synthetic_oracle_fixture,
)

__all__ = [
    "MINIMUM_FIXTURE_DESIGNS",
    "SYNTHETIC_FIXTURE_STATEMENT",
    "SYNTHETIC_ORACLE_FIXTURE_SCHEMA_VERSION",
    "SYNTHETIC_ORACLE_MANIFEST_SCHEMA_VERSION",
    "SYNTHETIC_ORACLE_RECORD_SCHEMA_VERSION",
    "SYNTHETIC_ORACLE_TRUTH_SCHEMA_VERSION",
    "SyntheticDesignSpec",
    "SyntheticFixtureConfig",
    "SyntheticFixtureConfigError",
    "generate_synthetic_oracle_fixture",
    "load_synthetic_fixture_config",
]
