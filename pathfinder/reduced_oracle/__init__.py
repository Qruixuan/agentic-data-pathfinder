"""Exhaustive reduced-design oracle and lock-in experiment support."""

from .contracts import (
    REDUCED_ORACLE_SCHEMA_VERSION,
    MaterializationSpec,
    OracleDesignSpec,
    ReducedOracleConfig,
    ReducedOracleConfigError,
    load_reduced_oracle_config,
)
from .objective import analyze_reduced_oracle
from .runner import run_reduced_oracle
from .transition import FilesystemTransitionExecutor, TransitionObservation

__all__ = [
    "REDUCED_ORACLE_SCHEMA_VERSION",
    "FilesystemTransitionExecutor",
    "MaterializationSpec",
    "OracleDesignSpec",
    "ReducedOracleConfig",
    "ReducedOracleConfigError",
    "TransitionObservation",
    "analyze_reduced_oracle",
    "load_reduced_oracle_config",
    "run_reduced_oracle",
]
