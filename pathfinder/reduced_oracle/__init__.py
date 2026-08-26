"""Exhaustive reduced-design oracle and lock-in experiment support."""

from ..synthetic_marker import (
    SyntheticFixtureRefusal,
    assert_not_synthetic_fixture,
    synthetic_fixture_evidence,
)
from .contracts import (
    REDUCED_ORACLE_SCHEMA_VERSION,
    MaterializationSpec,
    OracleDesignSpec,
    ReducedOracleConfig,
    ReducedOracleConfigError,
    load_reduced_oracle_config,
)
from .objective import analyze_reduced_oracle
from .recovery import (
    RecoveryCircuitOpenError,
    ReducedOracleRecoveryError,
    plan_reduced_oracle_recovery,
    run_reduced_oracle_recovery,
)
from .runner import run_reduced_oracle
from .transition import FilesystemTransitionExecutor, TransitionObservation

__all__ = [
    "REDUCED_ORACLE_SCHEMA_VERSION",
    "FilesystemTransitionExecutor",
    "MaterializationSpec",
    "OracleDesignSpec",
    "ReducedOracleConfig",
    "ReducedOracleConfigError",
    "ReducedOracleRecoveryError",
    "RecoveryCircuitOpenError",
    "SyntheticFixtureRefusal",
    "TransitionObservation",
    "analyze_reduced_oracle",
    "assert_not_synthetic_fixture",
    "load_reduced_oracle_config",
    "plan_reduced_oracle_recovery",
    "run_reduced_oracle",
    "run_reduced_oracle_recovery",
    "synthetic_fixture_evidence",
]
